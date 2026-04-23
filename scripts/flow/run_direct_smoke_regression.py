#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _workspace_root() -> Path:
    return _repo_root().parent


def _default_suite_root() -> Path:
    return _repo_root() / "examples" / "direct_smoke_inputs" / "validated_positive"


def _default_run_root() -> Path:
    return _workspace_root() / "orca_scratch" / f"direct_smoke_regression_{_now_stamp()}"


def _case_registry_path(suite_root: Path) -> Path:
    return suite_root / "cases.json"


def _read_xyz(path: Path) -> tuple[str, list[tuple[str, str, str, str]]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    natoms = int(lines[0].strip())
    comment = lines[1] if len(lines) > 1 else ""
    atoms: list[tuple[str, str, str, str]] = []
    for line in lines[2 : 2 + natoms]:
        parts = line.split()
        atoms.append((parts[0], parts[1], parts[2], parts[3]))
    return comment, atoms


def _write_xyz(path: Path, comment: str, atoms: list[tuple[str, str, str, str]]) -> None:
    lines = [str(len(atoms)), comment]
    for element, x, y, z in atoms:
        lines.append(f"{element:2s} {float(x): .8f} {float(y): .8f} {float(z): .8f}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _atom_sequence(atoms: list[tuple[str, str, str, str]]) -> tuple[str, ...]:
    return tuple(atom[0] for atom in atoms)


def _shared_runtime_yaml(*, root: Path, crest_executable: str, xtb_executable: str, orca_executable: str) -> str:
    return (
        "resources:\n"
        "  max_cores_per_task: 2\n"
        "  max_memory_gb_per_task: 4\n"
        "\n"
        "behavior:\n"
        "  auto_organize_on_terminal: false\n"
        "\n"
        "telegram:\n"
        '  bot_token: ""\n'
        '  chat_id: ""\n'
        "\n"
        "crest:\n"
        "  runtime:\n"
        f'    allowed_root: "{root}/crest_runs"\n'
        f'    organized_root: "{root}/crest_outputs"\n'
        "    max_concurrent: 1\n"
        f'    admission_root: "{root}/admission"\n'
        "    admission_limit: 2\n"
        "  paths:\n"
        f'    crest_executable: "{crest_executable}"\n'
        "\n"
        "xtb:\n"
        "  runtime:\n"
        f'    allowed_root: "{root}/xtb_runs"\n'
        f'    organized_root: "{root}/xtb_outputs"\n'
        "    max_concurrent: 1\n"
        f'    admission_root: "{root}/admission"\n'
        "    admission_limit: 2\n"
        "  paths:\n"
        f'    xtb_executable: "{xtb_executable}"\n'
        "\n"
        "orca:\n"
        "  runtime:\n"
        f'    allowed_root: "{root}/orca_runs"\n'
        f'    organized_root: "{root}/orca_outputs"\n'
        "    default_max_retries: 2\n"
        "    max_concurrent: 1\n"
        f'    admission_root: "{root}/admission"\n'
        "    admission_limit: 2\n"
        "  paths:\n"
        f'    orca_executable: "{orca_executable}"\n'
    )


def _list_case_dirs(suite_root: Path, requested: list[str]) -> list[Path]:
    available = sorted(path for path in suite_root.iterdir() if path.is_dir())
    if not requested:
        return available
    requested_set = set(requested)
    selected = [path for path in available if path.name in requested_set]
    missing = sorted(requested_set.difference(path.name for path in selected))
    if missing:
        raise ValueError(f"Unknown validated smoke cases: {', '.join(missing)}")
    return selected


def _load_case_registry(suite_root: Path) -> dict[str, dict[str, Any]]:
    path = _case_registry_path(suite_root)
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Case registry must be a JSON object: {path}")
    registry: dict[str, dict[str, Any]] = {}
    for key, value in payload.items():
        if isinstance(key, str) and isinstance(value, dict):
            registry[key] = value
    return registry


def _run_command(
    argv: list[str],
    *,
    env: dict[str, str],
    cwd: Path,
    timeout: int | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _process_one_worker_command(*, queue_module: str, config_path: Path) -> list[str]:
    code = (
        "from importlib import import_module; "
        "import sys; "
        f"queue_cmd = import_module({queue_module!r}); "
        f"cfg = queue_cmd.load_config({str(config_path)!r}); "
        "outcome = queue_cmd._process_one(cfg, auto_organize=False); "
        "sys.exit(0 if outcome in {'processed', 'idle', 'blocked'} else 1)"
    )
    return [sys.executable, "-c", code]


def _success_snapshot(case_root: Path, workflow_id: str, workflow_payload: dict[str, Any]) -> tuple[list[str], list[dict[str, Any]]]:
    xtb_root = case_root / "xtb_runs" / "workflow_jobs" / workflow_id
    xtb_ts_paths = [
        str(path)
        for path in sorted(xtb_root.rglob("xtbpath_ts.xyz"))
        if path.exists() and path.stat().st_size > 0
    ]
    orca_stages = [
        stage
        for stage in workflow_payload.get("stages", [])
        if stage.get("stage_kind") == "orca_stage"
    ]
    return xtb_ts_paths, orca_stages


def _result_reason(payload: dict[str, Any]) -> str:
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        return ""
    workflow_error = metadata.get("workflow_error")
    if not isinstance(workflow_error, dict):
        return ""
    return json.dumps(workflow_error, ensure_ascii=True)


def run_case(case_dir: Path, *, args: argparse.Namespace, env: dict[str, str]) -> dict[str, Any]:
    case_meta = dict(args.case_registry.get(case_dir.name, {}))
    case_root = Path(args.run_root).resolve() / case_dir.name
    case_root.mkdir(parents=True, exist_ok=True)
    for subdir in (
        "workflow_root",
        "inputs",
        "crest_runs",
        "crest_outputs",
        "xtb_runs",
        "xtb_outputs",
        "orca_runs",
        "orca_outputs",
        "admission",
    ):
        (case_root / subdir).mkdir(parents=True, exist_ok=True)

    reactant_comment, reactant_atoms = _read_xyz(case_dir / "reactant.xyz")
    product_comment, product_atoms = _read_xyz(case_dir / "product.xyz")
    reactant_sequence = _atom_sequence(reactant_atoms)
    product_sequence = _atom_sequence(product_atoms)
    if reactant_sequence != product_sequence:
        return {
            "case_name": case_dir.name,
            "category": str(case_meta.get("category", "")).strip(),
            "label": str(case_meta.get("label", "")).strip(),
            "case_root": str(case_root),
            "success": False,
            "status": "input_invalid",
            "reason": "reactant and product atom order must match exactly for xTB path search",
            "reactant_atom_sequence": list(reactant_sequence),
            "product_atom_sequence": list(product_sequence),
            "command_failures": [],
        }
    _write_xyz(case_root / "inputs" / "reactant.xyz", reactant_comment, reactant_atoms)
    _write_xyz(case_root / "inputs" / "product.xyz", product_comment, product_atoms)

    shared_config = case_root / "chemstack.yaml"
    shared_config.write_text(
        _shared_runtime_yaml(
            root=case_root,
            crest_executable=args.crest_executable,
            xtb_executable=args.xtb_executable,
            orca_executable=args.orca_executable,
        ),
        encoding="utf-8",
    )

    create_cp = _run_command(
        [
            sys.executable,
            "-m",
            "chemstack.flow.cli",
            "workflow",
            "create-reaction-ts-search",
            "--workflow-root",
            str(case_root / "workflow_root"),
            "--reactant-xyz",
            str(case_root / "inputs" / "reactant.xyz"),
            "--product-xyz",
            str(case_root / "inputs" / "product.xyz"),
            "--crest-mode",
            "standard",
            "--priority",
            str(args.priority),
            "--max-cores",
            str(args.max_cores),
            "--max-memory-gb",
            str(args.max_memory_gb),
            "--max-crest-candidates",
            str(args.max_crest_candidates),
            "--max-xtb-stages",
            str(args.max_xtb_stages),
            "--max-orca-stages",
            str(args.max_orca_stages),
            "--orca-route-line",
            args.orca_route_line,
            "--json",
        ],
        env=env,
        cwd=_repo_root(),
        timeout=args.command_timeout_seconds,
    )
    if create_cp.returncode != 0:
        return {
            "case_name": case_dir.name,
            "category": str(case_meta.get("category", "")).strip(),
            "label": str(case_meta.get("label", "")).strip(),
            "case_root": str(case_root),
            "success": False,
            "status": "create_failed",
            "reason": create_cp.stderr.strip() or create_cp.stdout.strip(),
            "reactant_atom_sequence": list(reactant_sequence),
            "product_atom_sequence": list(product_sequence),
            "command_failures": [
                {
                    "step": "create",
                    "returncode": create_cp.returncode,
                    "stdout": create_cp.stdout,
                    "stderr": create_cp.stderr,
                }
            ],
        }

    create_payload = json.loads(create_cp.stdout)
    workflow_id = str(create_payload["workflow_id"])
    workflow_json = case_root / "workflow_root" / "workflows" / workflow_id / "workflow.json"
    cycle_log: list[dict[str, Any]] = []
    command_failures: list[dict[str, Any]] = []

    worker_base = [
        sys.executable,
        "-m",
        "chemstack.flow.cli",
        "workflow",
        "worker",
        "--once",
        "--workflow-root",
        str(case_root / "workflow_root"),
        "--chemstack-config",
        str(shared_config),
    ]
    crest_worker = _process_one_worker_command(
        queue_module="chemstack.crest.commands.queue",
        config_path=shared_config,
    )
    xtb_worker = _process_one_worker_command(
        queue_module="chemstack.xtb.commands.queue",
        config_path=shared_config,
    )

    status = "unknown"
    reason = ""
    xtb_ts_paths: list[str] = []
    orca_stages: list[dict[str, Any]] = []
    for cycle in range(1, args.max_cycles + 1):
        commands = (
            ("workflow_pre", worker_base),
            ("crest_worker", crest_worker),
            ("xtb_worker", xtb_worker),
            ("workflow_post", worker_base),
        )
        for step_name, argv in commands:
            cp = _run_command(
                argv,
                env=env,
                cwd=_repo_root(),
                timeout=args.command_timeout_seconds,
            )
            if cp.returncode != 0:
                command_failures.append(
                    {
                        "cycle": cycle,
                        "step": step_name,
                        "returncode": cp.returncode,
                        "stdout": cp.stdout,
                        "stderr": cp.stderr,
                    }
                )
        payload = json.loads(workflow_json.read_text(encoding="utf-8"))
        xtb_ts_paths, orca_stages = _success_snapshot(case_root, workflow_id, payload)
        cycle_log.append(
            {
                "cycle": cycle,
                "workflow_status": payload.get("status"),
                "xtb_ts_count": len(xtb_ts_paths),
                "orca_stage_statuses": [stage.get("status", "") for stage in orca_stages],
            }
        )
        if xtb_ts_paths and any(stage.get("status") in {"queued", "running", "completed", "failed"} for stage in orca_stages):
            status = "validated_positive"
            break
        if payload.get("status") in {"failed", "cancelled"}:
            status = str(payload.get("status"))
            reason = _result_reason(payload)
            break

    if status == "unknown":
        payload = json.loads(workflow_json.read_text(encoding="utf-8"))
        status = str(payload.get("status", "unknown"))
        reason = _result_reason(payload)
        xtb_ts_paths, orca_stages = _success_snapshot(case_root, workflow_id, payload)

    return {
        "case_name": case_dir.name,
        "category": str(case_meta.get("category", "")).strip(),
        "label": str(case_meta.get("label", "")).strip(),
        "case_root": str(case_root),
        "workflow_id": workflow_id,
        "success": status == "validated_positive",
        "status": status,
        "reason": reason,
        "reactant_atom_sequence": list(reactant_sequence),
        "product_atom_sequence": list(product_sequence),
        "xtb_ts_paths": xtb_ts_paths,
        "orca_stage_statuses": [stage.get("status", "") for stage in orca_stages],
        "orca_reaction_dirs": [
            str(((stage.get("task") or {}).get("payload") or {}).get("reaction_dir", ""))
            for stage in orca_stages
        ],
        "cycle_log": cycle_log,
        "command_failures": command_failures,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_direct_smoke_regression.py",
        description="Run a small regression smoke suite for validated direct raw-input cases.",
    )
    parser.add_argument(
        "--suite-root",
        default=str(_default_suite_root()),
        help="Directory that contains validated_positive case folders.",
    )
    parser.add_argument(
        "--run-root",
        default=str(_default_run_root()),
        help="Directory where regression scratch runs and summary.json will be written.",
    )
    parser.add_argument(
        "--case",
        action="append",
        default=[],
        help="Limit the run to one case name; may be passed more than once.",
    )
    parser.add_argument(
        "--category",
        action="append",
        default=[],
        help="Limit the run to one category from cases.json; may be passed more than once.",
    )
    parser.add_argument("--max-cycles", type=int, default=18)
    parser.add_argument("--command-timeout-seconds", type=int, default=120)
    parser.add_argument("--priority", type=int, default=10)
    parser.add_argument("--max-cores", type=int, default=2)
    parser.add_argument("--max-memory-gb", type=int, default=4)
    parser.add_argument("--max-crest-candidates", type=int, default=1)
    parser.add_argument("--max-xtb-stages", type=int, default=1)
    parser.add_argument("--max-orca-stages", type=int, default=1)
    parser.add_argument("--orca-route-line", default="! XTB2 OptTS TightSCF")
    parser.add_argument(
        "--crest-executable",
        default="/home/daehyupsohn/bin/crest/crest",
    )
    parser.add_argument(
        "--xtb-executable",
        default="/home/daehyupsohn/bin/xtb-dist/bin/xtb",
    )
    parser.add_argument(
        "--orca-executable",
        default="/home/daehyupsohn/opt/orca/orca",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON summary to stdout.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    suite_root = Path(args.suite_root).expanduser().resolve()
    run_root = Path(args.run_root).expanduser().resolve()
    run_root.mkdir(parents=True, exist_ok=True)

    case_registry = _load_case_registry(suite_root)
    args.case_registry = case_registry
    case_dirs = _list_case_dirs(suite_root, list(args.case))
    if args.category:
        allowed_categories = {item.strip() for item in args.category if str(item).strip()}
        case_dirs = [
            path
            for path in case_dirs
            if str(case_registry.get(path.name, {}).get("category", "")).strip() in allowed_categories
        ]
        if not case_dirs:
            raise ValueError("No validated-positive cases matched the requested --category filter")
    sibling_paths = [
        _repo_root(),
        _repo_root() / "src",
    ]
    pythonpath = os.pathsep.join(str(path) for path in sibling_paths)
    env = dict(os.environ)
    env["PYTHONPATH"] = pythonpath

    results = [run_case(case_dir, args=args, env=env) for case_dir in case_dirs]
    category_counts: dict[str, int] = {}
    category_success_counts: dict[str, int] = {}
    for item in results:
        category = str(item.get("category", "")).strip() or "uncategorized"
        category_counts[category] = category_counts.get(category, 0) + 1
        if item.get("success"):
            category_success_counts[category] = category_success_counts.get(category, 0) + 1
    summary = {
        "suite_root": str(suite_root),
        "run_root": str(run_root),
        "case_count": len(results),
        "success_count": sum(1 for item in results if item.get("success")),
        "failure_count": sum(1 for item in results if not item.get("success")),
        "category_counts": category_counts,
        "category_success_counts": category_success_counts,
        "results": results,
    }
    (run_root / "summary.json").write_text(json.dumps(summary, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(summary, ensure_ascii=True, indent=2))
    else:
        print(f"run_root: {run_root}")
        print(f"case_count: {summary['case_count']}")
        print(f"success_count: {summary['success_count']}")
        print(f"failure_count: {summary['failure_count']}")
        if category_counts:
            print(f"category_counts: {category_counts}")
        for item in results:
            print(
                f"- {item['case_name']} category={item.get('category') or '-'} status={item['status']}"
                f" success={'yes' if item['success'] else 'no'}"
                f" xtb_ts={len(item.get('xtb_ts_paths', []))}"
                f" orca_stage_statuses={item.get('orca_stage_statuses', [])}"
            )
            print(f"  case_root={item['case_root']}")
            if item.get("reason"):
                print(f"  reason={item['reason']}")
    return 0 if summary["failure_count"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
