from __future__ import annotations

import json
import os
import re
import resource
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, TextIO

import yaml

from chemstack.core.config import engines as _config_engines
from chemstack.core.utils import now_utc_iso
from chemstack.core.utils import process as process_utils

from .commands._helpers import (
    MANIFEST_FILE_NAME,
    load_job_manifest,
    resolve_job_inputs,
    resource_request_from_manifest,
)
from .config import AppConfig

_CANDIDATE_PATTERNS = (
    "xtbpath*.xyz",
    "path*.xyz",
    "xtbopt.xyz",
)
_TRIAL_RE = re.compile(
    r"run\s+(\d+)\s+barrier:\s*([-+]?\d+(?:\.\d+)?)\s+dE:\s*([-+]?\d+(?:\.\d+)?)\s+product-end path RMSD:\s*([-+]?\d+(?:\.\d+)?)"
)
_FORWARD_BARRIER_RE = re.compile(
    r"forward\s+barrier\s+\(kcal\)\s*:\s*([-+]?\d+(?:\.\d+)?)", re.IGNORECASE
)
_BACKWARD_BARRIER_RE = re.compile(
    r"backward\s+barrier\s+\(kcal\)\s*:\s*([-+]?\d+(?:\.\d+)?)", re.IGNORECASE
)
_REACTION_ENERGY_RE = re.compile(
    r"reaction energy\s+\(kcal\)\s*:\s*([-+]?\d+(?:\.\d+)?)", re.IGNORECASE
)
_TS_FILE_RE = re.compile(r"estimated TS on file\s+(\S+)", re.IGNORECASE)
_POINT_COUNT_RE = re.compile(r"path\s+(\d+)\s+taken with\s+(\d+)\s+points", re.IGNORECASE)
_COMMENT_ENERGY_RE = re.compile(r"energy\s*[:=]\s*([-+]?\d+(?:\.\d+)?)", re.IGNORECASE)


@dataclass(frozen=True)
class XtbRunResult:
    status: str
    reason: str
    command: tuple[str, ...]
    exit_code: int
    started_at: str
    finished_at: str
    stdout_log: str
    stderr_log: str
    selected_input_xyz: str
    job_type: str
    reaction_key: str
    input_summary: dict[str, Any]
    candidate_count: int
    selected_candidate_paths: tuple[str, ...]
    candidate_details: tuple[dict[str, Any], ...]
    analysis_summary: dict[str, Any]
    manifest_path: str
    resource_request: dict[str, int]
    resource_actual: dict[str, int]


@dataclass
class XtbRunningJob:
    process: subprocess.Popen[str]
    command: tuple[str, ...]
    started_at: str
    stdout_log: str
    stderr_log: str
    stdout_handle: TextIO
    stderr_handle: TextIO
    selected_input_xyz: str
    job_type: str
    reaction_key: str
    input_summary: dict[str, Any]
    manifest_path: str
    resource_request: dict[str, int]
    resource_actual: dict[str, int]
    job_dir: str


@dataclass(frozen=True)
class _RankingRunContext:
    job_dir: Path
    started_at: str
    candidate_paths: list[Path]
    inputs: dict[str, Any]
    top_n: int
    resource_request: dict[str, int]
    resource_actual: dict[str, int]


def _resolve_xtb_executable(cfg: AppConfig) -> str:
    configured = str(cfg.paths.xtb_executable).strip()
    if configured:
        path = Path(configured).expanduser().resolve()
        if not path.exists() or not path.is_file():
            raise ValueError(f"Configured xTB executable not found: {path}")
        return str(path)

    discovered = shutil.which("xtb")
    if discovered:
        return discovered
    raise ValueError("xTB executable not configured and not found on PATH.")


def _resource_request_dict(cfg: AppConfig, manifest: dict[str, Any]) -> dict[str, int]:
    return resource_request_from_manifest(cfg, manifest)


def _resource_actual_dict(resource_request: dict[str, int]) -> dict[str, int]:
    return _config_engines.resource_actual_from_request(resource_request)


def _bool_flag(manifest: dict[str, Any], key: str) -> bool:
    return _config_engines.as_bool(manifest.get(key), False)


def _manifest_int(manifest: dict[str, Any], key: str) -> int | None:
    value = manifest.get(key)
    if value in (None, ""):
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        return int(stripped)
    if isinstance(value, (int, float)):
        return int(value)
    raise ValueError(f"Manifest field {key!r} must be an integer-compatible value.")


def _append_xtb_scalar_options(command: list[str], manifest: dict[str, Any]) -> None:
    gfn = str(manifest.get("gfn", "2")).strip()
    if gfn:
        command.extend(["--gfn", gfn])

    for manifest_key, option in (("charge", "--chrg"), ("uhf", "--uhf")):
        value = _manifest_int(manifest, manifest_key)
        if value is not None:
            command.extend([option, str(value)])


def _append_xtb_solvent_option(command: list[str], manifest: dict[str, Any]) -> None:
    solvent_model = str(manifest.get("solvent_model", "")).strip().lower()
    solvent = str(manifest.get("solvent", "")).strip()
    if solvent and solvent_model in {"gbsa", "alpb"}:
        command.extend([f"--{solvent_model}", solvent])


def _append_xtb_optional_text_options(command: list[str], manifest: dict[str, Any]) -> None:
    for manifest_key, option in (("namespace", "--namespace"), ("xcontrol", "--input")):
        value = str(manifest.get(manifest_key, "")).strip()
        if value:
            command.extend([option, value])


def _append_xtb_job_type_options(
    command: list[str],
    *,
    manifest: dict[str, Any],
    secondary_input_xyz: Path | None,
    job_type: str,
) -> None:
    if job_type == "path_search":
        if secondary_input_xyz is None:
            raise ValueError("path_search requires a product/reference structure")
        command.extend(["--path", str(secondary_input_xyz)])
        return
    if job_type == "opt":
        opt_level = (
            str(manifest.get("opt_level", manifest.get("opt", "normal"))).strip().lower()
            or "normal"
        )
        command.extend(["--opt", opt_level])
        return
    if job_type == "sp":
        command.append("--sp")
        return
    raise ValueError(f"Unsupported xtb job_type: {job_type}")


def _build_command(
    cfg: AppConfig,
    *,
    manifest: dict[str, Any],
    selected_input_xyz: Path,
    secondary_input_xyz: Path | None,
    job_type: str,
) -> list[str]:
    resource_request = _resource_request_dict(cfg, manifest)
    command = [
        _resolve_xtb_executable(cfg),
        str(selected_input_xyz),
        "--parallel",
        str(resource_request["max_cores"]),
        "--json",
    ]

    _append_xtb_scalar_options(command, manifest)
    _append_xtb_solvent_option(command, manifest)
    _append_xtb_optional_text_options(command, manifest)
    _append_xtb_job_type_options(
        command,
        manifest=manifest,
        secondary_input_xyz=secondary_input_xyz,
        job_type=job_type,
    )

    if _bool_flag(manifest, "dry_run"):
        command.append("--define")

    return command


def _resolve_existing_path(job_dir: Path, path_text: str) -> str:
    candidate = Path(path_text).expanduser()
    if not candidate.is_absolute():
        candidate = job_dir / candidate
    try:
        resolved = candidate.resolve()
    except OSError:
        return ""
    if not resolved.exists() or not resolved.is_file():
        return ""
    return str(resolved)


def _safe_float(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _load_xtbout_json(job_dir: Path) -> dict[str, Any]:
    path = job_dir / "xtbout.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_candidate_comment_energy(candidate_xyz: Path) -> float | None:
    try:
        lines = candidate_xyz.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return None
    if len(lines) < 2:
        return None
    match = _COMMENT_ENERGY_RE.search(lines[1])
    if not match:
        return None
    return _safe_float(match.group(1))


def _extract_sp_energy(job_dir: Path, candidate_xyz: Path) -> tuple[float | None, str]:
    xtbout = _load_xtbout_json(job_dir)
    for key in ("total energy", "electronic energy"):
        value = xtbout.get(key)
        if isinstance(value, (int, float)):
            return float(value), f"xtbout.json:{key}"
    if isinstance(xtbout.get("total energy"), str):
        value = _safe_float(xtbout["total energy"])
        if value is not None:
            return value, "xtbout.json:total energy"
    comment_energy = _parse_candidate_comment_energy(candidate_xyz)
    if comment_energy is not None:
        return comment_energy, "candidate_comment"
    return None, ""


def _ranking_top_n(manifest: dict[str, Any]) -> int:
    raw = manifest.get("top_n", 3)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = 3
    return max(1, value)


def _write_text(path: Path, text: str) -> str:
    path.write_text(text, encoding="utf-8")
    return str(path.resolve())


def _safe_rank_name(name: str, *, fallback: str) -> str:
    collapsed = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._-")
    return collapsed or fallback


def _run_candidate_sp_job(
    cfg: AppConfig,
    *,
    candidate_xyz: Path,
    candidate_run_dir: Path,
    manifest: dict[str, Any],
    should_cancel: Callable[[], bool] | None = None,
    on_running_job: Callable[[XtbRunningJob | None], None] | None = None,
    terminate_process: Callable[[subprocess.Popen[str]], None] | None = None,
) -> XtbRunResult:
    candidate_run_dir.mkdir(parents=True, exist_ok=True)
    candidate_input = candidate_run_dir / "input.xyz"
    shutil.copy2(candidate_xyz, candidate_input)
    candidate_manifest = dict(manifest)
    candidate_manifest["job_type"] = "sp"
    candidate_manifest["input_xyz"] = "input.xyz"
    candidate_manifest_path = candidate_run_dir / MANIFEST_FILE_NAME
    candidate_manifest_path.write_text(
        yaml.safe_dump(candidate_manifest, sort_keys=False), encoding="utf-8"
    )
    running = start_xtb_job(cfg, job_dir=candidate_run_dir, selected_input_xyz=candidate_input)
    if on_running_job is not None:
        on_running_job(running)
    try:
        process = getattr(running, "process", None)
        if process is None:
            return finalize_xtb_job(running)
        while True:
            if should_cancel is not None and should_cancel():
                if process.poll() is None:
                    if terminate_process is not None:
                        terminate_process(process)
                    else:
                        try:
                            process.terminate()
                        except Exception:
                            pass
                return finalize_xtb_job(
                    running,
                    forced_status="cancelled",
                    forced_reason="cancel_requested",
                )
            if process.poll() is not None:
                return finalize_xtb_job(running)
            time.sleep(1)
    finally:
        if on_running_job is not None:
            on_running_job(None)


def _ranking_candidate_run_dir(ranking_root: Path, index: int, candidate_path: Path) -> Path:
    name = _safe_rank_name(candidate_path.stem, fallback=f"candidate_{index:02d}")
    return ranking_root / f"{index:02d}_{name}"


def _run_ranking_candidate(
    cfg: AppConfig,
    *,
    candidate_path: Path,
    candidate_run_dir: Path,
    manifest: dict[str, Any],
    should_cancel: Callable[[], bool] | None,
    on_running_job: Callable[[XtbRunningJob | None], None] | None,
    terminate_process: Callable[[subprocess.Popen[str]], None] | None,
) -> XtbRunResult:
    if should_cancel is None and on_running_job is None and terminate_process is None:
        return _run_candidate_sp_job(
            cfg,
            candidate_xyz=candidate_path,
            candidate_run_dir=candidate_run_dir,
            manifest=manifest,
        )
    return _run_candidate_sp_job(
        cfg,
        candidate_xyz=candidate_path,
        candidate_run_dir=candidate_run_dir,
        manifest=manifest,
        should_cancel=should_cancel,
        on_running_job=on_running_job,
        terminate_process=terminate_process,
    )


def _ranking_candidate_result(
    *,
    candidate_path: Path,
    candidate_run_dir: Path,
    result: XtbRunResult,
    energy: float | None,
    energy_source: str,
) -> dict[str, Any]:
    return {
        "candidate_path": str(candidate_path.resolve()),
        "candidate_run_dir_path": str(candidate_run_dir.resolve()),
        "status": result.status,
        "reason": result.reason,
        "exit_code": result.exit_code,
        "selected_input_xyz": result.selected_input_xyz,
        "total_energy": energy,
        "energy_source": energy_source,
        "command": list(result.command),
        "analysis_summary": dict(result.analysis_summary),
    }


def _collect_ranking_candidate_results(
    cfg: AppConfig,
    *,
    ranking_root: Path,
    manifest: dict[str, Any],
    candidate_paths: list[Path],
    should_cancel: Callable[[], bool] | None,
    on_running_job: Callable[[XtbRunningJob | None], None] | None,
    terminate_process: Callable[[subprocess.Popen[str]], None] | None,
) -> tuple[list[dict[str, Any]], list[list[str]]]:
    candidate_results: list[dict[str, Any]] = []
    command_summary: list[list[str]] = []
    for index, candidate_path in enumerate(candidate_paths, start=1):
        if should_cancel is not None and should_cancel():
            break
        candidate_run_dir = _ranking_candidate_run_dir(ranking_root, index, candidate_path)
        result = _run_ranking_candidate(
            cfg,
            candidate_path=candidate_path,
            candidate_run_dir=candidate_run_dir,
            manifest=manifest,
            should_cancel=should_cancel,
            on_running_job=on_running_job,
            terminate_process=terminate_process,
        )
        energy, energy_source = _extract_sp_energy(candidate_run_dir, candidate_path)
        command_summary.append(list(result.command))
        candidate_results.append(
            _ranking_candidate_result(
                candidate_path=candidate_path,
                candidate_run_dir=candidate_run_dir,
                result=result,
                energy=energy,
                energy_source=energy_source,
            )
        )
        if result.status == "cancelled":
            break
    return candidate_results, command_summary


def _ranking_unsuccessful_detail(item: dict[str, Any], rank: int) -> dict[str, Any]:
    return {
        "rank": rank,
        "kind": "ranking_candidate",
        "path": item["candidate_path"],
        "candidate_run_dir_path": item["candidate_run_dir_path"],
        "energy_source": item["energy_source"],
        "status": item["status"],
        "reason": item["reason"],
        "exit_code": item["exit_code"],
        "selected": False,
    }


def _ranking_failure_analysis(
    *,
    candidate_results: list[dict[str, Any]],
    top_n: int,
    failure_reason: str,
) -> dict[str, Any]:
    return {
        "ranking_metric": "total_energy",
        "evaluated_candidate_count": len(candidate_results),
        "candidate_paths": [item["candidate_path"] for item in candidate_results],
        "candidate_run_dir_paths": [item["candidate_run_dir_path"] for item in candidate_results],
        "candidate_results": candidate_results,
        "top_n": top_n,
        "failure_reason": failure_reason,
    }


def _ranking_terminal_result(
    context: _RankingRunContext,
    *,
    status: str,
    reason: str,
    command: tuple[str, ...],
    stdout_text: str,
    stderr_text: str,
    candidate_results: list[dict[str, Any]],
) -> XtbRunResult:
    summary_stdout = context.job_dir / "ranking.stdout.log"
    summary_stderr = context.job_dir / "ranking.stderr.log"
    _write_text(summary_stdout, stdout_text)
    _write_text(summary_stderr, stderr_text)
    return XtbRunResult(
        status=status,
        reason=reason,
        command=command,
        exit_code=1,
        started_at=context.started_at,
        finished_at=now_utc_iso(),
        stdout_log=str(summary_stdout.resolve()),
        stderr_log=str(summary_stderr.resolve()),
        selected_input_xyz=str(context.candidate_paths[0].resolve()),
        job_type="ranking",
        reaction_key=str(context.inputs["reaction_key"]),
        input_summary=dict(context.inputs["input_summary"]),
        candidate_count=len(context.candidate_paths),
        selected_candidate_paths=(),
        candidate_details=tuple(
            _ranking_unsuccessful_detail(item, idx + 1)
            for idx, item in enumerate(candidate_results)
        ),
        analysis_summary=_ranking_failure_analysis(
            candidate_results=candidate_results,
            top_n=context.top_n,
            failure_reason=reason,
        ),
        manifest_path=str((context.job_dir / MANIFEST_FILE_NAME).resolve()),
        resource_request=context.resource_request,
        resource_actual=context.resource_actual,
    )


def _rank_usable_candidates(
    candidate_results: list[dict[str, Any]],
    *,
    top_n: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    ranked = sorted(candidate_results, key=lambda item: float(item["total_energy"]))
    candidate_details: list[dict[str, Any]] = []
    selected_paths: list[str] = []
    for rank, item in enumerate(ranked, start=1):
        is_selected = rank <= top_n
        candidate_details.append(
            {
                "rank": rank,
                "kind": "ranking_candidate",
                "path": item["candidate_path"],
                "candidate_run_dir_path": item["candidate_run_dir_path"],
                "energy_source": item["energy_source"],
                "total_energy": item["total_energy"],
                "score": round(-float(item["total_energy"]), 6),
                "status": item["status"],
                "reason": item["reason"],
                "exit_code": item["exit_code"],
                "selected": is_selected,
            }
        )
        if is_selected:
            selected_paths.append(item["candidate_path"])
    return ranked, candidate_details, selected_paths


def _write_ranking_success_logs(
    job_dir: Path,
    *,
    candidate_results: list[dict[str, Any]],
    selected_paths: list[str],
    usable_count: int,
    failed_count: int,
    best: dict[str, Any],
) -> tuple[Path, Path]:
    summary_stdout = job_dir / "ranking.stdout.log"
    summary_stderr = job_dir / "ranking.stderr.log"
    stdout_lines = [
        f"ranking completed: evaluated={len(candidate_results)} selected={len(selected_paths)}",
        f"best_candidate: {best['candidate_path']}",
        f"best_total_energy: {best['total_energy']}",
    ]
    if failed_count:
        stdout_lines.append(f"failed_candidates: {failed_count}")
    stdout_lines.append(f"usable_candidates: {usable_count}")
    _write_text(summary_stdout, "\n".join(stdout_lines) + "\n")
    _write_text(summary_stderr, "")
    return summary_stdout, summary_stderr


def _path_trial_from_match(match: re.Match[str]) -> dict[str, Any]:
    return {
        "trial_index": int(match.group(1)),
        "barrier_kcal": float(match.group(2)),
        "delta_e_kcal": float(match.group(3)),
        "product_end_rmsd": float(match.group(4)),
    }


def _apply_path_search_stdout_line(
    job_dir: Path, line: str, summary: dict[str, Any], trials: list[dict[str, Any]]
) -> None:
    if match := _TRIAL_RE.search(line):
        trials.append(_path_trial_from_match(match))
        return
    if match := _FORWARD_BARRIER_RE.search(line):
        summary["forward_barrier_kcal"] = float(match.group(1))
        return
    if match := _BACKWARD_BARRIER_RE.search(line):
        summary["backward_barrier_kcal"] = float(match.group(1))
        return
    if match := _REACTION_ENERGY_RE.search(line):
        summary["reaction_energy_kcal"] = float(match.group(1))
        return
    if match := _TS_FILE_RE.search(line):
        ts_guess_path = _resolve_existing_path(job_dir, match.group(1))
        if ts_guess_path:
            summary["ts_guess_path"] = ts_guess_path
        return
    if match := _POINT_COUNT_RE.search(line):
        summary["selected_path_index"] = int(match.group(1))
        summary["selected_path_point_count"] = int(match.group(2))


def run_xtb_ranking_job(
    cfg: AppConfig,
    *,
    job_dir: Path,
    should_cancel: Callable[[], bool] | None = None,
    on_running_job: Callable[[XtbRunningJob | None], None] | None = None,
    terminate_process: Callable[[subprocess.Popen[str]], None] | None = None,
) -> XtbRunResult:
    manifest = load_job_manifest(job_dir)
    inputs = resolve_job_inputs(job_dir, manifest)
    candidate_paths = [
        Path(path)
        for path in inputs.get("input_summary", {}).get("candidate_paths", [])
        if str(path).strip()
    ]
    if not candidate_paths:
        raise ValueError(f"No ranking candidates available in job directory: {job_dir}")

    ranking_root = job_dir / ".ranking_runs"
    ranking_root.mkdir(parents=True, exist_ok=True)
    top_n = _ranking_top_n(manifest)
    started_at = now_utc_iso()
    resource_request = _resource_request_dict(cfg, manifest)
    resource_actual = _resource_actual_dict(resource_request)
    context = _RankingRunContext(
        job_dir=job_dir,
        started_at=started_at,
        candidate_paths=candidate_paths,
        inputs=inputs,
        top_n=top_n,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )

    candidate_results, command_summary = _collect_ranking_candidate_results(
        cfg,
        ranking_root=ranking_root,
        manifest=manifest,
        candidate_paths=candidate_paths,
        should_cancel=should_cancel,
        on_running_job=on_running_job,
        terminate_process=terminate_process,
    )

    cancelled = any(item["status"] == "cancelled" for item in candidate_results) or (
        should_cancel is not None and should_cancel()
    )
    if cancelled:
        return _ranking_terminal_result(
            context,
            status="cancelled",
            reason="cancel_requested",
            command=tuple(command_summary[0]) if command_summary else tuple(),
            stdout_text="ranking cancelled: cancel_requested\n",
            stderr_text="",
            candidate_results=candidate_results,
        )

    usable = [
        item
        for item in candidate_results
        if item.get("total_energy") is not None and item["status"] == "completed"
    ]
    if not usable:
        failure_reason = "ranking_no_usable_energy"
        return _ranking_terminal_result(
            context,
            status="failed",
            reason=failure_reason,
            command=tuple(),
            stdout_text=f"ranking failed: {failure_reason}\n",
            stderr_text="no candidate produced a usable xTB energy\n",
            candidate_results=candidate_results,
        )

    ranked, candidate_details, selected_paths = _rank_usable_candidates(usable, top_n=top_n)
    selected = ranked[:top_n]
    failed_count = len(candidate_results) - len(usable)
    best = ranked[0]
    summary_stdout, summary_stderr = _write_ranking_success_logs(
        job_dir,
        candidate_results=candidate_results,
        selected_paths=selected_paths,
        usable_count=len(usable),
        failed_count=failed_count,
        best=best,
    )

    analysis_summary = {
        "ranking_metric": "total_energy",
        "evaluated_candidate_count": len(candidate_results),
        "usable_candidate_count": len(usable),
        "failed_candidate_count": failed_count,
        "candidate_paths": [item["candidate_path"] for item in candidate_results],
        "candidate_run_dir_paths": [item["candidate_run_dir_path"] for item in candidate_results],
        "candidate_results": candidate_results,
        "best_candidate_path": best["candidate_path"],
        "best_total_energy": best["total_energy"],
        "top_n": top_n,
        "selected_candidate_paths": list(selected_paths),
        "command_summary": command_summary,
    }
    return XtbRunResult(
        status="completed",
        reason="completed",
        command=tuple(selected[0]["command"])
        if selected
        else tuple(command_summary[0])
        if command_summary
        else tuple(),
        exit_code=0,
        started_at=started_at,
        finished_at=now_utc_iso(),
        stdout_log=str(summary_stdout.resolve()),
        stderr_log=str(summary_stderr.resolve()),
        selected_input_xyz=str(best["candidate_path"]),
        job_type="ranking",
        reaction_key=str(inputs["reaction_key"]),
        input_summary=dict(inputs["input_summary"]),
        candidate_count=len(candidate_results),
        selected_candidate_paths=tuple(selected_paths),
        candidate_details=tuple(candidate_details),
        analysis_summary=analysis_summary,
        manifest_path=str((job_dir / MANIFEST_FILE_NAME).resolve()),
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


def _parse_path_search_stdout(job_dir: Path, stdout_log: str) -> dict[str, Any]:
    path = Path(stdout_log)
    if not path.exists():
        return {}

    summary: dict[str, Any] = {}
    trials: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        _apply_path_search_stdout_line(job_dir, line, summary, trials)

    if trials:
        summary["path_trials"] = trials
    full_path = _resolve_existing_path(job_dir, "xtbpath.xyz")
    if full_path:
        summary["path_file"] = full_path
    selected_path = _resolve_existing_path(job_dir, "xtbpath_0.xyz")
    if selected_path:
        summary["selected_path_file"] = selected_path
    return summary


def _collect_path_search_candidates(
    job_dir: Path, stdout_log: str
) -> tuple[int, tuple[str, ...], tuple[dict[str, Any], ...], dict[str, Any]]:
    summary = _parse_path_search_stdout(job_dir, stdout_log)
    details: list[dict[str, Any]] = []

    ts_guess = summary.get("ts_guess_path")
    if ts_guess:
        details.append(
            {
                "rank": 1,
                "kind": "ts_guess",
                "path": ts_guess,
                "score": 1000.0,
                "selected": True,
            }
        )

    selected_path_file = summary.get("selected_path_file")
    if selected_path_file:
        details.append(
            {
                "rank": 2,
                "kind": "selected_path",
                "path": selected_path_file,
                "score": 900.0,
                "selected": True,
                "selected_path_index": summary.get("selected_path_index"),
                "selected_path_point_count": summary.get("selected_path_point_count"),
            }
        )

    # Keep the downstream contract intentionally small for path_search:
    # expose the explicit TS guess and the selected path artifact, while
    # retaining trial metrics in analysis_summary only.
    ordered_paths = [item["path"] for item in details if item.get("selected")]
    if not ordered_paths and summary.get("path_file"):
        ordered_paths = [str(summary["path_file"])]
    if ordered_paths:
        summary["selected_candidate_paths"] = list(ordered_paths)
    return len(details), tuple(ordered_paths), tuple(details), summary


def _collect_opt_candidates(
    job_dir: Path,
) -> tuple[int, tuple[str, ...], tuple[dict[str, Any], ...], dict[str, Any]]:
    optimized_geometry = _resolve_existing_path(job_dir, "xtbopt.xyz")
    summary = {
        "canonical_result_path": optimized_geometry,
        "optimization_log_path": _resolve_existing_path(job_dir, "xtbopt.log"),
        "optimization_ok": (job_dir / ".xtboptok").exists(),
    }
    if not optimized_geometry:
        return 0, (), (), summary
    detail = {
        "rank": 1,
        "kind": "optimized_geometry",
        "path": optimized_geometry,
        "score": 1000.0,
        "selected": True,
    }
    return 1, (optimized_geometry,), (detail,), summary


def _collect_sp_candidates(
    job_dir: Path,
) -> tuple[int, tuple[str, ...], tuple[dict[str, Any], ...], dict[str, Any]]:
    result_json = _resolve_existing_path(job_dir, "xtbout.json")
    xtbout = _load_xtbout_json(job_dir)
    summary: dict[str, Any] = {
        "canonical_result_path": result_json,
        "charges_path": _resolve_existing_path(job_dir, "charges"),
        "wbo_path": _resolve_existing_path(job_dir, "wbo"),
        "topology_path": _resolve_existing_path(job_dir, "xtbtopo.mol"),
    }
    if isinstance(xtbout.get("total energy"), (int, float)):
        summary["total_energy"] = float(xtbout["total energy"])
    if isinstance(xtbout.get("electronic energy"), (int, float)):
        summary["electronic_energy"] = float(xtbout["electronic energy"])
    if not result_json:
        return 0, (), (), summary
    detail = {
        "rank": 1,
        "kind": "single_point_result",
        "path": result_json,
        "score": 1000.0,
        "selected": True,
    }
    if "total_energy" in summary:
        detail["total_energy"] = summary["total_energy"]
        detail["score"] = round(-float(summary["total_energy"]), 6)
    return 1, (result_json,), (detail,), summary


def _preexec_with_limits(max_memory_gb: int):
    return process_utils.memory_limit_preexec(
        max_memory_gb,
        setrlimit_fn=resource.setrlimit,
        limit_resource=resource.RLIMIT_AS,
    )


def start_xtb_job(cfg: AppConfig, *, job_dir: Path, selected_input_xyz: Path) -> XtbRunningJob:
    manifest = load_job_manifest(job_dir)
    resource_request = _resource_request_dict(cfg, manifest)
    resource_actual = _resource_actual_dict(resource_request)
    inputs = resolve_job_inputs(job_dir, manifest)
    secondary_raw = inputs.get("secondary_input_xyz")
    secondary_input_xyz = None
    if secondary_raw:
        secondary_input_xyz = Path(str(secondary_raw)).expanduser().resolve()
    command = _build_command(
        cfg,
        manifest=manifest,
        selected_input_xyz=selected_input_xyz,
        secondary_input_xyz=secondary_input_xyz,
        job_type=str(inputs["job_type"]),
    )

    stdout_log = job_dir / "xtb.stdout.log"
    stderr_log = job_dir / "xtb.stderr.log"
    started_at = now_utc_iso()
    env = {
        **os.environ,
        "OMP_NUM_THREADS": str(resource_request["max_cores"]),
        "OPENBLAS_NUM_THREADS": str(resource_request["max_cores"]),
        "MKL_NUM_THREADS": str(resource_request["max_cores"]),
        "NUMEXPR_NUM_THREADS": str(resource_request["max_cores"]),
    }

    stdout_handle = stdout_log.open("w", encoding="utf-8")
    stderr_handle = stderr_log.open("w", encoding="utf-8")
    process = subprocess.Popen(
        command,
        cwd=job_dir,
        env=env,
        text=True,
        stdout=stdout_handle,
        stderr=stderr_handle,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        preexec_fn=_preexec_with_limits(resource_request["max_memory_gb"]),
    )
    return XtbRunningJob(
        process=process,
        command=tuple(command),
        started_at=started_at,
        stdout_log=str(stdout_log.resolve()),
        stderr_log=str(stderr_log.resolve()),
        stdout_handle=stdout_handle,
        stderr_handle=stderr_handle,
        selected_input_xyz=str(selected_input_xyz.resolve()),
        job_type=str(inputs["job_type"]),
        reaction_key=str(inputs["reaction_key"]),
        input_summary=dict(inputs["input_summary"]),
        manifest_path=str((job_dir / MANIFEST_FILE_NAME).resolve()),
        job_dir=str(job_dir.resolve()),
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


def finalize_xtb_job(
    running: XtbRunningJob,
    *,
    forced_status: str | None = None,
    forced_reason: str | None = None,
) -> XtbRunResult:
    try:
        running.stdout_handle.flush()
        running.stderr_handle.flush()
    finally:
        running.stdout_handle.close()
        running.stderr_handle.close()

    exit_code = running.process.poll()
    if exit_code is None:
        exit_code = running.process.wait()
    finished_at = now_utc_iso()

    if forced_status is not None:
        status = forced_status
    else:
        status = "completed" if exit_code == 0 else "failed"

    if forced_reason is not None:
        reason = forced_reason
    else:
        reason = "completed" if exit_code == 0 else f"xtb_exit_code_{exit_code}"

    if running.job_type == "path_search":
        candidate_count, candidate_paths, candidate_details, analysis_summary = (
            _collect_path_search_candidates(
                Path(running.job_dir),
                running.stdout_log,
            )
        )
    elif running.job_type == "opt":
        candidate_count, candidate_paths, candidate_details, analysis_summary = (
            _collect_opt_candidates(Path(running.job_dir))
        )
    elif running.job_type == "sp":
        candidate_count, candidate_paths, candidate_details, analysis_summary = (
            _collect_sp_candidates(Path(running.job_dir))
        )
    else:
        candidate_count, candidate_paths, candidate_details, analysis_summary = 0, (), (), {}

    return XtbRunResult(
        status=status,
        reason=reason,
        command=running.command,
        exit_code=int(exit_code),
        started_at=running.started_at,
        finished_at=finished_at,
        stdout_log=running.stdout_log,
        stderr_log=running.stderr_log,
        selected_input_xyz=running.selected_input_xyz,
        job_type=running.job_type,
        reaction_key=running.reaction_key,
        input_summary=dict(running.input_summary),
        candidate_count=candidate_count,
        selected_candidate_paths=candidate_paths,
        candidate_details=candidate_details,
        analysis_summary=analysis_summary,
        manifest_path=running.manifest_path,
        resource_request=running.resource_request,
        resource_actual=running.resource_actual,
    )
