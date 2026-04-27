from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any

_WORKFLOW_TYPES = frozenset({"reaction_ts_search", "conformer_screening"})
_CREST_MODE_ALIASES = {
    "std": "standard",
    "standard": "standard",
    "nci": "nci",
}
_SHORTCUT_NAMES = {
    "reaction_ts_search": "ts_search",
    "conformer_screening": "conformer_search",
}


def _write_if_missing(path: Path, content: str) -> bool:
    if path.exists():
        return False
    path.write_text(content, encoding="utf-8")
    return True


def _xyz(content_label: str, *, delta: float = 0.0) -> str:
    return "\n".join(
        [
            "3",
            content_label,
            "O 0.000000 0.000000 0.000000",
            f"H 0.000000 0.000000 {0.970000 + delta:.6f}",
            "H 0.000000 0.750000 -0.240000",
            "",
        ]
    )


def _normalize_crest_mode(value: object) -> str:
    text = str(value or "").strip().lower()
    return _CREST_MODE_ALIASES.get(text, "")


def _shortcut_name(workflow_type: str) -> str:
    return _SHORTCUT_NAMES.get(workflow_type, "workflow")


def _manifest(workflow_type: str, crest_mode: str) -> str:
    if workflow_type == "reaction_ts_search":
        return "\n".join(
            [
                "# chemstack workflow scaffold manifest",
                "workflow_type: reaction_ts_search",
                "# Change to `nci` when you want NCI-mode CREST stages.",
                f"crest_mode: {crest_mode}",
                "# Optional CREST job overrides; uncomment when GFN2 pre-opt changes topology.",
                "# crest:",
                "#   gfn: ff",
                "#   no_preopt: true",
                "priority: 10",
                "# Each selected reactant/product CREST conformer pair becomes an xTB path search.",
                "max_crest_candidates: 3",
                "resources:",
                "  max_cores: 8",
                "  max_memory_gb: 32",
                "orca:",
                '  route_line: "! r2scan-3c OptTS Freq TightSCF"',
                "  charge: 0",
                "  multiplicity: 1",
                "",
            ]
        )
    if workflow_type == "conformer_screening":
        return "\n".join(
            [
                "# chemstack workflow scaffold manifest",
                "workflow_type: conformer_screening",
                "# Change to `nci` when you want NCI-mode CREST stages.",
                f"crest_mode: {crest_mode}",
                "# Optional CREST job overrides; uncomment when GFN2 pre-opt changes topology.",
                "# crest:",
                "#   gfn: ff",
                "#   no_preopt: true",
                "priority: 10",
                "# Up to 20 retained CREST conformers are handed off to ORCA by default.",
                "max_orca_stages: 20",
                "resources:",
                "  max_cores: 8",
                "  max_memory_gb: 32",
                "orca:",
                '  route_line: "! r2scan-3c Opt TightSCF"',
                "  charge: 0",
                "  multiplicity: 1",
                "",
            ]
        )
    raise ValueError(f"Unsupported workflow scaffold type: {workflow_type}")


def _readme(root: Path, workflow_type: str) -> str:
    if workflow_type == "reaction_ts_search":
        lines = [
            "- Replace `reactant.xyz` and `product.xyz` with your precomplex inputs.",
            "- Adjust `flow.yaml` before materializing the workflow.",
            "- Change `crest_mode: standard` to `crest_mode: nci` when you want NCI-mode CREST stages.",
            "- Put CREST overrides under `crest:` in `flow.yaml`, for example "
            "`gfn: ff` or `no_preopt: true` when GFN2 pre-opt changes topology.",
            "- reaction_ts_search expands all selected reactant x product CREST pairs into xTB path searches, waits for the xTB phase to finish, and then batches matching ORCA OptTS child jobs from retained ts_guess artifacts.",
        ]
    elif workflow_type == "conformer_screening":
        lines = [
            "- Replace `input.xyz` with the molecule you want to screen.",
            "- Adjust `flow.yaml` before materializing the workflow.",
            "- Change `crest_mode: standard` to `crest_mode: nci` when you want NCI-mode CREST stages.",
            "- Put CREST overrides under `crest:` in `flow.yaml`, for example "
            "`gfn: ff` or `no_preopt: true` when GFN2 pre-opt changes topology.",
            "- conformer_search hands off up to 20 retained CREST conformers to ORCA child jobs by default.",
        ]
    else:
        raise ValueError(f"Unsupported workflow scaffold type: {workflow_type}")

    shortcut_name = _shortcut_name(workflow_type)
    root_text = shlex.quote(str(root))
    return "\n".join(
        [
            "# chemstack workflow scaffold",
            "",
            f"This directory was created for `chemstack scaffold {shortcut_name} {root_text}`.",
            "",
            *lines,
            "- Then materialize it with `chemstack run-dir <path>`.",
            "",
        ]
    )


def cmd_scaffold(args: Any) -> int:
    raw_root = str(getattr(args, "root", "")).strip()
    if not raw_root:
        print("error: scaffold requires --root")
        return 1

    workflow_type = str(getattr(args, "workflow_type", "")).strip().lower()
    if workflow_type not in _WORKFLOW_TYPES:
        print(f"error: unsupported workflow scaffold type: {workflow_type}")
        return 1

    crest_mode = _normalize_crest_mode(getattr(args, "crest_mode", "standard"))
    if not crest_mode:
        print(f"error: unsupported crest_mode: {getattr(args, 'crest_mode', '')}")
        return 1

    root = Path(raw_root).expanduser().resolve()
    if root.exists() and not root.is_dir():
        print(f"error: scaffold root is not a directory: {root}")
        return 1
    root.mkdir(parents=True, exist_ok=True)

    created: list[str] = []
    skipped: list[str] = []
    targets: list[tuple[Path, str, str]]
    if workflow_type == "reaction_ts_search":
        targets = [
            (root / "reactant.xyz", _xyz("chemstack workflow scaffold reactant"), "reactant.xyz"),
            (root / "product.xyz", _xyz("chemstack workflow scaffold product", delta=0.05), "product.xyz"),
            (root / "flow.yaml", _manifest(workflow_type, crest_mode), "flow.yaml"),
            (root / "README.md", _readme(root, workflow_type), "README.md"),
        ]
    else:
        targets = [
            (root / "input.xyz", _xyz("chemstack workflow scaffold input"), "input.xyz"),
            (root / "flow.yaml", _manifest(workflow_type, crest_mode), "flow.yaml"),
            (root / "README.md", _readme(root, workflow_type), "README.md"),
        ]

    for path, content, label in targets:
        if _write_if_missing(path, content):
            created.append(label)
        else:
            skipped.append(label)

    print(f"workflow_dir: {root}")
    print(f"workflow_type: {workflow_type}")
    print(f"crest_mode: {crest_mode}")
    print(f"created: {len(created)}")
    print(f"skipped: {len(skipped)}")
    for name in created:
        print(f"created_file: {name}")
    for name in skipped:
        print(f"skipped_file: {name}")
    return 0
