from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.paths import ensure_directory, require_subpath

from ..config import load_config


def _write_if_missing(path: Path, content: str) -> bool:
    if path.exists():
        return False
    path.write_text(content, encoding="utf-8")
    return True


def _scaffold_xyz() -> str:
    return "\n".join(
        [
            "3",
            "chemstack CREST scaffold",
            "O 0.000000 0.000000 0.000000",
            "H 0.000000 0.000000 0.970000",
            "H 0.000000 0.750000 -0.240000",
            "",
        ]
    )


def _scaffold_manifest() -> str:
    return "\n".join(
        [
            "# chemstack CREST scaffold manifest",
            "mode: standard",
            "speed: quick",
            "gfn: 2",
            "input_xyz: input.xyz",
            "",
        ]
    )


def _scaffold_readme(job_dir: Path) -> str:
    return "\n".join(
        [
            "# chemstack CREST job scaffold",
            "",
            "This directory is an internal CREST scaffold used by ChemStack workflow/runtime paths.",
            "",
            "- Replace `input.xyz` with the molecule you want to process.",
            "- Adjust `crest_job.yaml` if you need NCI mode, charge, or solvent settings.",
            "- Queueing is handled by the internal CREST runtime or by workflow orchestration.",
            "",
        ]
    )


def cmd_init(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    raw_root = str(getattr(args, "root", "")).strip()
    if not raw_root:
        print("error: init requires --root")
        return 1

    allowed_root = ensure_directory(cfg.runtime.allowed_root, label="Allowed root")
    job_dir = require_subpath(Path(raw_root), allowed_root, label="Init root")
    job_dir.mkdir(parents=True, exist_ok=True)

    created: list[str] = []
    skipped: list[str] = []

    if _write_if_missing(job_dir / "input.xyz", _scaffold_xyz()):
        created.append("input.xyz")
    else:
        skipped.append("input.xyz")

    if _write_if_missing(job_dir / "crest_job.yaml", _scaffold_manifest()):
        created.append("crest_job.yaml")
    else:
        skipped.append("crest_job.yaml")

    if _write_if_missing(job_dir / "README.md", _scaffold_readme(job_dir)):
        created.append("README.md")
    else:
        skipped.append("README.md")

    print(f"job_dir: {job_dir}")
    print(f"created: {len(created)}")
    print(f"skipped: {len(skipped)}")
    for name in created:
        print(f"created_file: {name}")
    for name in skipped:
        print(f"skipped_file: {name}")
    return 0
