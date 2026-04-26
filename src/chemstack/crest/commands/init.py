from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.scaffold import (
    ScaffoldFile,
    print_scaffold_report,
    resolve_scaffold_job_dir,
    write_scaffold_files,
)

from ..config import load_config


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

    job_dir = resolve_scaffold_job_dir(raw_root, cfg, engine="crest", engine_label="CREST")
    result = write_scaffold_files(
        [
            ScaffoldFile(job_dir / "input.xyz", _scaffold_xyz(), "input.xyz"),
            ScaffoldFile(job_dir / "crest_job.yaml", _scaffold_manifest(), "crest_job.yaml"),
            ScaffoldFile(job_dir / "README.md", _scaffold_readme(job_dir), "README.md"),
        ]
    )

    print_scaffold_report(job_dir, result)
    return 0
