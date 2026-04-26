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

_SUPPORTED_JOB_TYPES = {"path_search", "opt", "sp", "ranking"}


def _scaffold_xyz(comment: str) -> str:
    return "\n".join(
        [
            "3",
            comment,
            "O 0.000000 0.000000 0.000000",
            "H 0.000000 0.000000 0.970000",
            "H 0.000000 0.750000 -0.240000",
            "",
        ]
    )


def _scaffold_manifest(job_type: str) -> str:
    if job_type == "path_search":
        body = [
            "# chemstack xTB scaffold manifest",
            "job_type: path_search",
            "gfn: 2",
            "charge: 0",
            "uhf: 0",
            "reactant_xyz: r1.xyz",
            "product_xyz: p1.xyz",
            "dry_run: true",
        ]
    elif job_type == "opt":
        body = [
            "# chemstack xTB scaffold manifest",
            "job_type: opt",
            "gfn: 2",
            "charge: 0",
            "uhf: 0",
            "input_xyz: input.xyz",
            "dry_run: true",
        ]
    elif job_type == "sp":
        body = [
            "# chemstack xTB scaffold manifest",
            "job_type: sp",
            "gfn: 2",
            "charge: 0",
            "uhf: 0",
            "input_xyz: input.xyz",
            "dry_run: true",
        ]
    elif job_type == "ranking":
        body = [
            "# chemstack xTB scaffold manifest",
            "job_type: ranking",
            "gfn: 2",
            "charge: 0",
            "uhf: 0",
            "candidates_dir: candidates",
            "top_n: 3",
            "dry_run: true",
        ]
    else:
        raise ValueError(f"Unsupported scaffold job_type: {job_type}")
    return "\n".join(
        [*body, ""]
    )


def _scaffold_readme(job_dir: Path, job_type: str) -> str:
    if job_type == "path_search":
        layout_lines = [
            "- Replace `reactants/r1.xyz` and `products/p1.xyz` with your structures.",
            "- This scaffold is for reaction path-search workflows.",
        ]
    elif job_type == "opt":
        layout_lines = [
            "- Replace `input.xyz` with the geometry you want to optimize.",
            "- This scaffold is for xTB geometry optimization workflows.",
        ]
    elif job_type == "sp":
        layout_lines = [
            "- Replace `input.xyz` with the geometry you want to evaluate.",
            "- This scaffold is for xTB single-point workflows.",
        ]
    elif job_type == "ranking":
        layout_lines = [
            "- Place candidate `.xyz` files under `candidates/`.",
            "- This scaffold is for low-cost xTB ranking or prescreening workflows.",
            "- The worker will evaluate the candidate set and rank them by xTB energy.",
        ]
    else:
        raise ValueError(f"Unsupported scaffold job_type: {job_type}")

    return "\n".join(
        [
            "# chemstack xTB job scaffold",
            "",
            "This directory is an internal xTB scaffold used by ChemStack workflow/runtime paths.",
            "",
            *layout_lines,
            "- Adjust `xtb_job.yaml` as needed.",
            "- Set `dry_run: false` when you are ready for a real run.",
            "- Queueing is handled by the internal xTB runtime or by workflow orchestration.",
            "",
        ]
    )


def cmd_init(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    raw_root = str(getattr(args, "root", "")).strip()
    if not raw_root:
        print("error: scaffold requires --root")
        return 1

    job_type = str(getattr(args, "job_type", "path_search")).strip().lower() or "path_search"
    if job_type not in _SUPPORTED_JOB_TYPES:
        print(f"error: unsupported scaffold job_type: {job_type}")
        return 1

    job_dir = resolve_scaffold_job_dir(raw_root, cfg, engine="xtb", engine_label="xTB")

    if job_type == "path_search":
        reactants_dir = job_dir / "reactants"
        products_dir = job_dir / "products"
        reactants_dir.mkdir(parents=True, exist_ok=True)
        products_dir.mkdir(parents=True, exist_ok=True)
        targets = [
            ScaffoldFile(
                reactants_dir / "r1.xyz",
                _scaffold_xyz("chemstack xTB scaffold reactant"),
                "reactants/r1.xyz",
            ),
            ScaffoldFile(
                products_dir / "p1.xyz",
                _scaffold_xyz("chemstack xTB scaffold product"),
                "products/p1.xyz",
            ),
            ScaffoldFile(job_dir / "xtb_job.yaml", _scaffold_manifest(job_type), "xtb_job.yaml"),
            ScaffoldFile(job_dir / "README.md", _scaffold_readme(job_dir, job_type), "README.md"),
        ]
    elif job_type == "ranking":
        candidates_dir = job_dir / "candidates"
        candidates_dir.mkdir(parents=True, exist_ok=True)
        targets = [
            ScaffoldFile(job_dir / "xtb_job.yaml", _scaffold_manifest(job_type), "xtb_job.yaml"),
            ScaffoldFile(job_dir / "README.md", _scaffold_readme(job_dir, job_type), "README.md"),
        ]
    else:
        targets = [
            ScaffoldFile(
                job_dir / "input.xyz",
                _scaffold_xyz(f"chemstack xTB scaffold {job_type}"),
                "input.xyz",
            ),
            ScaffoldFile(job_dir / "xtb_job.yaml", _scaffold_manifest(job_type), "xtb_job.yaml"),
            ScaffoldFile(job_dir / "README.md", _scaffold_readme(job_dir, job_type), "README.md"),
        ]

    result = write_scaffold_files(targets)
    print_scaffold_report(job_dir, result, metadata=(("job_type", job_type),))
    return 0
