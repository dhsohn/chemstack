from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.paths import ensure_directory, require_subpath

from ..config import load_config

_SUPPORTED_JOB_TYPES = {"path_search", "opt", "sp", "ranking"}


def _write_if_missing(path: Path, content: str) -> bool:
    if path.exists():
        return False
    path.write_text(content, encoding="utf-8")
    return True


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
        raise ValueError(f"Unsupported init job_type: {job_type}")
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
        raise ValueError(f"Unsupported init job_type: {job_type}")

    return "\n".join(
        [
            "# chemstack xTB job scaffold",
            "",
            f"This directory was created by `python -m chemstack.xtb.cli init --root {job_dir} --job-type {job_type}`.",
            "",
            *layout_lines,
            "- Adjust `xtb_job.yaml` as needed.",
            "- Set `dry_run: false` when you are ready for a real run.",
            "- Then queue the directory with `python -m chemstack.xtb.cli run-dir <path>`.",
            "",
        ]
    )


def cmd_init(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    raw_root = str(getattr(args, "root", "")).strip()
    if not raw_root:
        print("error: init requires --root")
        return 1

    job_type = str(getattr(args, "job_type", "path_search")).strip().lower() or "path_search"
    if job_type not in _SUPPORTED_JOB_TYPES:
        print(f"error: unsupported init job_type: {job_type}")
        return 1

    allowed_root = ensure_directory(cfg.runtime.allowed_root, label="Allowed root")
    job_dir = require_subpath(Path(raw_root), allowed_root, label="Init root")
    job_dir.mkdir(parents=True, exist_ok=True)

    created: list[str] = []
    skipped: list[str] = []

    if job_type == "path_search":
        reactants_dir = job_dir / "reactants"
        products_dir = job_dir / "products"
        reactants_dir.mkdir(parents=True, exist_ok=True)
        products_dir.mkdir(parents=True, exist_ok=True)
        targets = [
            (reactants_dir / "r1.xyz", _scaffold_xyz("chemstack xTB scaffold reactant"), "reactants/r1.xyz"),
            (products_dir / "p1.xyz", _scaffold_xyz("chemstack xTB scaffold product"), "products/p1.xyz"),
            (job_dir / "xtb_job.yaml", _scaffold_manifest(job_type), "xtb_job.yaml"),
            (job_dir / "README.md", _scaffold_readme(job_dir, job_type), "README.md"),
        ]
    elif job_type == "ranking":
        candidates_dir = job_dir / "candidates"
        candidates_dir.mkdir(parents=True, exist_ok=True)
        targets = [
            (job_dir / "xtb_job.yaml", _scaffold_manifest(job_type), "xtb_job.yaml"),
            (job_dir / "README.md", _scaffold_readme(job_dir, job_type), "README.md"),
        ]
    else:
        targets = [
            (job_dir / "input.xyz", _scaffold_xyz(f"chemstack xTB scaffold {job_type}"), "input.xyz"),
            (job_dir / "xtb_job.yaml", _scaffold_manifest(job_type), "xtb_job.yaml"),
            (job_dir / "README.md", _scaffold_readme(job_dir, job_type), "README.md"),
        ]

    for path, content, label in targets:
        if _write_if_missing(path, content):
            created.append(label)
        else:
            skipped.append(label)

    print(f"job_dir: {job_dir}")
    print(f"job_type: {job_type}")
    print(f"created: {len(created)}")
    print(f"skipped: {len(skipped)}")
    for name in created:
        print(f"created_file: {name}")
    for name in skipped:
        print(f"skipped_file: {name}")
    return 0
