from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.paths import ensure_directory, require_subpath
from chemstack.flow.state import workflow_workspace_internal_engine_paths_from_path


@dataclass(frozen=True)
class ScaffoldFile:
    path: Path
    content: str
    label: str


@dataclass(frozen=True)
class ScaffoldWriteResult:
    created: tuple[str, ...]
    skipped: tuple[str, ...]


def resolve_scaffold_job_dir(
    raw_root: str | Path,
    cfg: Any,
    *,
    engine: str,
    engine_label: str,
) -> Path:
    workflow_root = str(getattr(cfg, "workflow_root", "")).strip()
    if workflow_root:
        runtime_paths = workflow_workspace_internal_engine_paths_from_path(
            raw_root,
            workflow_root=workflow_root,
            engine=engine,
        )
        if runtime_paths is None:
            raise ValueError(
                f"Init root must be under a workflow-local {engine_label} root: "
                f"<workflow.root>/<workflow_id>/<nn>_{engine}/..."
            )
        allowed_root = ensure_directory(runtime_paths["allowed_root"], label="Allowed root")
    else:
        allowed_root = ensure_directory(cfg.runtime.allowed_root, label="Allowed root")

    job_dir = require_subpath(Path(raw_root), allowed_root, label="Init root")
    job_dir.mkdir(parents=True, exist_ok=True)
    return job_dir


def write_if_missing(path: Path, content: str) -> bool:
    if path.exists():
        return False
    path.write_text(content, encoding="utf-8")
    return True


def write_scaffold_files(files: Iterable[ScaffoldFile]) -> ScaffoldWriteResult:
    created: list[str] = []
    skipped: list[str] = []

    for item in files:
        if write_if_missing(item.path, item.content):
            created.append(item.label)
        else:
            skipped.append(item.label)

    return ScaffoldWriteResult(created=tuple(created), skipped=tuple(skipped))


def print_scaffold_report(
    job_dir: Path,
    result: ScaffoldWriteResult,
    *,
    metadata: Sequence[tuple[str, object]] = (),
) -> None:
    print(f"job_dir: {job_dir}")
    for key, value in metadata:
        print(f"{key}: {value}")
    print(f"created: {len(result.created)}")
    print(f"skipped: {len(result.skipped)}")
    for name in result.created:
        print(f"created_file: {name}")
    for name in result.skipped:
        print(f"skipped_file: {name}")
