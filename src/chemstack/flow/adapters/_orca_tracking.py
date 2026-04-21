from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.indexing import JobLocationRecord


def _orca_module():
    from . import orca as o

    return o


def sibling_orca_auto_repo_root_impl() -> Path:
    o = _orca_module()
    return o.Path(__file__).resolve().parents[4]


def import_orca_auto_module_impl(module_name: str) -> Any | None:
    o = _orca_module()
    try:
        return o.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name not in {"chemstack", "chemstack.orca", module_name}:
            raise
        repo_root = o._sibling_orca_auto_repo_root()
        if repo_root.is_dir():
            for candidate in (repo_root / "src", repo_root):
                candidate_text = str(candidate)
                if candidate.is_dir() and candidate_text not in o.sys.path:
                    o.sys.path.insert(0, candidate_text)
            try:
                return o.import_module(module_name)
            except ModuleNotFoundError as retry_exc:
                if retry_exc.name not in {"chemstack", "chemstack.orca", module_name}:
                    raise
        return None


def orca_auto_tracking_module_impl() -> Any | None:
    o = _orca_module()
    return o._import_orca_auto_module("chemstack.orca.tracking")


def tracked_artifact_context_impl(
    *,
    index_root: Path | None,
    targets: tuple[str, ...],
) -> tuple[Path | None, JobLocationRecord | None, dict[str, Any], dict[str, Any], dict[str, Any]]:
    o = _orca_module()
    if index_root is None:
        return None, None, {}, {}, {}
    tracking_module = o._orca_auto_tracking_module()
    if tracking_module is None:
        return None, None, {}, {}, {}

    for raw_target in targets:
        target = o._normalize_text(raw_target)
        if not target:
            continue
        try:
            context = tracking_module.load_job_artifact_context(index_root, target)
        except Exception:
            continue
        job_dir = getattr(context, "job_dir", None)
        if job_dir is None:
            continue
        return (
            job_dir,
            getattr(context, "record", None),
            dict(context.state) if isinstance(getattr(context, "state", None), dict) else {},
            dict(context.report) if isinstance(getattr(context, "report", None), dict) else {},
            dict(context.organized_ref) if isinstance(getattr(context, "organized_ref", None), dict) else {},
        )
    return None, None, {}, {}, {}


def tracked_runtime_context_impl(
    *,
    index_root: Path | None,
    organized_root: Path | None,
    target: str,
    queue_id: str,
    run_id: str,
    reaction_dir: str,
) -> tuple[Path | None, JobLocationRecord | None, dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any] | None, Path | None] | None:
    o = _orca_module()
    if index_root is None:
        return None
    tracking_module = o._orca_auto_tracking_module()
    if tracking_module is None or not hasattr(tracking_module, "load_job_runtime_context"):
        return None

    try:
        context = tracking_module.load_job_runtime_context(
            index_root,
            target,
            organized_root=organized_root,
            queue_id=queue_id,
            run_id=run_id,
            reaction_dir=reaction_dir,
        )
    except Exception:
        return None

    artifact = getattr(context, "artifact", None)
    if artifact is None:
        return None

    queue_entry = getattr(context, "queue_entry", None)
    return (
        getattr(artifact, "job_dir", None),
        getattr(artifact, "record", None),
        dict(getattr(artifact, "state", {})) if isinstance(getattr(artifact, "state", None), dict) else {},
        dict(getattr(artifact, "report", {})) if isinstance(getattr(artifact, "report", None), dict) else {},
        dict(getattr(artifact, "organized_ref", {})) if isinstance(getattr(artifact, "organized_ref", None), dict) else {},
        dict(queue_entry) if isinstance(queue_entry, dict) else None,
        getattr(context, "organized_dir", None),
    )


def tracked_contract_payload_impl(
    *,
    index_root: Path | None,
    organized_root: Path | None,
    target: str,
    queue_id: str,
    run_id: str,
    reaction_dir: str,
) -> dict[str, Any] | None:
    o = _orca_module()
    if index_root is None:
        return None
    tracking_module = o._orca_auto_tracking_module()
    if tracking_module is None or not hasattr(tracking_module, "load_orca_contract_payload"):
        return None

    try:
        payload = tracking_module.load_orca_contract_payload(
            index_root,
            target,
            organized_root=organized_root,
            queue_id=queue_id,
            run_id=run_id,
            reaction_dir=reaction_dir,
        )
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if not any(
        o._normalize_text(payload.get(key))
        for key in ("run_id", "reaction_dir", "latest_known_path", "status", "queue_id")
    ):
        return None
    return {str(key): value for key, value in payload.items()}


__all__ = [
    "import_orca_auto_module_impl",
    "orca_auto_tracking_module_impl",
    "sibling_orca_auto_repo_root_impl",
    "tracked_artifact_context_impl",
    "tracked_contract_payload_impl",
    "tracked_runtime_context_impl",
]
