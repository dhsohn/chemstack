from __future__ import annotations

from pathlib import Path
from typing import Any


def matching_tracked_job_dirs(index_root: str | Path, target: str, *, deps: Any) -> list[Path]:
    target_text = deps.normalize_text(target)
    if not target_text:
        return []

    candidates: list[Path] = []
    seen: set[Path] = set()
    for record in deps.list_job_location_records(index_root):
        job_dir = deps.resolve_record_job_dir(record)
        if job_dir is None or job_dir in seen:
            continue

        state = deps.load_state(job_dir)
        report = deps.load_report_json(job_dir) or {}
        organized_ref = deps.load_organized_ref(job_dir) or {}

        if not organized_ref:
            original_dir = deps.resolve_existing_job_dir(record.original_run_dir)
            if original_dir is not None and original_dir != job_dir:
                organized_ref = deps.load_organized_ref(original_dir) or {}

        lookup_values = (
            record.job_id,
            report.get("job_id"),
            (state or {}).get("job_id"),
            organized_ref.get("job_id"),
            report.get("run_id"),
            (state or {}).get("run_id"),
            organized_ref.get("run_id"),
        )
        if any(deps.normalize_text(value) == target_text for value in lookup_values):
            seen.add(job_dir)
            candidates.append(job_dir)

    return candidates


def _dict_payload(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _initial_artifact_context(
    *,
    index_root: Path,
    target: str,
    run_id: str,
    reaction_dir: str,
    queue_entry: dict[str, Any] | None,
    deps: Any,
) -> Any:
    artifact = deps._first_artifact_context(index_root, (target, run_id, reaction_dir))
    queue_reaction_dir = deps.resolve_existing_job_dir(
        deps.queue_entry_metadata_value(queue_entry, "reaction_dir")
    )
    if artifact.job_dir is not None or queue_reaction_dir is None:
        return artifact
    return deps._first_artifact_context(
        index_root,
        (str(queue_reaction_dir), target, run_id, reaction_dir),
    )


def _hydrate_artifact_context(artifact: Any, *, deps: Any) -> Any:
    return deps._job_artifact_context(
        record=artifact.record,
        job_dir=artifact.job_dir,
        state=artifact.state,
        report=artifact.report,
        organized_ref=deps._hydrated_organized_ref(artifact),
    )


def _resolved_run_id(
    *,
    run_id: str,
    state: dict[str, Any],
    report: dict[str, Any],
    organized_ref: dict[str, Any],
    queue_entry: dict[str, Any] | None,
    deps: Any,
) -> str:
    return (
        deps.normalize_text(run_id)
        or deps.normalize_text(state.get("run_id"))
        or deps.normalize_text(report.get("run_id"))
        or deps.normalize_text(organized_ref.get("run_id"))
        or deps.normalize_text(deps.queue_entry_metadata_value(queue_entry, "run_id"))
    )


def _refresh_from_organized_dir(
    *,
    index_root: Path,
    artifact: Any,
    organized_dir: Path,
    target: str,
    resolved_run_id: str,
    reaction_dir: str,
    deps: Any,
) -> Any:
    refreshed = deps._first_artifact_context(
        index_root,
        (str(organized_dir), target, resolved_run_id, reaction_dir),
    )
    refreshed_dir = refreshed.job_dir or organized_dir
    return deps._job_artifact_context(
        record=refreshed.record or artifact.record,
        job_dir=refreshed_dir,
        state=refreshed.state or dict(deps.load_state(refreshed_dir) or {}),
        report=refreshed.report or deps.load_report_json(refreshed_dir),
        organized_ref=deps._hydrated_organized_ref(refreshed)
        or deps.load_organized_ref(refreshed_dir),
    )


def _needs_organized_refresh(
    *,
    organized_dir: Path | None,
    current_dir: Path | None,
    state: dict[str, Any],
    report: dict[str, Any],
) -> bool:
    return organized_dir is not None and (
        current_dir is None
        or not current_dir.exists()
        or (not state and not report)
    )


def load_job_runtime_context(
    index_root: str | Path,
    target: str,
    *,
    organized_root: str | Path | None = None,
    queue_id: str = "",
    run_id: str = "",
    reaction_dir: str = "",
    deps: Any,
) -> Any:
    del organized_root
    resolved_index_root = Path(index_root).expanduser().resolve()
    queue_entry = deps._find_queue_entry(
        index_root=resolved_index_root,
        target=target,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
    )

    artifact = _initial_artifact_context(
        index_root=resolved_index_root,
        target=target,
        run_id=run_id,
        reaction_dir=reaction_dir,
        queue_entry=queue_entry,
        deps=deps,
    )
    artifact = _hydrate_artifact_context(artifact, deps=deps)

    state_payload = _dict_payload(artifact.state)
    report_payload = _dict_payload(artifact.report)
    organized_ref_payload = _dict_payload(artifact.organized_ref)
    queue_reaction_dir = deps.resolve_existing_job_dir(
        deps.queue_entry_metadata_value(queue_entry, "reaction_dir")
    )
    current_dir = artifact.job_dir or deps.resolve_existing_job_dir(reaction_dir) or queue_reaction_dir

    resolved_run_id = _resolved_run_id(
        run_id=run_id,
        state=state_payload,
        report=report_payload,
        organized_ref=organized_ref_payload,
        queue_entry=queue_entry,
        deps=deps,
    )
    organized_dir = deps._record_organized_dir(artifact.record)

    if _needs_organized_refresh(
        organized_dir=organized_dir,
        current_dir=current_dir,
        state=state_payload,
        report=report_payload,
    ):
        artifact = _refresh_from_organized_dir(
            index_root=resolved_index_root,
            artifact=artifact,
            organized_dir=organized_dir,
            target=target,
            resolved_run_id=resolved_run_id,
            reaction_dir=reaction_dir,
            deps=deps,
        )

    return deps.JobRuntimeContext(
        artifact=artifact,
        queue_entry=queue_entry,
        organized_dir=organized_dir,
    )
