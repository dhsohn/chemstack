from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from chemstack.core.app_ids import CHEMSTACK_ORCA_SOURCE

from ._activity_model import ActivityCancelRequest, ActivityRecord, ResolvedActivitySources
from .submitters.common import normalize_text


@dataclass(frozen=True)
class ActivityCancelProvider:
    source: str
    cancel: Callable[[ActivityRecord, ResolvedActivitySources, ActivityCancelRequest], dict[str, Any]]


def match_activity_record(records: list[ActivityRecord], target: str) -> ActivityRecord:
    normalized_target = normalize_text(target)
    if not normalized_target:
        raise ValueError("Cancel target is empty.")

    exact_matches = [
        record
        for record in records
        if normalized_target in {record.activity_id, record.cancel_target}
    ]
    if len(exact_matches) == 1:
        return exact_matches[0]
    if len(exact_matches) > 1:
        raise ValueError(
            f"Ambiguous activity target: {normalized_target}. Matches: "
            + ", ".join(sorted(record.activity_id for record in exact_matches))
        )

    alias_matches = [record for record in records if normalized_target in set(record.aliases)]
    if len(alias_matches) == 1:
        return alias_matches[0]
    if len(alias_matches) > 1:
        raise ValueError(
            f"Ambiguous activity target: {normalized_target}. Matches: "
            + ", ".join(sorted(record.activity_id for record in alias_matches))
        )
    raise LookupError(f"Activity target not found: {normalized_target}")


def cancel_activity_payload(
    record: ActivityRecord,
    result: dict[str, Any],
    *,
    fallback_status: str,
) -> dict[str, Any]:
    return {
        "activity_id": record.activity_id,
        "kind": record.kind,
        "engine": record.engine,
        "source": record.source,
        "label": record.label,
        "status": normalize_text(result.get("status")) or fallback_status,
        "cancel_target": record.cancel_target,
        "result": result,
    }


def cancel_workflow_activity(
    record: ActivityRecord,
    resolved: ResolvedActivitySources,
    request: ActivityCancelRequest,
) -> dict[str, Any]:
    from .operations import cancel_workflow

    return cancel_workflow(
        target=record.cancel_target,
        workflow_root=resolved.workflow_root,
        crest_config=resolved.crest_config,
        crest_executable=request.engine_options.crest.executable,
        crest_repo_root=request.engine_options.crest.repo_root,
        xtb_config=resolved.xtb_config,
        xtb_executable=request.engine_options.xtb.executable,
        xtb_repo_root=request.engine_options.xtb.repo_root,
        orca_config=resolved.orca_config,
        orca_executable=request.engine_options.orca.executable,
        orca_repo_root=request.engine_options.orca.repo_root,
    )


def cancel_crest_activity(
    record: ActivityRecord,
    resolved: ResolvedActivitySources,
    request: ActivityCancelRequest,
    *,
    deps: Any,
) -> dict[str, Any]:
    config_path = normalize_text(resolved.crest_config)
    if not config_path:
        raise ValueError("crest_config is required to cancel crest activities.")
    return deps.cancel_crest_target(
        target=record.cancel_target,
        config_path=config_path,
        executable=request.engine_options.crest.executable,
        repo_root=request.engine_options.crest.repo_root,
    )


def cancel_xtb_activity(
    record: ActivityRecord,
    resolved: ResolvedActivitySources,
    request: ActivityCancelRequest,
    *,
    deps: Any,
) -> dict[str, Any]:
    config_path = normalize_text(resolved.xtb_config)
    if not config_path:
        raise ValueError("xtb_config is required to cancel xtb activities.")
    return deps.cancel_xtb_target(
        target=record.cancel_target,
        config_path=config_path,
        executable=request.engine_options.xtb.executable,
        repo_root=request.engine_options.xtb.repo_root,
    )


def cancel_orca_activity(
    record: ActivityRecord,
    resolved: ResolvedActivitySources,
    request: ActivityCancelRequest,
    *,
    deps: Any,
) -> dict[str, Any]:
    config_path = normalize_text(resolved.orca_config)
    if not config_path:
        raise ValueError("chemstack_config is required to cancel chemstack ORCA activities.")
    return deps.cancel_orca_target(
        target=record.cancel_target,
        config_path=config_path,
        executable=request.engine_options.orca.executable,
        repo_root=deps._discover_orca_repo_root(request.engine_options.orca.repo_root),
    )


def cancel_providers(deps: Any) -> tuple[ActivityCancelProvider, ...]:
    return (
        ActivityCancelProvider(
            "chemstack_crest",
            lambda record, resolved, request: cancel_crest_activity(
                record,
                resolved,
                request,
                deps=deps,
            ),
        ),
        ActivityCancelProvider(
            "chemstack_xtb",
            lambda record, resolved, request: cancel_xtb_activity(
                record,
                resolved,
                request,
                deps=deps,
            ),
        ),
        ActivityCancelProvider(
            CHEMSTACK_ORCA_SOURCE,
            lambda record, resolved, request: cancel_orca_activity(
                record,
                resolved,
                request,
                deps=deps,
            ),
        ),
    )


def cancel_non_workflow_activity(
    record: ActivityRecord,
    resolved: ResolvedActivitySources,
    request: ActivityCancelRequest,
    *,
    deps: Any,
) -> dict[str, Any]:
    for provider in cancel_providers(deps):
        if record.source == provider.source:
            return provider.cancel(record, resolved, request)
    raise ValueError(f"Unsupported activity source: {record.source}")
