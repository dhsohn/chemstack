from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from chemstack.core.utils import normalize_text

from .internal_engine_cancellation import cancel_engine_target
from .internal_engine_models import InternalEngineSubmitterDeps, InternalEngineSubmitterSpec
from .internal_engine_submission import submit_engine_job_dir


@dataclass(frozen=True)
class InternalEngineSubmitter:
    spec: InternalEngineSubmitterSpec
    deps_factory: Callable[[], InternalEngineSubmitterDeps]

    def submit_job_dir(
        self,
        *,
        job_dir: str,
        priority: int,
        config_path: str,
    ) -> dict[str, Any]:
        return submit_engine_job_dir(
            spec=self.spec,
            deps=self.deps_factory(),
            config_path=config_path,
            job_dir=job_dir,
            priority=priority,
        )

    def cancel_target(
        self,
        *,
        target: str,
        config_path: str,
    ) -> dict[str, Any]:
        return cancel_engine_target(
            spec=self.spec,
            deps=self.deps_factory(),
            config_path=config_path,
            target=target,
        )


def submitter_deps_from_namespace(namespace: Mapping[str, Any]) -> InternalEngineSubmitterDeps:
    """Compatibility adapter; prefer constructing InternalEngineSubmitterDeps explicitly."""
    return InternalEngineSubmitterDeps(
        load_config_fn=namespace["load_config"],
        resolve_job_dir_fn=namespace["resolve_job_dir"],
        load_manifest_fn=namespace["load_job_manifest"],
        build_submission_fn=namespace["build_submission"],
        record_queued_fn=namespace["record_queued"],
        enqueue_fn=namespace["enqueue"],
        load_queue_config_fn=namespace["load_queue_config"],
        queue_entries_with_roots_fn=namespace["queue_entries_with_roots"],
        request_cancel_fn=namespace["request_cancel"],
        display_status_fn=namespace["display_status"],
    )


def build_internal_engine_submitter(
    *,
    run_dir_api_name: str,
    cancel_api_name: str,
    deps_factory: Callable[[], InternalEngineSubmitterDeps] | None = None,
    namespace: Mapping[str, Any] | None = None,
    extra_fields_fn: Callable[[Any | None, Any | None], dict[str, Any]] | None = None,
) -> tuple[Callable[..., dict[str, Any]], Callable[..., dict[str, Any]]]:
    if deps_factory is not None and namespace is not None:
        raise ValueError("pass either deps_factory or namespace, not both")
    if deps_factory is None:
        if namespace is None:
            raise ValueError("build_internal_engine_submitter requires deps_factory or namespace")
        namespace_ref = namespace

        def namespace_deps_factory() -> InternalEngineSubmitterDeps:
            return submitter_deps_from_namespace(namespace_ref)

        deps_factory = namespace_deps_factory

    submitter = InternalEngineSubmitter(
        spec=InternalEngineSubmitterSpec(
            run_dir_api_name=run_dir_api_name,
            cancel_api_name=cancel_api_name,
            extra_fields_fn=extra_fields_fn,
        ),
        deps_factory=deps_factory,
    )
    return submitter.submit_job_dir, submitter.cancel_target


def build_internal_engine_module_submitter(
    *,
    engine: str,
    namespace: Mapping[str, Any],
    extra_fields_fn: Callable[[Any | None, Any | None], dict[str, Any]] | None = None,
) -> tuple[Callable[..., dict[str, Any]], Callable[..., dict[str, Any]]]:
    engine_name = normalize_text(engine)
    if not engine_name:
        raise ValueError("build_internal_engine_module_submitter requires engine")
    return build_internal_engine_submitter(
        run_dir_api_name=f"chemstack.{engine_name}.submission.direct_enqueue",
        cancel_api_name=f"chemstack.{engine_name}.queue_runtime.direct_cancel",
        namespace=namespace,
        extra_fields_fn=extra_fields_fn,
    )
