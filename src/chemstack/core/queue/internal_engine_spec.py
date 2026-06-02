from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from .internal_engine_child import InternalEngineWorkerChild
from .internal_engine_policies import InternalEngineAdmission, InternalEngineLifecycle
from .internal_engine_status import entry_status_is_running


@dataclass(frozen=True)
class InternalEngineSpec:
    engine: str
    worker_job_module: str = ""
    worker_pid_file_name: str = ""
    include_admission_root: bool = False

    def admission(self) -> InternalEngineAdmission:
        return InternalEngineAdmission(
            engine=self.engine,
            include_admission_root=self.include_admission_root,
        )

    def lifecycle(self) -> InternalEngineLifecycle:
        return InternalEngineLifecycle()

    def worker_child(
        self,
        shutdown_exception_type: type[BaseException],
        *,
        entry_ready_fn: Callable[[Any], bool] = entry_status_is_running,
        process_dequeued_entry_kwargs_fn: Callable[[], Mapping[str, Any]] | None = None,
        outcome_exit_code_fn: Callable[[Any], int] | None = None,
    ) -> InternalEngineWorkerChild:
        if not self.worker_job_module:
            raise ValueError("worker_job_module is required for worker child support")
        return InternalEngineWorkerChild(
            worker_job_module=self.worker_job_module,
            include_admission_root=self.include_admission_root,
            shutdown_exception_type=shutdown_exception_type,
            entry_ready_fn=entry_ready_fn,
            process_dequeued_entry_kwargs_fn=process_dequeued_entry_kwargs_fn,
            outcome_exit_code_fn=outcome_exit_code_fn,
        )


__all__ = ["InternalEngineSpec"]
