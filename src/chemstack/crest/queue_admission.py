from __future__ import annotations

from chemstack.core.queue.internal_engine import InternalEngineSpec


_ADMISSION = InternalEngineSpec(engine="crest").admission()

reserve_admission_slot = _ADMISSION.reserve_admission_slot
start_background_job_process = _ADMISSION.start_background_job_process
mark_worker_start_error = _ADMISSION.mark_worker_start_error
attach_started_process = _ADMISSION.attach_started_process


__all__ = [
    "attach_started_process",
    "mark_worker_start_error",
    "reserve_admission_slot",
    "start_background_job_process",
]
