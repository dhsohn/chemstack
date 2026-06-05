from __future__ import annotations

from typing import Any

from orca_auto.flow.engines.crest.queue_runtime_execution import (
    CrestQueueRuntimeWorkerExecutionCallbacks,
    build_queue_runtime_worker_execution_dependencies,
)


def _callable(name: str, calls: list[str]) -> Any:
    def _call(*_args: Any, **_kwargs: Any) -> str:
        calls.append(name)
        return name

    return _call


def _callbacks(calls: list[str]) -> CrestQueueRuntimeWorkerExecutionCallbacks:
    return CrestQueueRuntimeWorkerExecutionCallbacks(
        terminate_process=_callable("terminate", calls),
        wait_for_cancellable_process=_callable("wait", calls),
        sleep=_callable("sleep", calls),
        now_utc_iso=lambda: "2026-01-01T00:00:00+00:00",
        get_cancel_requested=_callable("get_cancel_requested", calls),
        mark_completed=_callable("mark_completed", calls),
        mark_cancelled=_callable("mark_cancelled", calls),
        mark_failed=_callable("mark_failed", calls),
        start_crest_job=_callable("start_crest_job", calls),
        finalize_crest_job=_callable("finalize_crest_job", calls),
        write_running_state=_callable("write_running_state", calls),
        write_execution_artifacts=_callable("write_execution_artifacts", calls),
        upsert_job_record=_callable("upsert_job_record", calls),
        notify_job_started=_callable("notify_job_started", calls),
        notify_job_finished=_callable("notify_job_finished", calls),
    )


def test_build_worker_execution_dependencies_maps_callback_groups() -> None:
    calls: list[str] = []
    callbacks = _callbacks(calls)

    deps = build_queue_runtime_worker_execution_dependencies(
        callbacks,
        cancel_check_interval_seconds=7,
    )

    assert deps.timing.now_utc_iso() == "2026-01-01T00:00:00+00:00"
    assert deps.runner.cancel_check_interval_seconds == 7
    assert deps.runner.start_crest_job is callbacks.start_crest_job
    assert deps.runner.finalize_crest_job is callbacks.finalize_crest_job
    assert deps.artifacts.write_running_state is callbacks.write_running_state
    assert deps.artifacts.write_execution_artifacts is callbacks.write_execution_artifacts
    assert deps.tracking.upsert_job_record is callbacks.upsert_job_record
    assert deps.tracking.notify_job_started is callbacks.notify_job_started
    assert deps.tracking.notify_job_finished is callbacks.notify_job_finished

    deps.queue.mark_failed("root", "queue-1")
    deps.runner.start_crest_job("cfg", job_dir="job", selected_xyz="input.xyz")

    assert calls == ["mark_failed", "start_crest_job"]
