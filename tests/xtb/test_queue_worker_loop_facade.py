from __future__ import annotations

from types import SimpleNamespace

from chemstack.core.queue.worker import ChildProcessQueueWorker, process_one_child_queue
from chemstack.xtb import queue_worker_loop


def test_queue_worker_loop_reexports_common_worker_types() -> None:
    assert queue_worker_loop.QueueWorker is ChildProcessQueueWorker
    assert queue_worker_loop.process_one is process_one_child_queue


def test_request_job_cancellation_uses_engine_terminator() -> None:
    calls: list[object] = []
    proc = SimpleNamespace(
        pid=123,
        send_signal=lambda signal_number: calls.append(("signal", signal_number)),
    )

    queue_worker_loop.request_job_cancellation(
        proc,
        cancel_signal=15,
        deps=SimpleNamespace(_terminate_process=lambda process: calls.append(("term", process))),
    )

    assert calls == [("signal", 15)]
