from __future__ import annotations

import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from chemstack.core.queue.worker import QueueWorkerLoop


@dataclass
class RunningJob:
    queue_root: Path
    entry: Any
    process: Any
    admission_token: str
    cancel_requested: bool = False
    started_at: float = field(default_factory=time.monotonic)


def build_background_worker_command(
    *,
    config_path: str,
    queue_root: Path,
    queue_id: str,
    admission_root: str,
    admission_token: str,
    auto_organize: bool,
    worker_job_module: str,
) -> list[str]:
    command = [
        sys.executable,
        "-m",
        worker_job_module,
        "--config",
        config_path,
        "--queue-root",
        str(queue_root),
        "--queue-id",
        queue_id,
        "--admission-root",
        str(admission_root),
        "--admission-token",
        admission_token,
    ]
    if auto_organize:
        command.append("--auto-organize")
    return command


def start_background_job_process(
    *,
    config_path: str,
    queue_root: Path,
    entry: Any,
    admission_root: str,
    admission_token: str,
    auto_organize: bool,
    worker_job_module: str,
) -> subprocess.Popen[str]:
    return subprocess.Popen(
        build_background_worker_command(
            config_path=config_path,
            queue_root=queue_root,
            queue_id=entry.queue_id,
            admission_root=admission_root,
            admission_token=admission_token,
            auto_organize=auto_organize,
            worker_job_module=worker_job_module,
        ),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        text=True,
    )


def request_job_cancellation(proc: Any, *, cancel_signal: int, deps: Any) -> None:
    try:
        send_signal = getattr(proc, "send_signal", None)
        if callable(send_signal):
            send_signal(cancel_signal)
        else:
            os.kill(proc.pid, cancel_signal)
    except (OSError, ProcessLookupError, PermissionError):
        deps._terminate_process(proc)


def resolve_worker_auto_organize(cfg: Any, args: Any) -> bool:
    auto_organize = bool(cfg.behavior.auto_organize_on_terminal)
    if bool(getattr(args, "auto_organize", False)):
        auto_organize = True
    elif bool(getattr(args, "no_auto_organize", False)):
        auto_organize = False
    return auto_organize


def config_path_for_worker(args: Any, *, default_config_path_fn: Any) -> str:
    configured = str(getattr(args, "config", "") or "").strip()
    return configured or default_config_path_fn()


class QueueWorker(QueueWorkerLoop):
    def __init__(
        self,
        cfg: Any,
        *,
        config_path: str,
        auto_organize: bool,
        max_concurrent: int | None = None,
        deps: Any,
    ) -> None:
        configured_max = cfg.runtime.max_concurrent if max_concurrent is None else max_concurrent
        super().__init__(
            max_concurrent=max(1, int(configured_max)),
            poll_interval_seconds=deps.POLL_INTERVAL_SECONDS,
            sleep_fn=lambda seconds: deps.time.sleep(seconds),
        )
        self.cfg = cfg
        self.config_path = config_path
        self.auto_organize = bool(auto_organize)
        self.admission_root = deps._admission_root(cfg)
        self.deps = deps

    def _before_run(self) -> None:
        self._reconcile_worker_state()

    def run_once(
        self,
        *,
        idle_message: str | None = "No pending jobs.",
        blocked_message: str | None = "status: waiting_for_slot",
    ) -> int:
        return super().run_once(
            idle_message=idle_message,
            blocked_message=blocked_message,
        )

    def _reserve_next_entry(self) -> tuple[str, Any | None]:
        deps = self.deps
        return deps.reserve_dequeued_entry(
            self.cfg,
            admission_root=self.admission_root,
            reserve_slot_fn=deps._try_reserve_admission_slot,
            dequeue_next_fn=deps._dequeue_next_entry,
            release_slot_fn=deps.release_slot,
        )

    def _start_reserved(self, reserved: Any) -> None:
        self._start_job(
            reserved.queue_root,
            reserved.entry,
            admission_token=reserved.admission_token,
        )

    def _start_job(self, queue_root: Path, entry: Any, *, admission_token: str) -> None:
        deps = self.deps
        try:
            proc = deps._start_background_job_process(
                config_path=self.config_path,
                queue_root=queue_root,
                entry=entry,
                admission_root=self.admission_root,
                admission_token=admission_token,
                auto_organize=self.auto_organize,
            )
        except OSError as exc:
            deps.release_slot(self.admission_root, admission_token)
            failure = deps._build_terminal_result(
                entry,
                job_dir=deps._job_dir(entry),
                selected_xyz=deps._selected_xyz(entry),
                job_type=deps._job_type(entry),
                reaction_key=deps._reaction_key(entry, deps._job_dir(entry)),
                input_summary=deps._input_summary(entry),
                resource_request=deps._entry_resource_request(self.cfg, entry),
                status="failed",
                reason=f"worker_start_error:{exc}",
            )
            deps._finalize_execution_result(
                self.cfg,
                queue_root=queue_root,
                entry=entry,
                result=failure,
                auto_organize=self.auto_organize,
                emit_output=True,
            )
            return

        self._running[entry.queue_id] = RunningJob(
            queue_root=queue_root,
            entry=entry,
            process=proc,
            admission_token=admission_token,
        )

    def _poll_job(self, job: Any) -> int | None:
        return job.process.poll()

    def _finalize_completed_job(self, _queue_id: str, job: Any, rc: int) -> None:
        deps = self.deps
        summary = deps._load_terminal_summary(job.queue_root, job.entry, rc=rc)
        deps._ensure_terminal_queue_status(job.queue_root, job.entry, summary)
        deps._print_terminal_summary(summary)
        deps.release_slot(self.admission_root, job.admission_token)

    def _check_cancel_requests(self) -> None:
        deps = self.deps
        for job in self._running.values():
            if job.cancel_requested:
                continue
            if deps.get_cancel_requested(str(job.queue_root), job.entry.queue_id):
                deps._request_job_cancellation(job.process)
                job.cancel_requested = True

    def _shutdown_all(self) -> None:
        if not self._running:
            return
        deps = self.deps
        for queue_id, job in list(self._running.items()):
            deps._terminate_process(job.process)
            deps._mark_recovery_pending_state(self.cfg, job.entry, reason="worker_shutdown")
            deps.requeue_running_entry(str(job.queue_root), queue_id)
            deps.release_slot(self.admission_root, job.admission_token)
            del self._running[queue_id]

    def _reconcile_worker_state(self) -> None:
        deps = self.deps
        deps.reconcile_stale_slots(self.admission_root)
        for queue_root, entry in deps._queue_entries_with_roots(self.cfg):
            status = str(getattr(getattr(entry, "status", None), "value", "")).strip().lower()
            if status != "running":
                continue
            summary = deps._load_terminal_summary(queue_root, entry)
            if summary.status in {"completed", "failed", "cancelled"}:
                deps._ensure_terminal_queue_status(queue_root, entry, summary)
                continue

            state = deps.load_state(deps._job_dir(entry)) or {}
            worker_job_pid = int(state.get("worker_job_pid", 0) or 0)
            if worker_job_pid and deps._pid_is_alive(worker_job_pid):
                continue
            deps.requeue_running_entry(str(queue_root), entry.queue_id)
            deps._mark_recovery_pending_state(self.cfg, entry, reason="crashed_recovery")


def process_one(cfg: Any, *, auto_organize: bool, deps: Any) -> str:
    slot_token = deps._try_reserve_admission_slot(cfg)
    if slot_token is None:
        return "blocked"

    try:
        dequeued = deps._dequeue_next_entry(cfg)
        if dequeued is None:
            return "idle"
        queue_root, entry = dequeued
        deps._execute_queue_entry(
            cfg,
            queue_root=queue_root,
            entry=entry,
            auto_organize=auto_organize,
            emit_output=True,
        )
        return "processed"
    finally:
        deps.release_slot(deps._admission_root(cfg), slot_token)
