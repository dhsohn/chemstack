from __future__ import annotations

import os
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from chemstack.core.admission import (
    activate_reserved_slot,
    list_slots,
    reconcile_stale_slots,
    release_slot,
    reserve_slot,
)
from chemstack.core.queue import (
    dequeue_next,
    get_cancel_requested,
    list_queue,
    mark_cancelled,
    mark_completed,
    mark_failed,
    requeue_running_entry,
    request_cancel,
)
from chemstack.core.queue.types import QueueStatus
from chemstack.core.utils import now_utc_iso

from ..config import default_config_path, load_config
from ..job_locations import runtime_roots_for_cfg, upsert_job_record
from ..notifications import notify_job_finished, notify_job_started
from ..runner import finalize_crest_job, start_crest_job
from ..worker_execution import (
    WorkerExecutionDependencies,
    _molecule_key,
    _resource_caps,
    _terminate_process,
    _write_execution_artifacts,
    _write_running_state,
    build_worker_child_command,
    process_dequeued_entry,
)
from .organize import organize_job_dir

POLL_INTERVAL_SECONDS = 5
WORKER_PID_FILE = "queue_worker.pid"
WORKER_SHUTDOWN_GRACE_SECONDS = 10.0


@dataclass
class _RunningJob:
    queue_root: Path
    entry: Any
    process: subprocess.Popen[str]
    admission_token: str
    started_at: float = field(default_factory=time.monotonic)


def _display_status(entry: Any) -> str:
    status_value = getattr(getattr(entry, "status", None), "value", None)
    normalized = str(status_value).strip() or "unknown"
    if getattr(entry, "cancel_requested", False) and normalized == "running":
        return "cancel_requested"
    return normalized


def _find_entry_by_target(entries: list[Any], target: str) -> Any | None:
    for entry in entries:
        if entry.queue_id == target or entry.task_id == target:
            return entry
    return None


def _queue_roots(cfg: Any) -> tuple[Path, ...]:
    try:
        return tuple(runtime_roots_for_cfg(cfg))
    except Exception:
        return (Path(cfg.runtime.allowed_root).expanduser().resolve(),)


def _queue_entries_with_roots(cfg: Any) -> list[tuple[Path, Any]]:
    rows: list[tuple[Path, Any]] = []
    for root in _queue_roots(cfg):
        for entry in list_queue(root):
            rows.append((root, entry))
    return rows


def _find_queue_entry(queue_root: Path, queue_id: str) -> Any | None:
    for entry in list_queue(queue_root):
        if entry.queue_id == queue_id:
            return entry
    return None


def _dequeue_next_entry(cfg: Any) -> tuple[Path, Any] | None:
    roots = _queue_roots(cfg)
    if len(roots) == 1:
        entry = dequeue_next(roots[0])
        if entry is None:
            return None
        return roots[0], entry

    selected_root: Path | None = None
    selected_key: tuple[int, str, int, str] | None = None

    for root_index, root in enumerate(roots):
        for entry in list_queue(root):
            status_value = getattr(getattr(entry, "status", None), "value", None)
            status = str(status_value).strip().lower()
            if status != "pending" or getattr(entry, "cancel_requested", False):
                continue
            key = (
                int(getattr(entry, "priority", 10) or 10),
                str(getattr(entry, "enqueued_at", "")),
                root_index,
                str(getattr(entry, "queue_id", "")),
            )
            if selected_key is None or key < selected_key:
                selected_key = key
                selected_root = root

    if selected_root is None:
        return None

    entry = dequeue_next(selected_root)
    if entry is None:
        return None
    return selected_root, entry


def _admission_root_for_cfg(cfg: Any) -> str:
    return str(
        getattr(cfg.runtime, "resolved_admission_root", None)
        or getattr(cfg.runtime, "admission_root", "")
        or cfg.runtime.allowed_root
    )


def _admission_limit_for_cfg(cfg: Any) -> int:
    raw = getattr(cfg.runtime, "resolved_admission_limit", None)
    if raw in {None, "", 0}:
        raw = getattr(cfg.runtime, "admission_limit", None)
    if raw in {None, "", 0}:
        raw = getattr(cfg.runtime, "max_concurrent", 1)
    try:
        return max(1, int(raw if raw is not None else 1))
    except (TypeError, ValueError):
        return 1


def _try_reserve_admission_slot(cfg: Any) -> str | None:
    return reserve_slot(
        _admission_root_for_cfg(cfg),
        _admission_limit_for_cfg(cfg),
        source="chemstack.crest.queue_worker",
        app_name="crest_auto",
    )


def _worker_dependencies() -> WorkerExecutionDependencies:
    return WorkerExecutionDependencies(
        now_utc_iso=now_utc_iso,
        get_cancel_requested=get_cancel_requested,
        start_crest_job=start_crest_job,
        finalize_crest_job=finalize_crest_job,
        terminate_process=_terminate_process,
        write_running_state=_write_running_state,
        write_execution_artifacts=_write_execution_artifacts,
        mark_completed=mark_completed,
        mark_cancelled=mark_cancelled,
        mark_failed=mark_failed,
        upsert_job_record=upsert_job_record,
        notify_job_started=notify_job_started,
        notify_job_finished=notify_job_finished,
        organize_job_dir=organize_job_dir,
    )


def cmd_queue_cancel(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    target = str(getattr(args, "target", "")).strip()
    if not target:
        print("error: queue cancel requires a queue_id or job_id")
        return 1

    entry_with_root = None
    for queue_root, entry in _queue_entries_with_roots(cfg):
        if entry.queue_id == target or entry.task_id == target:
            entry_with_root = (queue_root, entry)
            break
    if entry_with_root is None:
        print(f"error: queue target not found: {target}")
        return 1
    queue_root, entry = entry_with_root

    updated = request_cancel(queue_root, entry.queue_id)
    if updated is None:
        print(f"error: queue target already terminal: {target}")
        return 1

    print(f"status: {_display_status(updated)}")
    print(f"queue_id: {updated.queue_id}")
    print(f"job_id: {updated.task_id}")
    return 0


def _process_one(cfg: Any, *, auto_organize: bool) -> str:
    slot_token = _try_reserve_admission_slot(cfg)
    if slot_token is None:
        return "blocked"

    try:
        dequeued = _dequeue_next_entry(cfg)
        if dequeued is None:
            return "idle"
        queue_root, entry = dequeued
        outcome = process_dequeued_entry(
            cfg,
            entry,
            queue_root=queue_root,
            auto_organize=auto_organize,
            resource_caps=_resource_caps,
            molecule_key_resolver=_molecule_key,
            dependencies=_worker_dependencies(),
        )

        print(f"queue_id: {entry.queue_id}")
        print(f"job_id: {entry.task_id}")
        print(f"status: {outcome.result.status}")
        print(f"reason: {outcome.result.reason}")
        return "processed"
    finally:
        release_slot(_admission_root_for_cfg(cfg), slot_token)


def read_worker_pid(allowed_root: Path) -> int | None:
    pid_path = allowed_root / WORKER_PID_FILE
    if not pid_path.exists():
        return None
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None
    try:
        os.kill(pid, 0)
    except OSError:
        try:
            pid_path.unlink()
        except OSError:
            pass
        return None
    return pid


class QueueWorker:
    def __init__(
        self,
        cfg: Any,
        config_path: str,
        *,
        max_concurrent: int,
        auto_organize: bool,
    ) -> None:
        self.cfg = cfg
        self.config_path = str(config_path).strip() or default_config_path()
        self.max_concurrent = max(1, int(max_concurrent))
        self.auto_organize = bool(auto_organize)
        self.allowed_root = Path(str(cfg.runtime.allowed_root)).expanduser().resolve()
        self.admission_root = Path(_admission_root_for_cfg(cfg)).expanduser().resolve()
        self._running: dict[str, _RunningJob] = {}
        self._shutdown_requested = False

    def run(self) -> int:
        self._install_signal_handlers()
        self._write_pid_file()
        self._reconcile_orphaned_running()

        try:
            while not self._shutdown_requested:
                self._run_iteration()
        except KeyboardInterrupt:
            self._shutdown_requested = True
        finally:
            self._shutdown_all()
            self._remove_pid_file()
        return 0

    def _run_iteration(self) -> None:
        self._check_completed_jobs()
        if self._shutdown_requested:
            return
        self._fill_slots()
        if self._shutdown_requested:
            return
        time.sleep(POLL_INTERVAL_SECONDS)

    def _reconcile_orphaned_running(self) -> None:
        reconcile_stale_slots(self.admission_root)
        live_queue_ids = {slot.queue_id for slot in list_slots(self.admission_root) if str(slot.queue_id).strip()}

        for queue_root in _queue_roots(self.cfg):
            for entry in list_queue(queue_root):
                if getattr(entry, "status", None) != QueueStatus.RUNNING:
                    continue
                if entry.queue_id in live_queue_ids:
                    continue
                if getattr(entry, "cancel_requested", False):
                    mark_cancelled(queue_root, entry.queue_id, error="cancel_requested")
                else:
                    requeue_running_entry(queue_root, entry.queue_id)

    def _fill_slots(self) -> None:
        while len(self._running) < self.max_concurrent:
            reserved = self._reserve_next_entry()
            if reserved is None:
                break
            queue_root, entry, admission_token = reserved
            self._start_job(queue_root, entry, admission_token=admission_token)

    def _reserve_next_entry(self) -> tuple[Path, Any, str] | None:
        admission_token = _try_reserve_admission_slot(self.cfg)
        if admission_token is None:
            return None

        dequeued = _dequeue_next_entry(self.cfg)
        if dequeued is None:
            release_slot(self.admission_root, admission_token)
            return None

        queue_root, entry = dequeued
        return queue_root, entry, admission_token

    def _start_job(self, queue_root: Path, entry: Any, *, admission_token: str) -> bool:
        try:
            process = subprocess.Popen(
                build_worker_child_command(
                    config_path=self.config_path,
                    queue_root=queue_root,
                    queue_id=entry.queue_id,
                    auto_organize=self.auto_organize,
                    admission_token=admission_token,
                ),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
                text=True,
            )
        except OSError as exc:
            mark_failed(queue_root, entry.queue_id, error=str(exc))
            release_slot(self.admission_root, admission_token)
            return False

        job_dir_text = str(getattr(entry, "metadata", {}).get("job_dir", "")).strip()
        attached = activate_reserved_slot(
            self.admission_root,
            admission_token,
            owner_pid=process.pid,
            source="chemstack.crest.queue_worker.child",
            queue_id=entry.queue_id,
            work_dir=job_dir_text or None,
        )
        if attached is None:
            _terminate_process(process)
            mark_failed(queue_root, entry.queue_id, error="admission_slot_missing")
            release_slot(self.admission_root, admission_token)
            return False

        self._running[entry.queue_id] = _RunningJob(
            queue_root=queue_root,
            entry=entry,
            process=process,
            admission_token=admission_token,
        )
        return True

    def _check_completed_jobs(self) -> None:
        done_ids: list[str] = []
        for queue_id, job in list(self._running.items()):
            rc = job.process.poll()
            if rc is None:
                continue
            self._finalize_child_exit(job, rc=rc)
            done_ids.append(queue_id)
        for queue_id in done_ids:
            self._running.pop(queue_id, None)

    def _finalize_child_exit(self, job: _RunningJob, *, rc: int) -> None:
        current = _find_queue_entry(job.queue_root, job.entry.queue_id)
        if current is not None and getattr(current, "status", None) == QueueStatus.RUNNING:
            if self._shutdown_requested:
                if getattr(current, "cancel_requested", False):
                    mark_cancelled(job.queue_root, current.queue_id, error="cancel_requested")
                else:
                    requeue_running_entry(job.queue_root, current.queue_id)
            elif getattr(current, "cancel_requested", False):
                mark_cancelled(job.queue_root, current.queue_id, error="cancel_requested")
            else:
                mark_failed(job.queue_root, current.queue_id, error=f"worker_child_exit_code={rc}")
        release_slot(self.admission_root, job.admission_token)

    def _shutdown_all(self) -> None:
        for queue_id, job in list(self._running.items()):
            self._shutdown_child(job)
            self._running.pop(queue_id, None)

    def _shutdown_child(self, job: _RunningJob) -> None:
        if job.process.poll() is None:
            try:
                job.process.terminate()
            except Exception:
                pass

        deadline = time.monotonic() + WORKER_SHUTDOWN_GRACE_SECONDS
        while job.process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.1)

        if job.process.poll() is None:
            _terminate_process(job.process)

        rc = job.process.poll()
        self._finalize_child_exit(job, rc=int(rc) if rc is not None else 0)

    def _install_signal_handlers(self) -> None:
        def _handle_signal(_signum: int, _frame: object) -> None:
            self._shutdown_requested = True

        try:
            signal.signal(signal.SIGTERM, _handle_signal)
            signal.signal(signal.SIGINT, _handle_signal)
        except ValueError:
            pass

    def _pid_file_path(self) -> Path:
        return self.allowed_root / WORKER_PID_FILE

    def _write_pid_file(self) -> None:
        self._pid_file_path().write_text(str(os.getpid()), encoding="utf-8")

    def _remove_pid_file(self) -> None:
        try:
            self._pid_file_path().unlink()
        except OSError:
            pass


def cmd_queue_worker(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    auto_organize = bool(cfg.behavior.auto_organize_on_terminal)
    if bool(getattr(args, "auto_organize", False)):
        auto_organize = True
    elif bool(getattr(args, "no_auto_organize", False)):
        auto_organize = False

    existing_pid = read_worker_pid(Path(str(cfg.runtime.allowed_root)).expanduser().resolve())
    if existing_pid is not None:
        print(f"error: queue worker already running (pid={existing_pid})")
        return 1

    worker = QueueWorker(
        cfg,
        getattr(args, "config", None) or default_config_path(),
        max_concurrent=max(1, int(getattr(cfg.runtime, "max_concurrent", 1))),
        auto_organize=auto_organize,
    )
    return worker.run()
