from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Generic, MutableMapping, TypeVar

from chemstack.core.admission import reserve_slot

from .child_execution import find_queue_entry_by_id
from .dependencies import ChildQueueWorkerDeps
from .child_process import (
    build_background_worker_command,
    live_queue_ids_for_slots,
    reconcile_orphaned_child_queue_entries,
    request_job_cancellation,
    shutdown_child_process_with_grace,
    start_background_process,
    start_background_job_process,
    status_matches,
)
from .processes import (
    ManagedProcess,
    ShutdownSignalDeps,
    current_worker_pid_payload,
    install_shutdown_signal_handlers as _install_shutdown_signal_handlers,
    pid_is_alive,
    read_worker_pid_file,
    remove_worker_pid_file,
    terminate_process_group,
    worker_pid_file_path,
    write_worker_pid_file,
)

T = TypeVar("T")
LOGGER = logging.getLogger(__name__)

__all__ = [
    "BackgroundRunningJob",
    "ChildProcessQueueWorker",
    "EngineRunningJob",
    "ManagedProcess",
    "PidFileChildProcessQueueWorker",
    "QueueWorkerLoop",
    "QueueWorkerPidFileMixin",
    "ReservedQueueEntry",
    "SlotFillResult",
    "ShutdownSignalDeps",
    "build_background_worker_command",
    "config_path_for_worker",
    "dequeue_next_across_roots",
    "fill_worker_slots",
    "install_shutdown_signal_handlers",
    "live_queue_ids_for_slots",
    "pid_is_alive",
    "pop_completed_worker_jobs",
    "queue_entry_by_id",
    "read_worker_pid_file",
    "reconcile_orphaned_child_queue_entries",
    "remove_worker_pid_file",
    "make_child_queue_worker_deps",
    "request_job_cancellation",
    "reserve_dequeued_entry",
    "reserve_engine_queue_worker_slot",
    "reserve_queue_worker_slot",
    "resolve_admission_limit",
    "resolve_admission_root",
    "resolve_worker_auto_organize",
    "shutdown_child_process_with_grace",
    "start_background_process",
    "start_background_job_process",
    "status_matches",
    "terminate_process_group",
    "worker_pid_file_path",
    "write_worker_pid_file",
    "current_worker_pid_payload",
]


@dataclass(frozen=True)
class SlotFillResult:
    status: str
    started: int


@dataclass(frozen=True)
class ReservedQueueEntry(Generic[T]):
    queue_root: Path
    entry: T
    admission_token: str


@dataclass
class BackgroundRunningJob:
    queue_root: Path
    entry: Any
    process: Any
    admission_token: str
    cancel_requested: bool = False
    started_at: float = field(default_factory=time.monotonic)


@dataclass
class EngineRunningJob:
    queue_id: str
    reaction_dir: str
    process: Any
    admission_token: str
    task_id: str | None = None
    started_at: float = field(default_factory=time.monotonic)


class QueueWorkerPidFileMixin:
    worker_pid_file_name = "queue_worker.pid"
    allowed_root: Path

    def _pid_file_path(self) -> Path:
        return worker_pid_file_path(self.allowed_root, self.worker_pid_file_name)

    def _write_pid_file(self) -> None:
        write_worker_pid_file(self.allowed_root, self.worker_pid_file_name)

    def _remove_pid_file(self) -> None:
        remove_worker_pid_file(self.allowed_root, self.worker_pid_file_name)


def resolve_admission_root(cfg: Any) -> str:
    return str(
        getattr(cfg.runtime, "resolved_admission_root", None)
        or getattr(cfg.runtime, "admission_root", "")
        or cfg.runtime.allowed_root
    )


def resolve_admission_limit(cfg: Any) -> int:
    raw = getattr(cfg.runtime, "resolved_admission_limit", None)
    if raw in (None, "", 0):
        raw = getattr(cfg.runtime, "admission_limit", None)
    if raw in (None, "", 0):
        raw = getattr(cfg.runtime, "max_concurrent", 1)
    try:
        return max(1, int(raw if raw is not None else 1))
    except (TypeError, ValueError):
        return 1


def reserve_queue_worker_slot(
    cfg: Any,
    *,
    source: str,
    app_name: str,
    reserve_slot_fn: Callable[..., str | None] = reserve_slot,
) -> str | None:
    return reserve_slot_fn(
        resolve_admission_root(cfg),
        resolve_admission_limit(cfg),
        source=source,
        app_name=app_name,
    )


def reserve_engine_queue_worker_slot(
    cfg: Any,
    *,
    engine: str,
    reserve_slot_fn: Callable[..., str | None] = reserve_slot,
) -> str | None:
    engine_slug = str(engine).strip().replace("-", "_")
    return reserve_queue_worker_slot(
        cfg,
        source=f"chemstack.{engine_slug}.queue_worker",
        app_name=f"chemstack_{engine_slug}",
        reserve_slot_fn=reserve_slot_fn,
    )


def dequeue_next_across_roots(
    roots: tuple[Path, ...],
    *,
    list_queue_fn: Callable[[Path], list[T]],
    dequeue_next_fn: Callable[[Path], T | None],
) -> tuple[Path, T] | None:
    if len(roots) == 1:
        entry = dequeue_next_fn(roots[0])
        if entry is None:
            return None
        return roots[0], entry

    selected_root: Path | None = None
    selected_key: tuple[int, str, int, str] | None = None

    for root_index, root in enumerate(roots):
        for entry in list_queue_fn(root):
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

    entry = dequeue_next_fn(selected_root)
    if entry is None:
        return None
    return selected_root, entry


def queue_entry_by_id(
    queue_root: str | Path,
    queue_id: str,
    *,
    list_queue_fn: Callable[[str | Path], Any],
) -> Any | None:
    return find_queue_entry_by_id(
        queue_root,
        queue_id,
        list_queue_fn=list_queue_fn,
    )


def reserve_dequeued_entry(
    cfg: Any,
    *,
    admission_root: str | Path,
    reserve_slot_fn: Callable[[Any], str | None],
    dequeue_next_fn: Callable[[Any], tuple[Path, T] | None],
    release_slot_fn: Callable[[str | Path, str], object],
) -> tuple[str, ReservedQueueEntry[T] | None]:
    admission_token = reserve_slot_fn(cfg)
    if admission_token is None:
        return "blocked", None

    dequeued = dequeue_next_fn(cfg)
    if dequeued is None:
        release_slot_fn(admission_root, admission_token)
        return "idle", None

    queue_root, entry = dequeued
    return (
        "processed",
        ReservedQueueEntry(
            queue_root=queue_root,
            entry=entry,
            admission_token=admission_token,
        ),
    )


def make_child_queue_worker_deps(
    *,
    poll_interval_seconds: int,
    time_module: Any,
    release_slot_fn: Callable[[str | Path, str], object],
    admission_root_fn: Callable[[Any], str],
    dequeue_next_entry_fn: Callable[[Any], tuple[Path, Any] | None],
    start_background_job_process_fn: Callable[..., Any],
    try_reserve_admission_slot_fn: Callable[[Any], str | None],
    reserve_dequeued_entry_fn: Callable[..., tuple[str, Any | None]] = reserve_dequeued_entry,
) -> ChildQueueWorkerDeps:
    return ChildQueueWorkerDeps(
        poll_interval_seconds=poll_interval_seconds,
        time=time_module,
        release_slot=release_slot_fn,
        reserve_dequeued_entry=reserve_dequeued_entry_fn,
        admission_root=admission_root_fn,
        dequeue_next_entry=dequeue_next_entry_fn,
        start_background_job_process=start_background_job_process_fn,
        try_reserve_admission_slot=try_reserve_admission_slot_fn,
    )


def fill_worker_slots(
    *,
    running_count: Callable[[], int],
    max_concurrent: int,
    reserve_next: Callable[[], tuple[str, T | None]],
    start_reserved: Callable[[T], None],
    max_new_jobs: int | None = None,
) -> SlotFillResult:
    started = 0
    while running_count() < max_concurrent:
        if max_new_jobs is not None and started >= max_new_jobs:
            break
        status, reserved = reserve_next()
        if status != "processed" or reserved is None:
            return SlotFillResult(status="processed" if started else status, started=started)
        start_reserved(reserved)
        started += 1
    return SlotFillResult(status="processed" if started else "idle", started=started)


def pop_completed_worker_jobs(
    running: MutableMapping[str, T],
    *,
    poll_job: Callable[[T], int | None],
    finalize_finished: Callable[[str, T, int], None],
) -> int:
    completed: list[tuple[str, T, int]] = []
    for queue_id, job in list(running.items()):
        rc = poll_job(job)
        if rc is None:
            continue
        completed.append((queue_id, job, rc))

    for queue_id, job, rc in completed:
        finalize_finished(queue_id, job, rc)
        running.pop(queue_id, None)
    return len(completed)


def resolve_worker_auto_organize(cfg: Any, args: Any) -> bool:
    auto_organize = bool(cfg.behavior.auto_organize_on_terminal)
    if bool(getattr(args, "auto_organize", False)):
        auto_organize = True
    elif bool(getattr(args, "no_auto_organize", False)):
        auto_organize = False
    return auto_organize


def config_path_for_worker(args: Any, *, default_config_path_fn: Callable[[], str]) -> str:
    configured = str(getattr(args, "config", "") or "").strip()
    return configured or default_config_path_fn()


class QueueWorkerLoop:
    def __init__(
        self,
        *,
        max_concurrent: int,
        poll_interval_seconds: float,
        sleep_fn: Callable[[float], None] | None = None,
    ) -> None:
        self.max_concurrent = max(1, int(max_concurrent))
        self.poll_interval_seconds = float(poll_interval_seconds)
        self._sleep_fn = sleep_fn or time.sleep
        self._running: dict[str, Any] = {}
        self._shutdown_requested = False

    def run(self) -> int:
        self._install_signal_handlers()
        self._before_run()
        try:
            while not self._shutdown_requested:
                self._run_iteration()
        except KeyboardInterrupt:
            self._shutdown_requested = True
        finally:
            self._shutdown_all()
            self._after_run()
        return 0

    def run_once(
        self,
        *,
        idle_message: str | None = None,
        blocked_message: str | None = None,
    ) -> int:
        self._install_signal_handlers()
        self._before_run()
        try:
            outcome = self._fill_slots(max_new_jobs=1)
            if outcome == "idle":
                if idle_message:
                    print(idle_message)
                return 0
            if outcome == "blocked":
                if blocked_message:
                    print(blocked_message)
                return 0

            while self._running and not self._shutdown_requested:
                self._check_completed_jobs()
                self._check_cancel_requests()
                if self._running:
                    self._sleep()
        except KeyboardInterrupt:
            self._shutdown_requested = True
        finally:
            self._shutdown_all()
            self._after_run()
        return 0

    def _before_run(self) -> None:
        return None

    def _after_run(self) -> None:
        return None

    def _run_iteration(self) -> None:
        self._check_completed_jobs()
        if self._shutdown_requested:
            return
        self._check_cancel_requests()
        if self._shutdown_requested:
            return
        self._fill_slots()
        if self._shutdown_requested:
            return
        self._sleep()

    def _sleep(self) -> None:
        self._sleep_fn(self.poll_interval_seconds)

    def _fill_slots(self, *, max_new_jobs: int | None = None) -> str:
        result = fill_worker_slots(
            running_count=lambda: len(self._running),
            max_concurrent=self.max_concurrent,
            reserve_next=self._reserve_next_entry,
            start_reserved=self._start_reserved,
            max_new_jobs=max_new_jobs,
        )
        return result.status

    def _check_completed_jobs(self) -> None:
        pop_completed_worker_jobs(
            self._running,
            poll_job=self._poll_job,
            finalize_finished=self._finalize_completed_job,
        )

    def _running_jobs(self) -> list[tuple[str, Any]]:
        return list(self._running.items())

    def _discard_running_job(self, queue_id: str) -> None:
        self._running.pop(queue_id, None)

    def _check_cancel_requests(self) -> None:
        return None

    def _install_signal_handlers(self) -> None:
        def request_shutdown() -> None:
            self._shutdown_requested = True

        install_shutdown_signal_handlers(request_shutdown)

    def _reserve_next_entry(self) -> tuple[str, Any | None]:
        raise NotImplementedError

    def _start_reserved(self, reserved: Any) -> None:
        raise NotImplementedError

    def _poll_job(self, job: Any) -> int | None:
        raise NotImplementedError

    def _finalize_completed_job(self, queue_id: str, job: Any, rc: int) -> None:
        raise NotImplementedError

    def _shutdown_all(self) -> None:
        raise NotImplementedError


class ChildProcessQueueWorker(QueueWorkerLoop):
    def __init__(
        self,
        cfg: Any,
        *,
        config_path: str,
        max_concurrent: int | None = None,
        deps: Any,
    ) -> None:
        configured_max = cfg.runtime.max_concurrent if max_concurrent is None else max_concurrent
        super().__init__(
            max_concurrent=max(1, int(configured_max)),
            poll_interval_seconds=deps.poll_interval_seconds,
            sleep_fn=lambda seconds: deps.time.sleep(seconds),
        )
        self.cfg = cfg
        self.config_path = config_path
        self.admission_root = deps.admission_root(cfg)
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
            reserve_slot_fn=deps.try_reserve_admission_slot,
            dequeue_next_fn=deps.dequeue_next_entry,
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
            proc = deps.start_background_job_process(
                config_path=self.config_path,
                queue_root=queue_root,
                entry=entry,
                admission_root=self.admission_root,
                admission_token=admission_token,
            )
        except OSError as exc:
            self._handle_worker_start_error(queue_root, entry, admission_token, exc)
            return

        if not self._on_worker_process_started(
            queue_root,
            entry,
            process=proc,
            admission_token=admission_token,
        ):
            return

        self._running[self._running_queue_id(entry)] = self._make_running_job(
            queue_root=queue_root,
            entry=entry,
            process=proc,
            admission_token=admission_token,
        )

    def _handle_worker_start_error(
        self,
        queue_root: Path,
        entry: Any,
        admission_token: str,
        exc: OSError,
    ) -> None:
        raise NotImplementedError

    def _on_worker_process_started(
        self,
        queue_root: Path,
        entry: Any,
        *,
        process: Any,
        admission_token: str,
    ) -> bool:
        del queue_root, entry, process, admission_token
        return True

    def _running_queue_id(self, entry: Any) -> str:
        return str(entry.queue_id)

    def _make_running_job(
        self,
        *,
        queue_root: Path,
        entry: Any,
        process: Any,
        admission_token: str,
    ) -> Any:
        return BackgroundRunningJob(
            queue_root=queue_root,
            entry=entry,
            process=process,
            admission_token=admission_token,
        )

    def _poll_job(self, job: Any) -> int | None:
        return job.process.poll()

    def _release_admission_slot(self, admission_token: str) -> object:
        return self.deps.release_slot(self.admission_root, admission_token)

    def _mark_entry_failed_and_release(
        self,
        queue_root: Path,
        entry: Any,
        admission_token: str,
        *,
        error: str,
        mark_failed_fn: Callable[..., Any],
    ) -> None:
        mark_failed_fn(queue_root, self._running_queue_id(entry), error=error)
        self._release_admission_slot(admission_token)

    def _finalize_completed_job(self, _queue_id: str, job: Any, rc: int) -> None:
        raise NotImplementedError

    def _shutdown_all(self) -> None:
        if not self._running:
            return
        self._before_shutdown_all(len(self._running))
        for queue_id, job in self._running_jobs():
            self._shutdown_running_job(queue_id, job)
            self._discard_running_job(queue_id)

    def _before_shutdown_all(self, running_count: int) -> None:
        del running_count

    def _shutdown_running_job(self, queue_id: str, job: Any) -> None:
        raise NotImplementedError

    def _reconcile_worker_state(self) -> None:
        raise NotImplementedError


class PidFileChildProcessQueueWorker(QueueWorkerPidFileMixin, ChildProcessQueueWorker):
    """Child-process queue worker with standard ChemStack pid-file lifecycle."""

    def __init__(
        self,
        cfg: Any,
        *,
        config_path: str,
        max_concurrent: int | None = None,
        deps: Any,
        allowed_root: str | Path | None = None,
        admission_root: str | Path | None = None,
    ) -> None:
        super().__init__(
            cfg,
            config_path=config_path,
            max_concurrent=max_concurrent,
            deps=deps,
        )
        raw_allowed_root = allowed_root if allowed_root is not None else cfg.runtime.allowed_root
        self.allowed_root = Path(str(raw_allowed_root)).expanduser().resolve()
        if admission_root is not None:
            self.admission_root = Path(str(admission_root)).expanduser().resolve()

    def _before_run(self) -> None:
        self._write_pid_file()
        super()._before_run()

    def _after_run(self) -> None:
        self._remove_pid_file()


def install_shutdown_signal_handlers(request_shutdown: Callable[[], None]) -> None:
    _install_shutdown_signal_handlers(
        request_shutdown,
        deps=ShutdownSignalDeps(
            signal_fn=signal.signal,
            sigterm=signal.SIGTERM,
            sigint=signal.SIGINT,
            logger=LOGGER,
        ),
    )
