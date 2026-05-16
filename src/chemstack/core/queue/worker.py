from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Generic, MutableMapping, Protocol, TypeVar

from chemstack.core.admission import reserve_slot
from chemstack.core.utils import process as process_utils
from chemstack.core.utils.persistence import now_utc_iso

T = TypeVar("T")


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


class ManagedProcess(Protocol):
    pid: int

    def kill(self) -> None: ...
    def poll(self) -> int | None: ...
    def terminate(self) -> None: ...
    def wait(self, timeout: float | None = None) -> int: ...


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


def build_background_worker_command(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    worker_job_module: str,
    admission_root: str | Path | None = None,
    admission_token: str | None = None,
    auto_organize: bool = False,
    include_admission_root: bool = True,
    auto_organize_before_admission_token: bool = False,
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
    ]
    if include_admission_root:
        if admission_root is None:
            raise ValueError("admission_root is required when include_admission_root is true")
        command.extend(["--admission-root", str(admission_root)])
    if auto_organize and auto_organize_before_admission_token:
        command.append("--auto-organize")
    if admission_token:
        command.extend(["--admission-token", admission_token])
    if auto_organize and not auto_organize_before_admission_token:
        command.append("--auto-organize")
    return command


def start_background_job_process(
    *,
    config_path: str,
    queue_root: str | Path,
    entry: Any,
    worker_job_module: str,
    admission_root: str | Path | None = None,
    admission_token: str | None = None,
    auto_organize: bool = False,
    include_admission_root: bool = True,
    auto_organize_before_admission_token: bool = False,
) -> subprocess.Popen[str]:
    return subprocess.Popen(
        build_background_worker_command(
            config_path=config_path,
            queue_root=queue_root,
            queue_id=entry.queue_id,
            worker_job_module=worker_job_module,
            admission_root=admission_root,
            admission_token=admission_token,
            auto_organize=auto_organize,
            include_admission_root=include_admission_root,
            auto_organize_before_admission_token=auto_organize_before_admission_token,
        ),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        text=True,
    )


def request_job_cancellation(
    proc: Any,
    *,
    cancel_signal: int,
    terminate_process_fn: Callable[[Any], None],
) -> None:
    try:
        send_signal = getattr(proc, "send_signal", None)
        if callable(send_signal):
            send_signal(cancel_signal)
        else:
            os.kill(proc.pid, cancel_signal)
    except (OSError, ProcessLookupError, PermissionError):
        terminate_process_fn(proc)


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

        self._running[entry.queue_id] = BackgroundRunningJob(
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


def process_one_child_queue(cfg: Any, *, auto_organize: bool, deps: Any) -> str:
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


def terminate_process_group(
    proc: ManagedProcess,
    *,
    graceful_timeout: float = 10,
    kill_timeout: float = 5,
    killpg_fn: Callable[[int, int], None] | None = None,
    sigterm: int | None = None,
    sigkill: int | None = None,
) -> None:
    if proc.poll() is not None:
        return
    active_killpg = killpg_fn or os.killpg
    active_sigterm = signal.SIGTERM if sigterm is None else sigterm
    active_sigkill = signal.SIGKILL if sigkill is None else sigkill

    try:
        active_killpg(proc.pid, active_sigterm)
    except (ProcessLookupError, PermissionError):
        try:
            proc.terminate()
        except Exception:
            pass

    try:
        proc.wait(timeout=graceful_timeout)
    except subprocess.TimeoutExpired:
        try:
            active_killpg(proc.pid, active_sigkill)
        except (ProcessLookupError, PermissionError):
            try:
                proc.kill()
            except Exception:
                pass
        try:
            proc.wait(timeout=kill_timeout)
        except subprocess.TimeoutExpired:
            pass


def install_shutdown_signal_handlers(request_shutdown: Callable[[], None]) -> None:
    def _handle_signal(_signum: int, _frame: object) -> None:
        request_shutdown()

    try:
        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)
    except ValueError:
        pass


def pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _process_start_ticks(pid: int) -> int | None:
    return process_utils.process_start_ticks(pid, proc_root=Path("/proc"))


def current_worker_pid_payload() -> dict[str, int | str]:
    return process_utils.current_pid_payload(
        now_fn=now_utc_iso,
        process_start_ticks_fn=_process_start_ticks,
        pid_fn=os.getpid,
    )


def worker_pid_file_path(allowed_root: Path | str, file_name: str = "queue_worker.pid") -> Path:
    return Path(allowed_root).expanduser().resolve() / file_name


def write_worker_pid_file(allowed_root: Path | str, file_name: str = "queue_worker.pid") -> None:
    payload = current_worker_pid_payload()
    worker_pid_file_path(allowed_root, file_name).write_text(
        json.dumps(payload, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )


def remove_worker_pid_file(allowed_root: Path | str, file_name: str = "queue_worker.pid") -> None:
    _remove_pid_file(worker_pid_file_path(allowed_root, file_name))


def read_worker_pid_file(allowed_root: Path | str, file_name: str = "queue_worker.pid") -> int | None:
    return read_live_pid_file(worker_pid_file_path(allowed_root, file_name))


def _positive_int(value: Any) -> int | None:
    return process_utils.positive_int(value)


def _read_pid_payload(pid_path: Path) -> tuple[int | None, int | None]:
    return process_utils.read_pid_payload(pid_path)


def _remove_pid_file(pid_path: Path) -> None:
    process_utils.remove_file_silent(pid_path)


def read_live_pid_file(pid_path: Path) -> int | None:
    return process_utils.read_live_pid_file(
        pid_path,
        is_process_alive_fn=pid_is_alive,
        process_start_ticks_fn=_process_start_ticks,
        remove_file_fn=_remove_pid_file,
    )
