from __future__ import annotations

import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO


@dataclass(frozen=True)
class LoggedEngineProcess:
    process: subprocess.Popen[str]
    started_at: str
    stdout_log: Path
    stderr_log: Path
    stdout_handle: TextIO
    stderr_handle: TextIO


def thread_limited_env(base_env: Mapping[str, str], max_cores: int) -> dict[str, str]:
    thread_count = str(max(1, int(max_cores)))
    return {
        **base_env,
        "OMP_NUM_THREADS": thread_count,
        "OPENBLAS_NUM_THREADS": thread_count,
        "MKL_NUM_THREADS": thread_count,
        "NUMEXPR_NUM_THREADS": thread_count,
    }


def start_logged_process(
    command: Sequence[str],
    *,
    cwd: Path,
    stdout_log: Path,
    stderr_log: Path,
    max_cores: int,
    base_env: Mapping[str, str],
    now_utc_iso_fn: Callable[[], str],
    popen_fn: Callable[..., subprocess.Popen[str]],
    stdin_value: object,
    preexec_fn: Callable[[], None] | None,
) -> LoggedEngineProcess:
    started_at = now_utc_iso_fn()
    env = thread_limited_env(base_env, max_cores)
    stdout_handle = stdout_log.open("w", encoding="utf-8")
    stderr_handle = stderr_log.open("w", encoding="utf-8")
    try:
        process = popen_fn(
            list(command),
            cwd=cwd,
            env=env,
            text=True,
            stdout=stdout_handle,
            stderr=stderr_handle,
            stdin=stdin_value,
            start_new_session=True,
            preexec_fn=preexec_fn,
        )
    except Exception:
        stdout_handle.close()
        stderr_handle.close()
        raise
    return LoggedEngineProcess(
        process=process,
        started_at=started_at,
        stdout_log=stdout_log,
        stderr_log=stderr_log,
        stdout_handle=stdout_handle,
        stderr_handle=stderr_handle,
    )


__all__ = [
    "LoggedEngineProcess",
    "start_logged_process",
    "thread_limited_env",
]
