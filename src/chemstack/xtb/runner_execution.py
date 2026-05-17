from __future__ import annotations

import shutil
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml

from .commands._helpers import MANIFEST_FILE_NAME


def run_candidate_sp_job(
    cfg: Any,
    *,
    candidate_xyz: Path,
    candidate_run_dir: Path,
    manifest: dict[str, Any],
    should_cancel: Callable[[], bool] | None = None,
    on_running_job: Callable[[Any | None], None] | None = None,
    terminate_process: Callable[[subprocess.Popen[str]], None] | None = None,
    deps: Any,
) -> Any:
    candidate_run_dir.mkdir(parents=True, exist_ok=True)
    candidate_input = candidate_run_dir / "input.xyz"
    shutil.copy2(candidate_xyz, candidate_input)
    candidate_manifest = dict(manifest)
    candidate_manifest["job_type"] = "sp"
    candidate_manifest["input_xyz"] = "input.xyz"
    candidate_manifest_path = candidate_run_dir / MANIFEST_FILE_NAME
    candidate_manifest_path.write_text(
        yaml.safe_dump(candidate_manifest, sort_keys=False), encoding="utf-8"
    )
    running = deps.start_xtb_job(
        cfg,
        job_dir=candidate_run_dir,
        selected_input_xyz=candidate_input,
    )
    if on_running_job is not None:
        on_running_job(running)
    try:
        return _wait_for_candidate_sp_result(
            running,
            should_cancel=should_cancel,
            on_cancel=terminate_process,
            deps=deps,
        )
    finally:
        if on_running_job is not None:
            on_running_job(None)


def _wait_for_candidate_sp_result(
    running: Any,
    *,
    should_cancel: Callable[[], bool] | None,
    on_cancel: Callable[[subprocess.Popen[str]], None] | None,
    deps: Any,
    sleep_fn: Callable[[float], None] = time.sleep,
    poll_interval_seconds: float = 1.0,
) -> Any:
    process = getattr(running, "process", None)
    if process is None:
        return deps.finalize_xtb_job(running)
    while True:
        if should_cancel is not None and should_cancel():
            _request_candidate_process_stop(process, on_cancel=on_cancel)
            return deps.finalize_xtb_job(
                running,
                forced_status="cancelled",
                forced_reason="cancel_requested",
            )
        if process.poll() is not None:
            return deps.finalize_xtb_job(running)
        sleep_fn(poll_interval_seconds)


def _request_candidate_process_stop(
    process: subprocess.Popen[str],
    *,
    on_cancel: Callable[[subprocess.Popen[str]], None] | None,
) -> None:
    if process.poll() is not None:
        return
    if on_cancel is not None:
        on_cancel(process)
        return
    try:
        process.terminate()
    except Exception:
        pass
