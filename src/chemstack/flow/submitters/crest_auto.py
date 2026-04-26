from __future__ import annotations

import subprocess
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_CREST_MODULE

from .common import normalize_text, parse_key_value_lines, run_sibling_app

_MODULE_NAME = CHEMSTACK_CREST_MODULE
_CANCEL_TIMEOUT_SECONDS = 5.0


def submit_job_dir(
    *,
    job_dir: str,
    priority: int,
    config_path: str,
    executable: str = "crest_auto",
    repo_root: str | None = None,
) -> dict[str, Any]:
    result = run_sibling_app(
        executable=normalize_text(executable) or "crest_auto",
        config_path=normalize_text(config_path),
        repo_root=normalize_text(repo_root) or None,
        module_name=_MODULE_NAME,
        tail_argv=[
            "run-dir",
            job_dir,
            "--priority",
            str(int(priority)),
        ],
    )
    parsed = parse_key_value_lines(result.stdout)
    status = "submitted" if result.returncode == 0 and parsed.get("status") == "queued" else "failed"
    argv = list(result.args) if isinstance(result.args, (list, tuple)) else [str(result.args)]
    return {
        "status": status,
        "returncode": int(result.returncode),
        "command_argv": argv,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "parsed_stdout": parsed,
        "job_id": parsed.get("job_id", ""),
        "queue_id": parsed.get("queue_id", ""),
        "job_dir": parsed.get("job_dir", job_dir),
    }


def cancel_target(
    *,
    target: str,
    config_path: str,
    executable: str = "crest_auto",
    repo_root: str | None = None,
) -> dict[str, Any]:
    try:
        result = run_sibling_app(
            executable=normalize_text(executable) or "crest_auto",
            config_path=normalize_text(config_path),
            repo_root=normalize_text(repo_root) or None,
            module_name=_MODULE_NAME,
            tail_argv=["queue", "cancel", target],
            timeout_seconds=_CANCEL_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        command_argv = list(exc.cmd) if isinstance(exc.cmd, (list, tuple)) else [str(exc.cmd)]
        return {
            "status": "failed",
            "reason": "cancel_command_timeout",
            "returncode": 124,
            "command_argv": command_argv,
            "stdout": exc.stdout or "",
            "stderr": exc.stderr or "",
            "parsed_stdout": {},
            "queue_id": "",
            "job_id": "",
        }
    parsed = parse_key_value_lines(result.stdout)
    status = "failed"
    if result.returncode == 0:
        parsed_status = normalize_text(parsed.get("status")).lower()
        if parsed_status == "cancel_requested":
            status = "cancel_requested"
        elif parsed_status == "cancelled":
            status = "cancelled"
        elif "cancel requested" in result.stdout.lower():
            status = "cancel_requested"
        else:
            status = "cancelled"
    argv = list(result.args) if isinstance(result.args, (list, tuple)) else [str(result.args)]
    return {
        "status": status,
        "reason": "" if status != "failed" else "cancel_command_failed",
        "returncode": int(result.returncode),
        "command_argv": argv,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "parsed_stdout": parsed,
        "queue_id": parsed.get("queue_id", ""),
        "job_id": parsed.get("job_id", ""),
    }


__all__ = [
    "cancel_target",
    "submit_job_dir",
]
