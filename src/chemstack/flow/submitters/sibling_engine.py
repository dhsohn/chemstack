from __future__ import annotations

import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SubmitterDeps:
    normalize_text: Callable[[Any], str]
    parse_key_value_lines: Callable[[str], dict[str, str]]
    queue_submission_status: Callable[..., tuple[str, str]]
    run_sibling_app: Callable[..., subprocess.CompletedProcess[str]]


def submitter_deps(namespace: Any) -> SubmitterDeps:
    return SubmitterDeps(
        normalize_text=namespace.normalize_text,
        parse_key_value_lines=namespace.parse_key_value_lines,
        queue_submission_status=namespace.queue_submission_status,
        run_sibling_app=namespace.run_sibling_app,
    )


def command_argv(args: object) -> list[str]:
    return list(args) if isinstance(args, (list, tuple)) else [str(args)]


def timeout_result(exc: subprocess.TimeoutExpired) -> dict[str, Any]:
    return {
        "status": "failed",
        "reason": "cancel_command_timeout",
        "returncode": 124,
        "command_argv": command_argv(exc.cmd),
        "stdout": exc.stdout or "",
        "stderr": exc.stderr or "",
    }


def queue_cancel_status(
    *,
    returncode: int,
    parsed_stdout: dict[str, str],
    stdout: str,
    normalize_text: Callable[[Any], str],
) -> str:
    status = "failed"
    if int(returncode) == 0:
        parsed_status = normalize_text(parsed_stdout.get("status")).lower()
        if parsed_status == "cancel_requested":
            status = "cancel_requested"
        elif parsed_status == "cancelled":
            status = "cancelled"
        elif "cancel requested" in stdout.lower():
            status = "cancel_requested"
        else:
            status = "cancelled"
    return status


def cli_cancel_status(*, returncode: int, stdout: str) -> str:
    if int(returncode) != 0:
        return "failed"
    text = stdout.strip()
    if text.startswith("Cancelled:"):
        return "cancelled"
    if "Cancel requested" in text:
        return "cancel_requested"
    return "cancelled"


def submit_job_dir(
    *,
    deps: Any,
    job_dir: str,
    priority: int,
    config_path: str,
    executable: str,
    repo_root: str | None,
    module_name: str,
    extra_fields: Callable[[dict[str, str]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    result = deps.run_sibling_app(
        executable=deps.normalize_text(executable),
        config_path=deps.normalize_text(config_path),
        repo_root=deps.normalize_text(repo_root) or None,
        module_name=module_name,
        tail_argv=[
            "run-dir",
            job_dir,
            "--priority",
            str(int(priority)),
        ],
    )
    parsed = deps.parse_key_value_lines(result.stdout)
    status, reason = deps.queue_submission_status(
        returncode=int(result.returncode),
        parsed_stdout=parsed,
        stdout=result.stdout,
        stderr=result.stderr,
    )
    payload = {
        "status": status,
        "reason": reason,
        "returncode": int(result.returncode),
        "command_argv": command_argv(result.args),
        "stdout": result.stdout,
        "stderr": result.stderr,
        "parsed_stdout": parsed,
        "job_id": parsed.get("job_id", ""),
        "queue_id": parsed.get("queue_id", ""),
        "job_dir": parsed.get("job_dir", job_dir),
    }
    if extra_fields is not None:
        payload.update(extra_fields(parsed))
    return payload


def cancel_target(
    *,
    deps: Any,
    target: str,
    config_path: str,
    executable: str,
    repo_root: str | None,
    module_name: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    try:
        result = deps.run_sibling_app(
            executable=deps.normalize_text(executable),
            config_path=deps.normalize_text(config_path),
            repo_root=deps.normalize_text(repo_root) or None,
            module_name=module_name,
            tail_argv=["queue", "cancel", target],
            timeout_seconds=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            **timeout_result(exc),
            "parsed_stdout": {},
            "queue_id": "",
            "job_id": "",
        }
    parsed = deps.parse_key_value_lines(result.stdout)
    status = queue_cancel_status(
        returncode=int(result.returncode),
        parsed_stdout=parsed,
        stdout=result.stdout,
        normalize_text=deps.normalize_text,
    )
    return {
        "status": status,
        "reason": "" if status != "failed" else "cancel_command_failed",
        "returncode": int(result.returncode),
        "command_argv": command_argv(result.args),
        "stdout": result.stdout,
        "stderr": result.stderr,
        "parsed_stdout": parsed,
        "queue_id": parsed.get("queue_id", ""),
        "job_id": parsed.get("job_id", ""),
    }


def orca_cancel_target(
    *,
    deps: Any,
    target: str,
    config_path: str,
    executable: str,
    repo_root: str | None,
    module_name: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    try:
        result = deps.run_sibling_app(
            executable=deps._normalize_text(executable),
            config_path=deps._normalize_text(config_path),
            repo_root=deps._normalize_text(repo_root) or None,
            module_name=module_name,
            tail_argv=["queue", "cancel", target],
            timeout_seconds=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        return timeout_result(exc)
    return {
        "status": cli_cancel_status(returncode=int(result.returncode), stdout=result.stdout),
        "returncode": int(result.returncode),
        "command_argv": command_argv(result.args),
        "stdout": result.stdout,
        "stderr": result.stderr,
    }
