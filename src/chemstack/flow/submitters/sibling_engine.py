from __future__ import annotations

from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import subprocess
from collections.abc import Callable
from typing import Any

from .common import normalize_text, parse_key_value_lines, queue_submission_status


def command_argv(args: object) -> list[str]:
    return list(args) if isinstance(args, (list, tuple)) else [str(args)]


def api_command_argv(
    *,
    api_name: str,
    config_path: str,
    tail_argv: list[str],
) -> list[str]:
    return ["python-api", api_name, "--config", config_path, *tail_argv]


def _stderr_with_exception(stderr: str, exc: Exception) -> str:
    pieces = [stderr] if stderr else []
    if pieces and not pieces[-1].endswith("\n"):
        pieces[-1] += "\n"
    pieces.append(f"{exc.__class__.__name__}: {exc}\n")
    return "".join(pieces)


def run_python_command_handler(
    *,
    handler: Callable[[Any], int],
    args: Any,
    command_argv: list[str],
) -> subprocess.CompletedProcess[str]:
    stdout_buffer = StringIO()
    stderr_buffer = StringIO()
    try:
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            returncode = int(handler(args))
    except Exception as exc:
        return subprocess.CompletedProcess(
            args=command_argv,
            returncode=1,
            stdout=stdout_buffer.getvalue(),
            stderr=_stderr_with_exception(stderr_buffer.getvalue(), exc),
        )
    return subprocess.CompletedProcess(
        args=command_argv,
        returncode=returncode,
        stdout=stdout_buffer.getvalue(),
        stderr=stderr_buffer.getvalue(),
    )


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


def submit_job_dir_direct(
    *,
    run_dir_handler: Callable[[Any], int],
    api_name: str,
    job_dir: str,
    priority: int,
    config_path: str,
    extra_fields: Callable[[dict[str, str]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    tail_argv = ["run-dir", job_dir, "--priority", str(int(priority))]
    result = run_python_command_handler(
        handler=run_dir_handler,
        args=Namespace(config=config_path, path=job_dir, priority=int(priority)),
        command_argv=api_command_argv(
            api_name=api_name,
            config_path=config_path,
            tail_argv=tail_argv,
        ),
    )
    parsed = parse_key_value_lines(result.stdout)
    status, reason = queue_submission_status(
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


def cancel_target_direct(
    *,
    cancel_handler: Callable[[Any], int],
    api_name: str,
    target: str,
    config_path: str,
) -> dict[str, Any]:
    tail_argv = ["queue", "cancel", target]
    result = run_python_command_handler(
        handler=cancel_handler,
        args=Namespace(config=config_path, target=target),
        command_argv=api_command_argv(
            api_name=api_name,
            config_path=config_path,
            tail_argv=tail_argv,
        ),
    )
    parsed = parse_key_value_lines(result.stdout)
    status = queue_cancel_status(
        returncode=int(result.returncode),
        parsed_stdout=parsed,
        stdout=result.stdout,
        normalize_text=normalize_text,
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
    normalize_text_fn: Callable[[Any], str],
    run_sibling_app: Callable[..., subprocess.CompletedProcess[str]],
    target: str,
    config_path: str,
    repo_root: str | None,
    module_name: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    try:
        result = run_sibling_app(
            config_path=normalize_text_fn(config_path),
            repo_root=normalize_text_fn(repo_root) or None,
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
