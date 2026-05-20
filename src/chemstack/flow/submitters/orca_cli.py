from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from chemstack.core.app_ids import (
    CHEMSTACK_CLI_MODULE,
    CHEMSTACK_EXECUTABLE,
    CHEMSTACK_ORCA_INTERNAL_MODULE,
)

from . import sibling_engine

_SUBMIT_MODULE_NAME = CHEMSTACK_CLI_MODULE
_CANCEL_MODULE_NAME = CHEMSTACK_ORCA_INTERNAL_MODULE
_CANCEL_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True)
class OrcaCliDeps:
    _normalize_text: Callable[[Any], str]
    run_sibling_app: Callable[..., Any]
    parse_key_value_lines: Callable[[str], dict[str, str]]
    queue_submission_status: Callable[..., tuple[str, str]]


def submission_tail_argv(
    *,
    reaction_dir: str,
    priority: int,
    max_cores: int | None = None,
    max_memory_gb: int | None = None,
    force: bool = False,
) -> list[str]:
    argv = [
        "run-dir",
        reaction_dir,
        "--priority",
        str(int(priority)),
    ]
    if force:
        argv.append("--force")
    if max_cores is not None and int(max_cores) > 0:
        argv.extend(["--max-cores", str(int(max_cores))])
    if max_memory_gb is not None and int(max_memory_gb) > 0:
        argv.extend(["--max-memory-gb", str(int(max_memory_gb))])
    return argv


def submit_reaction_dir(
    *,
    deps: OrcaCliDeps,
    reaction_dir: str,
    priority: int,
    config_path: str,
    max_cores: int | None = None,
    max_memory_gb: int | None = None,
    force: bool = False,
    executable: str = CHEMSTACK_EXECUTABLE,
    repo_root: str | None = None,
) -> dict[str, Any]:
    result = deps.run_sibling_app(
        executable=deps._normalize_text(executable) or CHEMSTACK_EXECUTABLE,
        config_path=deps._normalize_text(config_path),
        repo_root=deps._normalize_text(repo_root) or None,
        module_name=_SUBMIT_MODULE_NAME,
        tail_argv=submission_tail_argv(
            reaction_dir=reaction_dir,
            priority=priority,
            max_cores=max_cores,
            max_memory_gb=max_memory_gb,
            force=force,
        ),
    )
    parsed = deps.parse_key_value_lines(result.stdout)
    status, reason = deps.queue_submission_status(
        returncode=int(result.returncode),
        parsed_stdout=parsed,
        stdout=result.stdout,
        stderr=result.stderr,
    )
    argv = list(result.args) if isinstance(result.args, (list, tuple)) else [str(result.args)]
    return {
        "status": status,
        "reason": reason,
        "returncode": int(result.returncode),
        "command_argv": argv,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "parsed_stdout": parsed,
        "queue_id": parsed.get("queue_id", ""),
        "reaction_dir": parsed.get("job_dir") or parsed.get("reaction_dir", reaction_dir),
        "priority": int(priority),
        "force": bool(force),
    }


def cancel_target(
    *,
    deps: OrcaCliDeps,
    target: str,
    config_path: str,
    executable: str = CHEMSTACK_EXECUTABLE,
    repo_root: str | None = None,
) -> dict[str, Any]:
    return sibling_engine.orca_cancel_target(
        deps=deps,
        executable=deps._normalize_text(executable) or CHEMSTACK_EXECUTABLE,
        config_path=config_path,
        repo_root=repo_root,
        module_name=_CANCEL_MODULE_NAME,
        target=target,
        timeout_seconds=_CANCEL_TIMEOUT_SECONDS,
    )
