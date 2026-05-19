from __future__ import annotations

import os
import resource
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, TextIO

from chemstack.core.config import engines as _config_engines
from chemstack.core.engine_process import start_logged_process
from chemstack.core.utils import now_utc_iso
from chemstack.core.utils import process as process_utils

from .commands._helpers import (
    MANIFEST_FILE_NAME,
    load_job_manifest,
    resolve_job_inputs,
    resource_request_from_manifest,
)
from .config import AppConfig
from . import runner_execution as _runner_execution
from . import runner_finalize as _runner_finalize
from . import runner_ranking as _runner_ranking
from .runner_artifacts import (
    _collect_opt_candidates,
    _collect_path_search_candidates,
    _collect_sp_candidates,
    _extract_sp_energy,
)

_RankingRunContext = _runner_ranking.RankingRunContext
_RankingCollectedResults = _runner_ranking.RankingCollectedResults
_ranking_top_n = _runner_ranking.ranking_top_n
_safe_rank_name = _runner_ranking.safe_rank_name
_ranking_candidate_run_dir = _runner_ranking.ranking_candidate_run_dir
_ranking_candidate_paths = _runner_ranking.ranking_candidate_paths
_ranking_candidate_result = _runner_ranking.ranking_candidate_result
_ranking_unsuccessful_detail = _runner_ranking.ranking_unsuccessful_detail
_ranking_failure_analysis = _runner_ranking.ranking_failure_analysis
_rank_usable_candidates = _runner_ranking.rank_usable_candidates
_write_ranking_success_logs = _runner_ranking.write_ranking_success_logs
_ranking_was_cancelled = _runner_ranking.ranking_was_cancelled
_usable_ranking_candidates = _runner_ranking.usable_ranking_candidates
_ranking_success_analysis = _runner_ranking.ranking_success_analysis
_ranking_success_command = _runner_ranking.ranking_success_command


@dataclass(frozen=True)
class _RunnerDeps:
    finalize_xtb_job: Any
    now_utc_iso: Any
    start_xtb_job: Any
    _collect_opt_candidates: Any
    _collect_path_search_candidates: Any
    _collect_sp_candidates: Any


def _runner_deps() -> _RunnerDeps:
    return _RunnerDeps(
        finalize_xtb_job=finalize_xtb_job,
        now_utc_iso=now_utc_iso,
        start_xtb_job=start_xtb_job,
        _collect_opt_candidates=_collect_opt_candidates,
        _collect_path_search_candidates=_collect_path_search_candidates,
        _collect_sp_candidates=_collect_sp_candidates,
    )


@dataclass(frozen=True)
class XtbRunResult:
    status: str
    reason: str
    command: tuple[str, ...]
    exit_code: int
    started_at: str
    finished_at: str
    stdout_log: str
    stderr_log: str
    selected_input_xyz: str
    job_type: str
    reaction_key: str
    input_summary: dict[str, Any]
    candidate_count: int
    selected_candidate_paths: tuple[str, ...]
    candidate_details: tuple[dict[str, Any], ...]
    analysis_summary: dict[str, Any]
    manifest_path: str
    resource_request: dict[str, int]
    resource_actual: dict[str, int]


@dataclass
class XtbRunningJob:
    process: subprocess.Popen[str]
    command: tuple[str, ...]
    started_at: str
    stdout_log: str
    stderr_log: str
    stdout_handle: TextIO
    stderr_handle: TextIO
    selected_input_xyz: str
    job_type: str
    reaction_key: str
    input_summary: dict[str, Any]
    manifest_path: str
    resource_request: dict[str, int]
    resource_actual: dict[str, int]
    job_dir: str


def _resolve_xtb_executable(cfg: AppConfig) -> str:
    configured = str(cfg.paths.xtb_executable).strip()
    if configured:
        path = Path(configured).expanduser().resolve()
        if not path.exists() or not path.is_file():
            raise ValueError(f"Configured xTB executable not found: {path}")
        return str(path)

    discovered = shutil.which("xtb")
    if discovered:
        return discovered
    raise ValueError("xTB executable not configured and not found on PATH.")


def _resource_request_dict(cfg: AppConfig, manifest: dict[str, Any]) -> dict[str, int]:
    return resource_request_from_manifest(cfg, manifest)


def _resource_actual_dict(resource_request: dict[str, int]) -> dict[str, int]:
    return _config_engines.resource_actual_from_request(resource_request)


def _bool_flag(manifest: dict[str, Any], key: str) -> bool:
    return _config_engines.as_bool(manifest.get(key), False)


def _manifest_int(manifest: dict[str, Any], key: str) -> int | None:
    value = manifest.get(key)
    if value in (None, ""):
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        return int(stripped)
    if isinstance(value, (int, float)):
        return int(value)
    raise ValueError(f"Manifest field {key!r} must be an integer-compatible value.")


def _append_xtb_scalar_options(command: list[str], manifest: dict[str, Any]) -> None:
    gfn = str(manifest.get("gfn", "2")).strip()
    if gfn:
        command.extend(["--gfn", gfn])

    for manifest_key, option in (("charge", "--chrg"), ("uhf", "--uhf")):
        value = _manifest_int(manifest, manifest_key)
        if value is not None:
            command.extend([option, str(value)])


def _append_xtb_solvent_option(command: list[str], manifest: dict[str, Any]) -> None:
    solvent_model = str(manifest.get("solvent_model", "")).strip().lower()
    solvent = str(manifest.get("solvent", "")).strip()
    if solvent and solvent_model in {"gbsa", "alpb"}:
        command.extend([f"--{solvent_model}", solvent])


def _append_xtb_optional_text_options(command: list[str], manifest: dict[str, Any]) -> None:
    for manifest_key, option in (("namespace", "--namespace"), ("xcontrol", "--input")):
        value = str(manifest.get(manifest_key, "")).strip()
        if value:
            command.extend([option, value])


def _append_xtb_job_type_options(
    command: list[str],
    *,
    manifest: dict[str, Any],
    secondary_input_xyz: Path | None,
    job_type: str,
) -> None:
    if job_type == "path_search":
        if secondary_input_xyz is None:
            raise ValueError("path_search requires a product/reference structure")
        command.extend(["--path", str(secondary_input_xyz)])
        return
    if job_type == "opt":
        opt_level = (
            str(manifest.get("opt_level", manifest.get("opt", "normal"))).strip().lower()
            or "normal"
        )
        command.extend(["--opt", opt_level])
        return
    if job_type == "sp":
        command.append("--sp")
        return
    raise ValueError(f"Unsupported xtb job_type: {job_type}")


def _build_command(
    cfg: AppConfig,
    *,
    manifest: dict[str, Any],
    selected_input_xyz: Path,
    secondary_input_xyz: Path | None,
    job_type: str,
) -> list[str]:
    resource_request = _resource_request_dict(cfg, manifest)
    command = [
        _resolve_xtb_executable(cfg),
        str(selected_input_xyz),
        "--parallel",
        str(resource_request["max_cores"]),
        "--json",
    ]

    _append_xtb_scalar_options(command, manifest)
    _append_xtb_solvent_option(command, manifest)
    _append_xtb_optional_text_options(command, manifest)
    _append_xtb_job_type_options(
        command,
        manifest=manifest,
        secondary_input_xyz=secondary_input_xyz,
        job_type=job_type,
    )

    if _bool_flag(manifest, "dry_run"):
        command.append("--define")

    return command


def _run_candidate_sp_job(
    cfg: AppConfig,
    *,
    candidate_xyz: Path,
    candidate_run_dir: Path,
    manifest: dict[str, Any],
    should_cancel: Callable[[], bool] | None = None,
    on_running_job: Callable[[XtbRunningJob | None], None] | None = None,
    terminate_process: Callable[[subprocess.Popen[str]], None] | None = None,
) -> XtbRunResult:
    return _runner_execution.run_candidate_sp_job(
        cfg,
        candidate_xyz=candidate_xyz,
        candidate_run_dir=candidate_run_dir,
        manifest=manifest,
        should_cancel=should_cancel,
        on_running_job=on_running_job,
        terminate_process=terminate_process,
        deps=_runner_deps(),
    )


def _ranking_deps() -> _runner_ranking.RankingDeps:
    return _runner_ranking.RankingDeps(
        now_utc_iso=now_utc_iso,
        resource_request_dict=_resource_request_dict,
        resource_actual_dict=_resource_actual_dict,
        run_candidate_sp_job=_run_candidate_sp_job,
        extract_sp_energy=_extract_sp_energy,
        result_cls=XtbRunResult,
    )


def run_xtb_ranking_job(
    cfg: AppConfig,
    *,
    job_dir: Path,
    should_cancel: Callable[[], bool] | None = None,
    on_running_job: Callable[[XtbRunningJob | None], None] | None = None,
    terminate_process: Callable[[subprocess.Popen[str]], None] | None = None,
) -> XtbRunResult:
    manifest = load_job_manifest(job_dir)
    inputs = resolve_job_inputs(job_dir, manifest)
    return _runner_ranking.run_ranking_job(
        cfg,
        job_dir=job_dir,
        manifest=manifest,
        inputs=inputs,
        should_cancel=should_cancel,
        on_running_job=on_running_job,
        terminate_process=terminate_process,
        deps=_ranking_deps(),
    )


def _preexec_with_limits(max_memory_gb: int):
    return process_utils.memory_limit_preexec(
        max_memory_gb,
        setrlimit_fn=resource.setrlimit,
        limit_resource=resource.RLIMIT_AS,
    )


def start_xtb_job(cfg: AppConfig, *, job_dir: Path, selected_input_xyz: Path) -> XtbRunningJob:
    manifest = load_job_manifest(job_dir)
    resource_request = _resource_request_dict(cfg, manifest)
    resource_actual = _resource_actual_dict(resource_request)
    inputs = resolve_job_inputs(job_dir, manifest)
    secondary_raw = inputs.get("secondary_input_xyz")
    secondary_input_xyz = None
    if secondary_raw:
        secondary_input_xyz = Path(str(secondary_raw)).expanduser().resolve()
    command = _build_command(
        cfg,
        manifest=manifest,
        selected_input_xyz=selected_input_xyz,
        secondary_input_xyz=secondary_input_xyz,
        job_type=str(inputs["job_type"]),
    )

    stdout_log = job_dir / "xtb.stdout.log"
    stderr_log = job_dir / "xtb.stderr.log"
    launched = start_logged_process(
        command,
        cwd=job_dir,
        stdout_log=stdout_log,
        stderr_log=stderr_log,
        max_cores=resource_request["max_cores"],
        base_env=os.environ,
        now_utc_iso_fn=now_utc_iso,
        popen_fn=subprocess.Popen,
        stdin_value=subprocess.DEVNULL,
        preexec_fn=_preexec_with_limits(resource_request["max_memory_gb"]),
    )
    return XtbRunningJob(
        process=launched.process,
        command=tuple(command),
        started_at=launched.started_at,
        stdout_log=str(launched.stdout_log.resolve()),
        stderr_log=str(launched.stderr_log.resolve()),
        stdout_handle=launched.stdout_handle,
        stderr_handle=launched.stderr_handle,
        selected_input_xyz=str(selected_input_xyz.resolve()),
        job_type=str(inputs["job_type"]),
        reaction_key=str(inputs["reaction_key"]),
        input_summary=dict(inputs["input_summary"]),
        manifest_path=str((job_dir / MANIFEST_FILE_NAME).resolve()),
        job_dir=str(job_dir.resolve()),
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


def finalize_xtb_job(
    running: XtbRunningJob,
    *,
    forced_status: str | None = None,
    forced_reason: str | None = None,
) -> XtbRunResult:
    return _runner_finalize.finalize_xtb_job(
        running,
        forced_status=forced_status,
        forced_reason=forced_reason,
        result_cls=XtbRunResult,
        deps=_runner_deps(),
    )
