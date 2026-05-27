from __future__ import annotations

import re
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.config.engines import WorkflowEngineAppConfig as AppConfig

from .commands._helpers import MANIFEST_FILE_NAME


@dataclass(frozen=True)
class RankingDeps:
    now_utc_iso: Callable[[], str]
    resource_request_dict: Callable[[AppConfig, dict[str, Any]], dict[str, int]]
    resource_actual_dict: Callable[[dict[str, int]], dict[str, int]]
    run_candidate_sp_job: Callable[..., Any]
    extract_sp_energy: Callable[[Path, Path], tuple[float | None, str]]
    result_cls: type[Any]


@dataclass(frozen=True)
class RankingRunContext:
    job_dir: Path
    started_at: str
    candidate_paths: list[Path]
    inputs: dict[str, Any]
    top_n: int
    resource_request: dict[str, int]
    resource_actual: dict[str, int]


@dataclass(frozen=True)
class RankingCollectedResults:
    candidate_results: list[dict[str, Any]]
    command_summary: list[list[str]]


def ranking_top_n(manifest: dict[str, Any]) -> int:
    raw = manifest.get("top_n", 3)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = 3
    return max(1, value)


def safe_rank_name(name: str, *, fallback: str) -> str:
    collapsed = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._-")
    return collapsed or fallback


def ranking_candidate_run_dir(ranking_root: Path, index: int, candidate_path: Path) -> Path:
    name = safe_rank_name(candidate_path.stem, fallback=f"candidate_{index:02d}")
    return ranking_root / f"{index:02d}_{name}"


def ranking_candidate_paths(inputs: dict[str, Any]) -> list[Path]:
    return [
        Path(path)
        for path in inputs.get("input_summary", {}).get("candidate_paths", [])
        if str(path).strip()
    ]


def ranking_context(
    cfg: AppConfig,
    *,
    job_dir: Path,
    manifest: dict[str, Any],
    inputs: dict[str, Any],
    candidate_paths: list[Path],
    deps: RankingDeps,
) -> RankingRunContext:
    resource_request = deps.resource_request_dict(cfg, manifest)
    return RankingRunContext(
        job_dir=job_dir,
        started_at=deps.now_utc_iso(),
        candidate_paths=candidate_paths,
        inputs=inputs,
        top_n=ranking_top_n(manifest),
        resource_request=resource_request,
        resource_actual=deps.resource_actual_dict(resource_request),
    )


def _write_text(path: Path, text: str) -> str:
    path.write_text(text, encoding="utf-8")
    return str(path.resolve())


def _run_ranking_candidate(
    cfg: AppConfig,
    *,
    candidate_path: Path,
    candidate_run_dir: Path,
    manifest: dict[str, Any],
    should_cancel: Callable[[], bool] | None,
    on_running_job: Callable[[Any | None], None] | None,
    terminate_process: Callable[[subprocess.Popen[str]], None] | None,
    deps: RankingDeps,
) -> Any:
    if should_cancel is None and on_running_job is None and terminate_process is None:
        return deps.run_candidate_sp_job(
            cfg,
            candidate_xyz=candidate_path,
            candidate_run_dir=candidate_run_dir,
            manifest=manifest,
        )
    return deps.run_candidate_sp_job(
        cfg,
        candidate_xyz=candidate_path,
        candidate_run_dir=candidate_run_dir,
        manifest=manifest,
        should_cancel=should_cancel,
        on_running_job=on_running_job,
        terminate_process=terminate_process,
    )


def ranking_candidate_result(
    *,
    candidate_path: Path,
    candidate_run_dir: Path,
    result: Any,
    energy: float | None,
    energy_source: str,
) -> dict[str, Any]:
    return {
        "candidate_path": str(candidate_path.resolve()),
        "candidate_run_dir_path": str(candidate_run_dir.resolve()),
        "status": result.status,
        "reason": result.reason,
        "exit_code": result.exit_code,
        "selected_input_xyz": result.selected_input_xyz,
        "total_energy": energy,
        "energy_source": energy_source,
        "command": list(result.command),
        "analysis_summary": dict(result.analysis_summary),
    }


def collect_ranking_candidate_results(
    cfg: AppConfig,
    *,
    ranking_root: Path,
    manifest: dict[str, Any],
    candidate_paths: list[Path],
    should_cancel: Callable[[], bool] | None,
    on_running_job: Callable[[Any | None], None] | None,
    terminate_process: Callable[[subprocess.Popen[str]], None] | None,
    deps: RankingDeps,
) -> tuple[list[dict[str, Any]], list[list[str]]]:
    candidate_results: list[dict[str, Any]] = []
    command_summary: list[list[str]] = []
    for index, candidate_path in enumerate(candidate_paths, start=1):
        if should_cancel is not None and should_cancel():
            break
        candidate_run_dir = ranking_candidate_run_dir(ranking_root, index, candidate_path)
        result = _run_ranking_candidate(
            cfg,
            candidate_path=candidate_path,
            candidate_run_dir=candidate_run_dir,
            manifest=manifest,
            should_cancel=should_cancel,
            on_running_job=on_running_job,
            terminate_process=terminate_process,
            deps=deps,
        )
        energy, energy_source = deps.extract_sp_energy(candidate_run_dir, candidate_path)
        command_summary.append(list(result.command))
        candidate_results.append(
            ranking_candidate_result(
                candidate_path=candidate_path,
                candidate_run_dir=candidate_run_dir,
                result=result,
                energy=energy,
                energy_source=energy_source,
            )
        )
        if result.status == "cancelled":
            break
    return candidate_results, command_summary


def ranking_unsuccessful_detail(item: dict[str, Any], rank: int) -> dict[str, Any]:
    return {
        "rank": rank,
        "kind": "ranking_candidate",
        "path": item["candidate_path"],
        "candidate_run_dir_path": item["candidate_run_dir_path"],
        "energy_source": item["energy_source"],
        "status": item["status"],
        "reason": item["reason"],
        "exit_code": item["exit_code"],
        "selected": False,
    }


def ranking_failure_analysis(
    *,
    candidate_results: list[dict[str, Any]],
    top_n: int,
    failure_reason: str,
) -> dict[str, Any]:
    return {
        "ranking_metric": "total_energy",
        "evaluated_candidate_count": len(candidate_results),
        "candidate_paths": [item["candidate_path"] for item in candidate_results],
        "candidate_run_dir_paths": [item["candidate_run_dir_path"] for item in candidate_results],
        "candidate_results": candidate_results,
        "top_n": top_n,
        "failure_reason": failure_reason,
    }


def ranking_terminal_result(
    context: RankingRunContext,
    *,
    status: str,
    reason: str,
    command: tuple[str, ...],
    stdout_text: str,
    stderr_text: str,
    candidate_results: list[dict[str, Any]],
    deps: RankingDeps,
) -> Any:
    summary_stdout = context.job_dir / "ranking.stdout.log"
    summary_stderr = context.job_dir / "ranking.stderr.log"
    _write_text(summary_stdout, stdout_text)
    _write_text(summary_stderr, stderr_text)
    return deps.result_cls(
        status=status,
        reason=reason,
        command=command,
        exit_code=1,
        started_at=context.started_at,
        finished_at=deps.now_utc_iso(),
        stdout_log=str(summary_stdout.resolve()),
        stderr_log=str(summary_stderr.resolve()),
        selected_input_xyz=str(context.candidate_paths[0].resolve()),
        job_type="ranking",
        reaction_key=str(context.inputs["reaction_key"]),
        input_summary=dict(context.inputs["input_summary"]),
        candidate_count=len(context.candidate_paths),
        selected_candidate_paths=(),
        candidate_details=tuple(
            ranking_unsuccessful_detail(item, idx + 1) for idx, item in enumerate(candidate_results)
        ),
        analysis_summary=ranking_failure_analysis(
            candidate_results=candidate_results,
            top_n=context.top_n,
            failure_reason=reason,
        ),
        manifest_path=str((context.job_dir / MANIFEST_FILE_NAME).resolve()),
        resource_request=context.resource_request,
        resource_actual=context.resource_actual,
    )


def rank_usable_candidates(
    candidate_results: list[dict[str, Any]],
    *,
    top_n: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    ranked = sorted(candidate_results, key=lambda item: float(item["total_energy"]))
    candidate_details: list[dict[str, Any]] = []
    selected_paths: list[str] = []
    for rank, item in enumerate(ranked, start=1):
        is_selected = rank <= top_n
        candidate_details.append(
            {
                "rank": rank,
                "kind": "ranking_candidate",
                "path": item["candidate_path"],
                "candidate_run_dir_path": item["candidate_run_dir_path"],
                "energy_source": item["energy_source"],
                "total_energy": item["total_energy"],
                "score": round(-float(item["total_energy"]), 6),
                "status": item["status"],
                "reason": item["reason"],
                "exit_code": item["exit_code"],
                "selected": is_selected,
            }
        )
        if is_selected:
            selected_paths.append(item["candidate_path"])
    return ranked, candidate_details, selected_paths


def write_ranking_success_logs(
    job_dir: Path,
    *,
    candidate_results: list[dict[str, Any]],
    selected_paths: list[str],
    usable_count: int,
    failed_count: int,
    best: dict[str, Any],
) -> tuple[Path, Path]:
    summary_stdout = job_dir / "ranking.stdout.log"
    summary_stderr = job_dir / "ranking.stderr.log"
    stdout_lines = [
        f"ranking completed: evaluated={len(candidate_results)} selected={len(selected_paths)}",
        f"best_candidate: {best['candidate_path']}",
        f"best_total_energy: {best['total_energy']}",
    ]
    if failed_count:
        stdout_lines.append(f"failed_candidates: {failed_count}")
    stdout_lines.append(f"usable_candidates: {usable_count}")
    _write_text(summary_stdout, "\n".join(stdout_lines) + "\n")
    _write_text(summary_stderr, "")
    return summary_stdout, summary_stderr


def ranking_was_cancelled(
    collected: RankingCollectedResults,
    *,
    should_cancel: Callable[[], bool] | None,
) -> bool:
    return any(item["status"] == "cancelled" for item in collected.candidate_results) or (
        should_cancel is not None and should_cancel()
    )


def usable_ranking_candidates(candidate_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        item
        for item in candidate_results
        if item.get("total_energy") is not None and item["status"] == "completed"
    ]


def ranking_cancelled_result(
    context: RankingRunContext,
    collected: RankingCollectedResults,
    *,
    deps: RankingDeps,
) -> Any:
    return ranking_terminal_result(
        context,
        status="cancelled",
        reason="cancel_requested",
        command=tuple(collected.command_summary[0]) if collected.command_summary else tuple(),
        stdout_text="ranking cancelled: cancel_requested\n",
        stderr_text="",
        candidate_results=collected.candidate_results,
        deps=deps,
    )


def ranking_failed_result(
    context: RankingRunContext,
    collected: RankingCollectedResults,
    *,
    deps: RankingDeps,
) -> Any:
    failure_reason = "ranking_no_usable_energy"
    return ranking_terminal_result(
        context,
        status="failed",
        reason=failure_reason,
        command=tuple(),
        stdout_text=f"ranking failed: {failure_reason}\n",
        stderr_text="no candidate produced a usable xTB energy\n",
        candidate_results=collected.candidate_results,
        deps=deps,
    )


def ranking_success_analysis(
    *,
    candidate_results: list[dict[str, Any]],
    usable_count: int,
    failed_count: int,
    best: dict[str, Any],
    top_n: int,
    selected_paths: list[str],
    command_summary: list[list[str]],
) -> dict[str, Any]:
    return {
        "ranking_metric": "total_energy",
        "evaluated_candidate_count": len(candidate_results),
        "usable_candidate_count": usable_count,
        "failed_candidate_count": failed_count,
        "candidate_paths": [item["candidate_path"] for item in candidate_results],
        "candidate_run_dir_paths": [item["candidate_run_dir_path"] for item in candidate_results],
        "candidate_results": candidate_results,
        "best_candidate_path": best["candidate_path"],
        "best_total_energy": best["total_energy"],
        "top_n": top_n,
        "selected_candidate_paths": list(selected_paths),
        "command_summary": command_summary,
    }


def ranking_success_command(
    *,
    selected: list[dict[str, Any]],
    command_summary: list[list[str]],
) -> tuple[str, ...]:
    if selected:
        return tuple(selected[0]["command"])
    if command_summary:
        return tuple(command_summary[0])
    return tuple()


def ranking_completed_result(
    context: RankingRunContext,
    collected: RankingCollectedResults,
    *,
    usable: list[dict[str, Any]],
    deps: RankingDeps,
) -> Any:
    ranked, candidate_details, selected_paths = rank_usable_candidates(
        usable,
        top_n=context.top_n,
    )
    selected = ranked[: context.top_n]
    failed_count = len(collected.candidate_results) - len(usable)
    best = ranked[0]
    summary_stdout, summary_stderr = write_ranking_success_logs(
        context.job_dir,
        candidate_results=collected.candidate_results,
        selected_paths=selected_paths,
        usable_count=len(usable),
        failed_count=failed_count,
        best=best,
    )
    return deps.result_cls(
        status="completed",
        reason="completed",
        command=ranking_success_command(
            selected=selected,
            command_summary=collected.command_summary,
        ),
        exit_code=0,
        started_at=context.started_at,
        finished_at=deps.now_utc_iso(),
        stdout_log=str(summary_stdout.resolve()),
        stderr_log=str(summary_stderr.resolve()),
        selected_input_xyz=str(best["candidate_path"]),
        job_type="ranking",
        reaction_key=str(context.inputs["reaction_key"]),
        input_summary=dict(context.inputs["input_summary"]),
        candidate_count=len(collected.candidate_results),
        selected_candidate_paths=tuple(selected_paths),
        candidate_details=tuple(candidate_details),
        analysis_summary=ranking_success_analysis(
            candidate_results=collected.candidate_results,
            usable_count=len(usable),
            failed_count=failed_count,
            best=best,
            top_n=context.top_n,
            selected_paths=selected_paths,
            command_summary=collected.command_summary,
        ),
        manifest_path=str((context.job_dir / MANIFEST_FILE_NAME).resolve()),
        resource_request=context.resource_request,
        resource_actual=context.resource_actual,
    )


def run_ranking_job(
    cfg: AppConfig,
    *,
    job_dir: Path,
    manifest: dict[str, Any],
    inputs: dict[str, Any],
    should_cancel: Callable[[], bool] | None = None,
    on_running_job: Callable[[Any | None], None] | None = None,
    terminate_process: Callable[[subprocess.Popen[str]], None] | None = None,
    deps: RankingDeps,
) -> Any:
    candidate_paths = ranking_candidate_paths(inputs)
    if not candidate_paths:
        raise ValueError(f"No ranking candidates available in job directory: {job_dir}")

    ranking_root = job_dir / ".ranking_runs"
    ranking_root.mkdir(parents=True, exist_ok=True)
    context = ranking_context(
        cfg,
        job_dir=job_dir,
        manifest=manifest,
        inputs=inputs,
        candidate_paths=candidate_paths,
        deps=deps,
    )

    collected = RankingCollectedResults(
        *collect_ranking_candidate_results(
            cfg,
            ranking_root=ranking_root,
            manifest=manifest,
            candidate_paths=candidate_paths,
            should_cancel=should_cancel,
            on_running_job=on_running_job,
            terminate_process=terminate_process,
            deps=deps,
        )
    )
    if ranking_was_cancelled(collected, should_cancel=should_cancel):
        return ranking_cancelled_result(context, collected, deps=deps)

    usable = usable_ranking_candidates(collected.candidate_results)
    if not usable:
        return ranking_failed_result(context, collected, deps=deps)

    return ranking_completed_result(context, collected, usable=usable, deps=deps)
