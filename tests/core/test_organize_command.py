from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from chemstack.core.commands import organize


def _cfg(
    allowed_root: Path,
    organized_root: Path | None = None,
    *,
    workflow_root: Path | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        workflow_root=str(workflow_root or ""),
        runtime=SimpleNamespace(
            allowed_root=str(allowed_root),
            organized_root=str(organized_root or allowed_root),
        ),
    )


def _plan(job_dir: Path, *, action: str = "organize", job_id: str | None = None) -> dict[str, str]:
    return {
        "action": action,
        "job_id": job_id or job_dir.name,
        "job_dir": str(job_dir),
        "target_dir": str(job_dir.parent / "organized" / job_dir.name),
    }


def test_run_organize_command_dry_run_prints_planned_and_skipped_jobs(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    root = tmp_path / "allowed"
    first = root / "job-a"
    second = root / "job-b"
    cfg = _cfg(root)

    exit_code = organize.run_organize_command(
        SimpleNamespace(config=None, apply=False),
        load_config_fn=lambda _config: cfg,
        resolve_scope_fn=lambda _cfg, _args: (None, None),
        default_scan_roots_fn=lambda _cfg: [root],
        iter_candidate_job_dirs_fn=lambda _root: [second, first],
        collect_plan_for_dir_fn=lambda _cfg, job_dir: _plan(job_dir, action="skip" if job_dir == second else "organize"),
        organize_job_dir_fn=lambda **_kwargs: pytest.fail("dry-run should not apply"),
        notify_organize_summary_fn=lambda **_kwargs: pytest.fail("dry-run should not notify"),
    )

    assert exit_code == 0
    assert capsys.readouterr().out.splitlines() == [
        "action: dry_run",
        "to_organize: 1",
        "skipped: 1",
        f"job-a: {first} -> {root / 'organized' / 'job-a'}",
    ]


def test_run_organize_command_apply_success_notifies_summary(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    root = tmp_path / "allowed"
    job_dir = root / "job-a"
    cfg = _cfg(root)
    notifications: list[dict[str, Any]] = []

    exit_code = organize.run_organize_command(
        SimpleNamespace(config=None, apply=True),
        load_config_fn=lambda _config: cfg,
        resolve_scope_fn=lambda _cfg, _args: (None, root),
        default_scan_roots_fn=lambda _cfg: pytest.fail("explicit root should be used"),
        iter_candidate_job_dirs_fn=lambda _root: [job_dir],
        collect_plan_for_dir_fn=lambda _cfg, candidate: _plan(candidate),
        organize_job_dir_fn=lambda _cfg, candidate, notify_summary: {
            **_plan(candidate),
            "target_dir": str(tmp_path / "done" / candidate.name),
        },
        notify_organize_summary_fn=lambda _cfg, **kwargs: notifications.append(kwargs),
    )

    assert exit_code == 0
    assert capsys.readouterr().out.splitlines() == [
        "action: apply",
        "organized: 1",
        "skipped: 0",
        "failed: 0",
        f"job-a: {tmp_path / 'done' / 'job-a'}",
    ]
    assert notifications == [{"organized_count": 1, "skipped_count": 0, "root": root}]


def test_run_organize_command_apply_failure_reports_and_counts_failure_as_skipped(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    root = tmp_path / "allowed"
    job_dir = root / "job-a"
    cfg = _cfg(root)
    notifications: list[dict[str, Any]] = []

    def apply_job(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        raise RuntimeError("copy failed")

    exit_code = organize.run_organize_command(
        SimpleNamespace(config=None, apply=True),
        load_config_fn=lambda _config: cfg,
        resolve_scope_fn=lambda _cfg, _args: (job_dir, None),
        default_scan_roots_fn=lambda _cfg: [],
        iter_candidate_job_dirs_fn=lambda _root: [],
        collect_plan_for_dir_fn=lambda _cfg, candidate: _plan(candidate),
        organize_job_dir_fn=apply_job,
        notify_organize_summary_fn=lambda _cfg, **kwargs: notifications.append(kwargs),
    )

    assert exit_code == 1
    assert capsys.readouterr().out.splitlines() == [
        "action: apply",
        "organized: 0",
        "skipped: 0",
        "failed: 1",
        f"failed: job-a ({'copy failed'})",
    ]
    assert notifications == [{"organized_count": 0, "skipped_count": 1, "root": job_dir}]


def test_resolve_scope_rejects_explicit_job_dir_and_root_conflict(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        organize.resolve_scope(
            _cfg(tmp_path / "allowed"),
            SimpleNamespace(job_dir=str(tmp_path / "allowed" / "job"), root=str(tmp_path / "allowed")),
            engine="xtb",
            resolve_job_dir_fn=lambda _cfg, raw: Path(raw),
        )


def test_resolve_scope_rejects_unsupported_scan_root(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    outside = tmp_path / "outside"
    allowed.mkdir()
    outside.mkdir()

    with pytest.raises(ValueError, match=f"Scan root must be under allowed_root: {allowed.resolve()}"):
        organize.resolve_scope(
            _cfg(allowed),
            SimpleNamespace(job_dir="", root=str(outside)),
            engine="xtb",
            resolve_job_dir_fn=lambda _cfg, raw: Path(raw),
        )


def test_organize_job_dir_applies_only_organize_plans_and_notifies(tmp_path: Path) -> None:
    job_dir = tmp_path / "job-a"
    cfg = _cfg(tmp_path)
    notifications: list[dict[str, Any]] = []

    skipped = organize.organize_job_dir(
        cfg,
        job_dir,
        collect_plan_for_dir_fn=lambda _cfg, _job_dir: _plan(job_dir, action="skip"),
        apply_plan_fn=lambda *_args: pytest.fail("skip plan should not apply"),
        notify_organize_summary_fn=lambda **_kwargs: pytest.fail("skip plan should not notify"),
    )
    organized = organize.organize_job_dir(
        cfg,
        job_dir,
        notify_summary=True,
        collect_plan_for_dir_fn=lambda _cfg, _job_dir: _plan(job_dir),
        apply_plan_fn=lambda _cfg, plan: {**plan, "target_dir": str(tmp_path / "done")},
        notify_organize_summary_fn=lambda _cfg, **kwargs: notifications.append(kwargs),
    )

    assert skipped["action"] == "skip"
    assert organized["target_dir"] == str(tmp_path / "done")
    assert notifications == [{"organized_count": 1, "skipped_count": 0, "root": job_dir}]
