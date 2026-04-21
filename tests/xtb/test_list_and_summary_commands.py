from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from chemstack.core.config import CommonRuntimeConfig
from chemstack.core.queue import dequeue_next, enqueue, request_cancel

from chemstack.xtb.commands import list_jobs, summary as summary_cmd
from chemstack.xtb.config import AppConfig
from chemstack.xtb.tracking import upsert_job_record
from chemstack.xtb import state as state_mod


def _make_cfg(tmp_path: Path) -> AppConfig:
    allowed_root = tmp_path / "allowed"
    organized_root = tmp_path / "organized"
    allowed_root.mkdir()
    organized_root.mkdir()
    return AppConfig(
        runtime=CommonRuntimeConfig(
            allowed_root=str(allowed_root),
            organized_root=str(organized_root),
        )
    )


def _write_job_artifacts(
    job_dir: Path,
    *,
    job_id: str = "job-1",
    status: str = "completed",
    reason: str = "xtb_ok",
    job_type: str = "ranking",
    reaction_key: str = "rxn-report",
) -> dict[str, Path]:
    inputs_dir = job_dir / "inputs"
    candidates_dir = job_dir / "candidates"
    logs_dir = job_dir / "logs"
    inputs_dir.mkdir(parents=True, exist_ok=True)
    candidates_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    selected_xyz = inputs_dir / "selected.xyz"
    candidate_xyz = candidates_dir / "candidate-1.xyz"
    stdout_log = logs_dir / "xtb.stdout.log"
    stderr_log = logs_dir / "xtb.stderr.log"
    selected_xyz.write_text("3\nselected\nH 0 0 0\n", encoding="utf-8")
    candidate_xyz.write_text("3\ncandidate\nH 0 0 0\n", encoding="utf-8")
    stdout_log.write_text("stdout\n", encoding="utf-8")
    stderr_log.write_text("stderr\n", encoding="utf-8")

    state_mod.write_state(
        job_dir,
        {
            "job_id": job_id,
            "job_dir": str(job_dir),
            "status": "queued",
            "reason": "queued",
            "job_type": job_type,
            "reaction_key": "rxn-state",
            "selected_input_xyz": str(selected_xyz),
            "candidate_count": 1,
            "selected_candidate_paths": [str(candidate_xyz)],
            "analysis_summary": {"candidate_paths": [str(candidate_xyz)]},
            "resource_request": {"max_cores": 4, "max_memory_gb": 8},
            "resource_actual": {"assigned_cores": 2, "memory_limit_gb": 8},
        },
    )
    state_mod.write_report_json(
        job_dir,
        {
            "job_id": job_id,
            "job_dir": str(job_dir),
            "status": status,
            "reason": reason,
            "job_type": job_type,
            "reaction_key": reaction_key,
            "selected_input_xyz": str(selected_xyz),
            "candidate_count": 1,
            "selected_candidate_paths": [str(candidate_xyz)],
            "analysis_summary": {"best_candidate_path": str(candidate_xyz)},
            "resource_request": {"max_cores": 4, "max_memory_gb": 8},
            "resource_actual": {"assigned_cores": 4, "memory_limit_gb": 8},
            "stdout_log": str(stdout_log),
            "stderr_log": str(stderr_log),
        },
    )
    return {
        "selected_xyz": selected_xyz,
        "candidate_xyz": candidate_xyz,
        "stdout_log": stdout_log,
        "stderr_log": stderr_log,
    }


def test_cmd_list_prints_no_jobs_message(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_cfg(tmp_path)
    monkeypatch.setattr(list_jobs, "load_config", lambda _path=None: cfg)

    result = list_jobs.cmd_list(SimpleNamespace(config=None))

    assert result == 0
    assert capsys.readouterr().out == "No xTB jobs found.\n"


def test_cmd_list_prints_queue_rows_with_status_and_placeholders(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_cfg(tmp_path)
    running_job_dir = Path(cfg.runtime.allowed_root) / "job-running"
    running_job_dir.mkdir()

    running = enqueue(
        cfg.runtime.allowed_root,
        app_name="xtb_auto",
        task_id="job-running",
        task_kind="xtb_ranking",
        engine="xtb",
        priority=4,
        metadata={
            "job_dir": str(running_job_dir),
            "job_type": "ranking",
            "reaction_key": "rxn-1",
        },
    )
    dequeue_next(cfg.runtime.allowed_root)
    request_cancel(cfg.runtime.allowed_root, running.queue_id)

    pending = enqueue(
        cfg.runtime.allowed_root,
        app_name="xtb_auto",
        task_id="job-pending",
        task_kind="xtb_sp",
        engine="xtb",
        priority=9,
        metadata={},
    )

    monkeypatch.setattr(list_jobs, "load_config", lambda _path=None: cfg)

    result = list_jobs.cmd_list(SimpleNamespace(config=None))

    assert result == 0
    output_lines = capsys.readouterr().out.splitlines()
    assert output_lines[0] == "xTB queue: 2 entries"
    assert output_lines[2].startswith("QUEUE ID")

    running_line = next(line for line in output_lines if running.queue_id in line)
    pending_line = next(line for line in output_lines if pending.queue_id in line)
    assert "cancel_requested" in running_line
    assert "ranking" in running_line
    assert "rxn-1" in running_line
    assert running_line.rstrip().endswith("job-running")
    assert "pending" in pending_line
    assert pending_line.rstrip().endswith("-")


def test_cmd_summary_requires_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_cfg(tmp_path)
    monkeypatch.setattr(summary_cmd, "load_config", lambda _path=None: cfg)

    result = summary_cmd.cmd_summary(SimpleNamespace(config=None, target="  ", json=False))

    assert result == 1
    assert capsys.readouterr().out == "error: summary requires a job_id or job directory\n"


def test_cmd_summary_reports_missing_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_cfg(tmp_path)
    monkeypatch.setattr(summary_cmd, "load_config", lambda _path=None: cfg)

    result = summary_cmd.cmd_summary(SimpleNamespace(config=None, target="missing-job", json=False))

    assert result == 1
    assert capsys.readouterr().out == "error: job not found: missing-job\n"


def test_cmd_summary_json_accepts_job_directory_path_without_index_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_cfg(tmp_path)
    job_dir = Path(cfg.runtime.allowed_root) / "job-direct"
    artifacts = _write_job_artifacts(job_dir, job_id="job-direct")

    monkeypatch.setattr(summary_cmd, "load_config", lambda _path=None: cfg)

    result = summary_cmd.cmd_summary(SimpleNamespace(config=None, target=str(job_dir), json=True))

    payload = json.loads(capsys.readouterr().out)
    assert result == 0
    assert payload["target"] == str(job_dir)
    assert payload["job_dir"] == str(job_dir.resolve())
    assert payload["index_record"] is None
    assert payload["state"]["job_id"] == "job-direct"
    assert payload["report"]["stdout_log"] == str(artifacts["stdout_log"])


def test_cmd_summary_prints_text_report_with_index_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_cfg(tmp_path)
    job_dir = Path(cfg.runtime.allowed_root) / "job-1"
    artifacts = _write_job_artifacts(job_dir, job_id="job-1")

    upsert_job_record(
        cfg,
        job_id="job-1",
        status="completed",
        job_dir=job_dir,
        job_type="ranking",
        selected_input_xyz=str(artifacts["selected_xyz"]),
        reaction_key="rxn-index",
        resource_request={"max_cores": 4, "max_memory_gb": 8},
        resource_actual={"assigned_cores": 4, "memory_limit_gb": 8},
    )

    monkeypatch.setattr(summary_cmd, "load_config", lambda _path=None: cfg)

    result = summary_cmd.cmd_summary(SimpleNamespace(config=None, target="job-1", json=False))

    output = capsys.readouterr().out
    assert result == 0
    assert f"job_dir: {job_dir.resolve()}" in output
    assert "job_id: job-1" in output
    assert f"latest_known_path: {job_dir.resolve()}" in output
    assert "reaction_key: rxn-index" in output
    assert f"selected_input_xyz: {artifacts['selected_xyz']}" in output
    assert "status: completed" in output
    assert "reason: xtb_ok" in output
    assert "job_type: ranking" in output
    assert "reaction_key: rxn-report" in output
    assert "candidate_count: 1" in output
    assert f"selected_candidate_paths: ['{artifacts['candidate_xyz']}']" in output
    assert f"analysis_summary: {{'best_candidate_path': '{artifacts['candidate_xyz']}'}}" in output
    assert "resource_request: {'max_cores': 4, 'max_memory_gb': 8}" in output
    assert "resource_actual: {'assigned_cores': 4, 'memory_limit_gb': 8}" in output
    assert f"stdout_log: {artifacts['stdout_log']}" in output
    assert f"stderr_log: {artifacts['stderr_log']}" in output


def test_cmd_summary_prints_organized_output_dir_when_index_record_has_one(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_cfg(tmp_path)
    job_dir = Path(cfg.runtime.allowed_root) / "job-2"
    organized_dir = Path(cfg.runtime.organized_root) / "ranking" / "rxn-organized" / "job-2"
    artifacts = _write_job_artifacts(job_dir, job_id="job-2")

    upsert_job_record(
        cfg,
        job_id="job-2",
        status="completed",
        job_dir=job_dir,
        job_type="ranking",
        selected_input_xyz=str(artifacts["selected_xyz"]),
        organized_output_dir=organized_dir,
        reaction_key="rxn-organized",
        resource_request={"max_cores": 4, "max_memory_gb": 8},
        resource_actual={"assigned_cores": 4, "memory_limit_gb": 8},
    )

    monkeypatch.setattr(summary_cmd, "load_config", lambda _path=None: cfg)

    result = summary_cmd.cmd_summary(SimpleNamespace(config=None, target="job-2", json=False))

    output = capsys.readouterr().out
    assert result == 0
    assert f"organized_output_dir: {organized_dir.resolve()}" in output
