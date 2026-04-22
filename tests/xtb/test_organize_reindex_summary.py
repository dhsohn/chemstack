from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from chemstack.core.indexing import get_job_location
from chemstack.xtb.commands import organize as organize_cmd
from chemstack.xtb.commands import reindex as reindex_cmd
from chemstack.xtb.commands import summary as summary_cmd
from chemstack.xtb import state as state_mod


def _make_cfg(tmp_path: Path) -> SimpleNamespace:
    allowed_root = tmp_path / "allowed"
    organized_root = tmp_path / "organized"
    admission_root = tmp_path / "admission"
    allowed_root.mkdir()
    organized_root.mkdir()
    admission_root.mkdir()
    return SimpleNamespace(
        runtime=SimpleNamespace(
            allowed_root=str(allowed_root),
            organized_root=str(organized_root),
            max_concurrent=2,
            admission_root=str(admission_root),
            admission_limit=2,
        ),
        behavior=SimpleNamespace(auto_organize_on_terminal=False),
        resources=SimpleNamespace(max_cores_per_task=4, max_memory_gb_per_task=8),
        telegram=SimpleNamespace(bot_token="", chat_id=""),
        paths=SimpleNamespace(xtb_executable=""),
    )


def _write_job_artifacts(
    job_dir: Path,
    *,
    job_id: str,
    status: str = "completed",
    job_type: str = "ranking",
    reaction_key: str = "reaction-1",
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

    payload = {
        "job_id": job_id,
        "job_dir": str(job_dir),
        "selected_input_xyz": str(selected_xyz),
        "job_type": job_type,
        "reaction_key": reaction_key,
        "input_summary": {
            "candidates_dir": str(candidates_dir),
            "candidate_count": 1,
            "candidate_paths": [str(candidate_xyz)],
        },
        "status": status,
        "reason": "xtb_ok" if status == "completed" else status,
        "candidate_count": 1,
        "candidate_paths": [str(candidate_xyz)],
        "selected_candidate_paths": [str(candidate_xyz)],
        "candidate_details": [{"path": str(candidate_xyz)}],
        "analysis_summary": {
            "best_candidate_path": str(candidate_xyz),
            "candidate_paths": [str(candidate_xyz)],
            "output_dir": str(candidates_dir),
        },
        "resource_request": {"max_cores": 4, "max_memory_gb": 8},
        "resource_actual": {"assigned_cores": 4, "memory_limit_gb": 8},
    }
    state_mod.write_state(job_dir, payload)
    state_mod.write_report_json(
        job_dir,
        {
            **payload,
            "queue_id": f"queue-{job_id}",
            "command": ["xtb", str(selected_xyz)],
            "exit_code": 0 if status == "completed" else 1,
            "started_at": "2026-04-20T00:00:00Z",
            "finished_at": "2026-04-20T00:05:00Z",
            "stdout_log": str(stdout_log),
            "stderr_log": str(stderr_log),
            "organized_output_dir": "",
            "latest_known_path": str(job_dir),
        },
    )
    state_mod.write_report_md_lines(job_dir, ["# xtb_auto Report", "", "- Existing report line"])
    return {
        "selected_xyz": selected_xyz,
        "candidate_xyz": candidate_xyz,
        "candidates_dir": candidates_dir,
        "stdout_log": stdout_log,
        "stderr_log": stderr_log,
    }


def test_cmd_organize_apply_moves_terminal_job_and_rewrites_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_cfg(tmp_path)
    job_dir = Path(cfg.runtime.allowed_root) / "job-1"
    artifacts = _write_job_artifacts(job_dir, job_id="job-1", reaction_key="rxn-1")
    target_dir = Path(cfg.runtime.organized_root) / "ranking" / "rxn-1" / "job-1"

    monkeypatch.setattr(organize_cmd, "load_config", lambda _path=None: cfg)
    monkeypatch.setattr(organize_cmd, "notify_organize_summary", lambda *args, **kwargs: True)

    exit_code = organize_cmd.cmd_organize(
        SimpleNamespace(config=None, job_dir="", root=str(Path(cfg.runtime.allowed_root)), apply=True)
    )

    captured = capsys.readouterr().out
    assert exit_code == 0
    assert "action: apply" in captured
    assert "organized: 1" in captured
    assert target_dir.exists()

    organized_ref = state_mod.load_organized_ref(job_dir)
    organized_state = state_mod.load_state(target_dir)
    organized_report = state_mod.load_report_json(target_dir)
    assert organized_ref is not None
    assert organized_state is not None
    assert organized_report is not None
    assert organized_ref["organized_output_dir"] == str(target_dir)
    assert organized_state["job_dir"] == str(target_dir)
    assert organized_state["selected_input_xyz"] == str(target_dir / "inputs" / artifacts["selected_xyz"].name)
    assert organized_state["input_summary"]["candidates_dir"] == str(target_dir / "candidates")
    assert organized_state["selected_candidate_paths"] == [str(target_dir / "candidates" / artifacts["candidate_xyz"].name)]
    assert organized_state["analysis_summary"]["best_candidate_path"] == str(
        target_dir / "candidates" / artifacts["candidate_xyz"].name
    )
    assert "## Organization" in (target_dir / state_mod.REPORT_MD_FILE_NAME).read_text(encoding="utf-8")


def test_cmd_reindex_scans_allowed_and_organized_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_cfg(tmp_path)
    allowed_job_dir = Path(cfg.runtime.allowed_root) / "job-1"
    organized_job_dir = Path(cfg.runtime.organized_root) / "ranking" / "rxn-2" / "job-2"
    _write_job_artifacts(allowed_job_dir, job_id="job-1", reaction_key="rxn-1")
    _write_job_artifacts(organized_job_dir, job_id="job-2", reaction_key="rxn-2")

    monkeypatch.setattr(reindex_cmd, "load_config", lambda _path=None: cfg)

    exit_code = reindex_cmd.cmd_reindex(SimpleNamespace(config=None, root=None))

    captured = capsys.readouterr().out
    assert exit_code == 0
    assert "index_roots: 1" in captured
    assert "scan_roots: 2" in captured
    assert "candidate_dirs: 2" in captured
    assert "indexed: 2" in captured
    assert "skipped: 0" in captured
    assert get_job_location(cfg.runtime.allowed_root, "job-1") is not None
    assert get_job_location(cfg.runtime.allowed_root, "job-2") is not None


def test_cmd_summary_json_returns_index_record_and_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_cfg(tmp_path)
    job_dir = Path(cfg.runtime.allowed_root) / "job-1"
    artifacts = _write_job_artifacts(job_dir, job_id="job-1", reaction_key="rxn-1")

    monkeypatch.setattr(reindex_cmd, "load_config", lambda _path=None: cfg)
    monkeypatch.setattr(summary_cmd, "load_config", lambda _path=None: cfg)

    reindex_cmd.cmd_reindex(SimpleNamespace(config=None, root=None))
    capsys.readouterr()

    exit_code = summary_cmd.cmd_summary(SimpleNamespace(config=None, target="job-1", json=True))

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["job_dir"] == str(job_dir)
    assert payload["index_record"]["job_id"] == "job-1"
    assert payload["index_record"]["job_type"] == "xtb_ranking"
    assert payload["index_record"]["reaction_key"] == "rxn-1"
    assert payload["index_record"]["selected_input_xyz"] == str(artifacts["selected_xyz"])
    assert payload["state"]["status"] == "completed"
    assert payload["report"]["stdout_log"] == str(artifacts["stdout_log"])
