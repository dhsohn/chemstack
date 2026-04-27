from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

from chemstack.core.indexing import get_job_location

from chemstack.crest.commands import reindex as reindex_cmd
from chemstack.crest.commands.organize import cmd_organize, organize_job_dir
from chemstack.crest.commands.reindex import cmd_reindex
from chemstack.crest.commands.summary import cmd_summary
from chemstack.crest.config import load_config
from chemstack.crest.state import (
    load_organized_ref,
    load_report_json,
    load_state,
    write_report_json,
    write_report_md,
    write_state,
)


def _write_config(tmp_path: Path) -> tuple[Path, Path, Path]:
    workflow_root = tmp_path / "workflow_root"
    allowed_root = workflow_root / "wf_001" / "01_crest"
    organized_root = allowed_root
    allowed_root.mkdir(parents=True)
    config_path = tmp_path / "chemstack.yaml"
    config_path.write_text(
        "\n".join(
            [
                "workflow:",
                f"  root: {json.dumps(str(workflow_root))}",
                "resources:",
                "  max_cores_per_task: 8",
                "  max_memory_gb_per_task: 16",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return config_path, allowed_root, organized_root


def _write_xyz(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("1\nexample\nH 0.0 0.0 0.0\n", encoding="utf-8")


def _write_job_artifacts(
    job_dir: Path,
    *,
    job_id: str,
    status: str,
    mode: str = "standard",
    reason: str | None = None,
    selected_name: str = "sample.xyz",
    molecule_key: str | None = None,
    resource_request: dict[str, int] | None = None,
    resource_actual: dict[str, int] | None = None,
    original_run_dir: str | None = None,
    organized_output_dir: str | None = None,
    latest_known_path: str | None = None,
    include_report: bool = True,
) -> Path:
    job_dir.mkdir(parents=True, exist_ok=True)
    selected_xyz = job_dir / selected_name
    _write_xyz(selected_xyz)
    stdout_log = job_dir / "crest.stdout.log"
    stderr_log = job_dir / "crest.stderr.log"
    stdout_log.write_text("stdout\n", encoding="utf-8")
    stderr_log.write_text("stderr\n", encoding="utf-8")

    normalized_reason = reason or status
    normalized_molecule_key = molecule_key or selected_xyz.stem
    request = dict(resource_request or {"max_cores": 4, "max_memory_gb": 8})
    actual = dict(resource_actual or request)
    state_payload = {
        "job_id": job_id,
        "job_dir": str(job_dir.resolve()),
        "status": status,
        "reason": normalized_reason,
        "mode": mode,
        "molecule_key": normalized_molecule_key,
        "selected_input_xyz": str(selected_xyz.resolve()),
        "resource_request": request,
        "resource_actual": actual,
        "retained_conformer_count": 2,
    }
    if original_run_dir is not None:
        state_payload["original_run_dir"] = original_run_dir
    if organized_output_dir is not None:
        state_payload["organized_output_dir"] = organized_output_dir
    if latest_known_path is not None:
        state_payload["latest_known_path"] = latest_known_path
    write_state(job_dir, state_payload)

    if include_report:
        report_payload = dict(state_payload)
        report_payload["stdout_log"] = str(stdout_log.resolve())
        report_payload["stderr_log"] = str(stderr_log.resolve())
        write_report_json(job_dir, report_payload)
        write_report_md(
            job_dir,
            job_id=job_id,
            status=status,
            reason=normalized_reason,
            selected_xyz=selected_xyz.name,
        )

    return selected_xyz


def test_organize_job_dir_skips_workflow_stage_job_without_outputs(tmp_path: Path) -> None:
    config_path, allowed_root, organized_root = _write_config(tmp_path)
    cfg = load_config(str(config_path))
    job_dir = allowed_root / "job-complete"
    selected_xyz = _write_job_artifacts(
        job_dir,
        job_id="job-001",
        status="completed",
        selected_name="water.xyz",
        resource_request={"max_cores": 6, "max_memory_gb": 12},
        resource_actual={"max_cores": 4, "max_memory_gb": 10},
    )

    result = organize_job_dir(cfg, job_dir)

    assert result == {
        "action": "skip",
        "job_dir": str(job_dir.resolve()),
        "reason": "already_under_organized_root",
    }
    assert (job_dir / selected_xyz.name).exists()
    assert not (organized_root / "standard").exists()

    organized_ref = load_organized_ref(job_dir)
    assert organized_ref is None

    state = load_state(job_dir)
    report = load_report_json(job_dir)
    assert state is not None
    assert report is not None
    assert state["job_dir"] == str(job_dir.resolve())
    assert "organized_output_dir" not in state
    assert "latest_known_path" not in state
    report_md = (job_dir / "job_report.md").read_text(encoding="utf-8")
    assert "## Organization" not in report_md

    record = get_job_location(allowed_root, "job-001")
    assert record is None


def test_cmd_organize_dry_run_lists_planned_moves_without_moving_files(
    tmp_path: Path,
    capsys,
) -> None:
    config_path, allowed_root, organized_root = _write_config(tmp_path)
    completed_job = allowed_root / "job-complete"
    running_job = allowed_root / "job-running"
    _write_job_artifacts(completed_job, job_id="job-100", status="completed", selected_name="ethanol.xyz")
    _write_job_artifacts(running_job, job_id="job-200", status="running", selected_name="methane.xyz")

    rc = cmd_organize(
        Namespace(
            config=str(config_path),
            job_dir=None,
            root=str(allowed_root),
            apply=False,
        )
    )

    captured = capsys.readouterr().out
    assert rc == 0
    assert "action: dry_run" in captured
    assert "to_organize: 0" in captured
    assert "skipped: 2" in captured
    assert f"job-100: {completed_job.resolve()} ->" not in captured
    assert completed_job.exists()
    assert not (organized_root / "standard").exists()
    assert load_organized_ref(completed_job) is None


def test_cmd_reindex_indexes_jobs_from_allowed_and_organized_artifacts(
    tmp_path: Path,
    capsys,
) -> None:
    config_path, allowed_root, organized_root = _write_config(tmp_path)
    first_job = allowed_root / "job-alpha"
    second_job = allowed_root / "job-beta"
    skipped_job = allowed_root / "missing-id"

    _write_job_artifacts(first_job, job_id="job-alpha", status="completed", selected_name="alpha.xyz")
    _write_job_artifacts(
        second_job,
        job_id="job-beta",
        status="failed",
        selected_name="beta.xyz",
        molecule_key="beta",
    )
    skipped_job.mkdir(parents=True, exist_ok=True)
    write_state(skipped_job, {"status": "completed"})

    rc = cmd_reindex(Namespace(config=str(config_path), root=None))

    captured = capsys.readouterr().out
    assert rc == 0
    assert "scan_roots: 1" in captured
    assert "candidate_dirs: 3" in captured
    assert "indexed: 2" in captured
    assert "skipped: 1" in captured

    alpha_record = get_job_location(allowed_root, "job-alpha")
    beta_record = get_job_location(allowed_root, "job-beta")
    assert alpha_record is not None
    assert alpha_record.latest_known_path == str(first_job.resolve())
    assert beta_record is not None
    assert beta_record.status == "failed"
    assert beta_record.original_run_dir == str(second_job.resolve())
    assert beta_record.organized_output_dir == ""
    assert beta_record.latest_known_path == str(second_job.resolve())


def test_scan_roots_prefers_explicit_root_and_skips_invalid_default_roots(tmp_path: Path) -> None:
    explicit_root = tmp_path / "explicit"
    organized_root = tmp_path / "organized"
    explicit_root.mkdir()
    organized_root.mkdir()
    cfg = Namespace(
        runtime=Namespace(
            allowed_root="",
            organized_root=str(organized_root),
        )
    )

    assert reindex_cmd._scan_roots(cfg, str(explicit_root)) == [explicit_root.resolve()]
    assert reindex_cmd._scan_roots(cfg, None) == [organized_root.resolve()]


def test_cmd_reindex_reports_error_when_no_roots_are_available(
    monkeypatch,
    capsys,
) -> None:
    cfg = Namespace(runtime=Namespace(allowed_root="", organized_root=""))

    monkeypatch.setattr(reindex_cmd, "load_config", lambda path=None: cfg)

    rc = reindex_cmd.cmd_reindex(Namespace(config="ignored", root=None))

    assert rc == 1
    assert capsys.readouterr().out == "error: no reindex roots available\n"


def test_cmd_summary_resolves_job_id_and_original_path_with_json_and_text_output(
    tmp_path: Path,
    capsys,
) -> None:
    config_path, allowed_root, organized_root = _write_config(tmp_path)
    job_dir = allowed_root / "job-900"
    _write_job_artifacts(
        job_dir,
        job_id="job-900",
        status="completed",
        selected_name="ethanol.xyz",
        molecule_key="ethanol",
    )
    cmd_reindex(Namespace(config=str(config_path), root=None))
    capsys.readouterr()

    json_rc = cmd_summary(Namespace(config=str(config_path), target="job-900", json=True))
    json_payload = json.loads(capsys.readouterr().out)
    assert json_rc == 0
    assert json_payload["target"] == "job-900"
    assert json_payload["job_dir"] == str(job_dir.resolve())
    assert json_payload["index_record"]["job_id"] == "job-900"
    assert json_payload["index_record"]["latest_known_path"] == str(job_dir.resolve())
    assert json_payload["state"]["job_dir"] == str(job_dir.resolve())
    assert json_payload["report"]["status"] == "completed"

    text_rc = cmd_summary(Namespace(config=str(config_path), target=str(job_dir), json=False))
    text_output = capsys.readouterr().out
    assert text_rc == 0
    assert f"job_dir: {job_dir.resolve()}" in text_output
    assert "job_id: job-900" in text_output
    assert f"latest_known_path: {job_dir.resolve()}" in text_output
    assert "organized_output_dir:" not in text_output
    assert "status: completed" in text_output
    assert "stdout_log:" in text_output


def test_cmd_summary_requires_non_blank_target(
    tmp_path: Path,
    capsys,
) -> None:
    config_path, _, _ = _write_config(tmp_path)

    rc = cmd_summary(Namespace(config=str(config_path), target="   ", json=False))

    assert rc == 1
    assert capsys.readouterr().out == "error: summary requires a job_id or job directory\n"


def test_cmd_summary_reports_missing_job(
    tmp_path: Path,
    capsys,
) -> None:
    config_path, _, _ = _write_config(tmp_path)

    rc = cmd_summary(Namespace(config=str(config_path), target="job-missing", json=False))

    assert rc == 1
    assert capsys.readouterr().out == "error: job not found: job-missing\n"
