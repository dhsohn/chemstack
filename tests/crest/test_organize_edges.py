from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pytest

import chemstack.crest.commands.organize as organize_module
from chemstack.crest.config import load_config
from chemstack.crest.state import write_report_json, write_report_md, write_state


def _write_config(tmp_path: Path) -> tuple[Path, Path, Path]:
    workflow_root = tmp_path / "workflow_root"
    allowed_root = workflow_root / "wf_001" / "internal" / "crest" / "runs"
    organized_root = workflow_root / "wf_001" / "internal" / "crest" / "outputs"
    allowed_root.mkdir(parents=True)
    organized_root.mkdir(parents=True)
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
    selected_name: str = "sample.xyz",
    state_overrides: dict[str, object] | None = None,
    include_report: bool = True,
) -> Path:
    job_dir.mkdir(parents=True, exist_ok=True)
    selected_xyz = job_dir / selected_name
    _write_xyz(selected_xyz)

    state_payload: dict[str, object] = {
        "job_id": job_id,
        "job_dir": str(job_dir.resolve()),
        "status": status,
        "mode": mode,
        "selected_input_xyz": str(selected_xyz.resolve()),
        "resource_request": {"max_cores": 4, "max_memory_gb": 8},
        "resource_actual": {"max_cores": 4, "max_memory_gb": 8},
    }
    if state_overrides:
        state_payload.update(state_overrides)
    write_state(job_dir, state_payload)

    if include_report:
        stdout_log = job_dir / "crest.stdout.log"
        stderr_log = job_dir / "crest.stderr.log"
        stdout_log.write_text("stdout\n", encoding="utf-8")
        stderr_log.write_text("stderr\n", encoding="utf-8")
        report_payload = dict(state_payload)
        report_payload["stdout_log"] = str(stdout_log.resolve())
        report_payload["stderr_log"] = str(stderr_log.resolve())
        write_report_json(job_dir, report_payload)
        write_report_md(
            job_dir,
            job_id=str(state_payload.get("job_id", "")),
            status=str(state_payload.get("status", "")),
            reason=str(state_payload.get("status", "")),
            selected_xyz=selected_xyz.name,
        )

    return selected_xyz


def test_resolve_scope_rejects_mutually_exclusive_job_dir_and_root(tmp_path: Path) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    cfg = load_config(str(config_path))
    job_dir = allowed_root / "job-scope"
    job_dir.mkdir(parents=True)

    with pytest.raises(ValueError, match="mutually exclusive"):
        organize_module._resolve_scope(
            cfg,
            Namespace(job_dir=str(job_dir), root=str(allowed_root)),
        )


def test_resolve_scope_validates_job_dir_root_and_default_scope(tmp_path: Path) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    cfg = load_config(str(config_path))
    scan_root = allowed_root
    job_dir = scan_root / "job-scope"
    scan_root.mkdir(parents=True, exist_ok=True)
    job_dir.mkdir()

    resolved_job_dir, resolved_root = organize_module._resolve_scope(
        cfg,
        Namespace(job_dir=str(job_dir), root=None),
    )
    assert resolved_job_dir == job_dir.resolve()
    assert resolved_root is None

    resolved_job_dir, resolved_root = organize_module._resolve_scope(
        cfg,
        Namespace(job_dir=None, root=str(scan_root)),
    )
    assert resolved_job_dir is None
    assert resolved_root == scan_root.resolve()

    resolved_job_dir, resolved_root = organize_module._resolve_scope(
        cfg,
        Namespace(job_dir=None, root=None),
    )
    assert resolved_job_dir is None
    assert resolved_root is None

    outside_root = tmp_path / "outside"
    outside_root.mkdir()
    with pytest.raises(ValueError, match="Scan root must be under allowed_root"):
        organize_module._resolve_scope(
            cfg,
            Namespace(job_dir=None, root=str(outside_root)),
        )


def test_resolve_scope_and_plan_support_workflow_local_runtime_dirs(tmp_path: Path) -> None:
    config_path, _, organized_root = _write_config(tmp_path)
    cfg = load_config(str(config_path))
    workflow_job_dir = tmp_path / "workflow_root" / "wf_crest_001" / "internal" / "crest" / "runs" / "job-local"
    _write_job_artifacts(
        workflow_job_dir,
        job_id="job-local",
        status="completed",
        selected_name="local.xyz",
        include_report=False,
    )

    job_dir, root = organize_module._resolve_scope(
        cfg,
        Namespace(job_dir=str(workflow_job_dir), root=None),
    )

    assert job_dir == workflow_job_dir.resolve()
    assert root is None

    plan = organize_module._collect_plan_for_dir(cfg, workflow_job_dir.resolve())
    expected_target = (
        tmp_path
        / "workflow_root"
        / "wf_crest_001"
        / "internal"
        / "crest"
        / "outputs"
        / "standard"
        / "local"
        / "job-local"
    )
    assert plan["action"] == "organize"
    assert plan["target_dir"] == str(expected_target)
    assert plan["target_dir"] != str(organized_root / "standard" / "local" / "job-local")


def test_collect_plan_for_dir_returns_skip_reasons_for_edge_cases(tmp_path: Path) -> None:
    config_path, allowed_root, organized_root = _write_config(tmp_path)
    cfg = load_config(str(config_path))

    missing_state_dir = (allowed_root / "missing-state").resolve()
    missing_state_dir.mkdir(parents=True)
    assert organize_module._collect_plan_for_dir(cfg, missing_state_dir) == {
        "action": "skip",
        "job_dir": str(missing_state_dir),
        "reason": "missing_state",
    }

    missing_job_id_dir = (allowed_root / "missing-job-id").resolve()
    _write_job_artifacts(
        missing_job_id_dir,
        job_id="job-missing",
        status="completed",
        include_report=False,
        state_overrides={"job_id": ""},
    )
    assert organize_module._collect_plan_for_dir(cfg, missing_job_id_dir) == {
        "action": "skip",
        "job_dir": str(missing_job_id_dir),
        "reason": "missing_job_id",
    }

    already_organized_dir = (organized_root / "standard" / "water" / "job-300").resolve()
    _write_job_artifacts(
        already_organized_dir,
        job_id="job-300",
        status="completed",
        selected_name="water.xyz",
        include_report=False,
    )
    assert organize_module._collect_plan_for_dir(cfg, already_organized_dir) == {
        "action": "skip",
        "job_dir": str(already_organized_dir),
        "reason": "already_under_organized_root",
    }

    target_exists_dir = (allowed_root / "job-target-source").resolve()
    _write_job_artifacts(
        target_exists_dir,
        job_id="job-400",
        status="completed",
        selected_name="ethane.xyz",
        include_report=False,
    )
    existing_target = (organized_root / "standard" / "ethane" / "job-400").resolve()
    existing_target.mkdir(parents=True)
    assert organize_module._collect_plan_for_dir(cfg, target_exists_dir) == {
        "action": "skip",
        "job_dir": str(target_exists_dir),
        "job_id": "job-400",
        "reason": "target_exists",
        "target_dir": str(existing_target),
    }


def test_organize_job_dir_returns_skip_plan_without_moving_files(tmp_path: Path) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    cfg = load_config(str(config_path))
    job_dir = allowed_root / "job-empty"
    job_dir.mkdir(parents=True)

    result = organize_module.organize_job_dir(cfg, job_dir)

    assert result == {
        "action": "skip",
        "job_dir": str(job_dir.resolve()),
        "reason": "missing_state",
    }
    assert list(job_dir.iterdir()) == []


def test_organize_job_dir_notifies_summary_when_requested(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, allowed_root, organized_root = _write_config(tmp_path)
    cfg = load_config(str(config_path))
    job_dir = allowed_root / "job-notify"
    _write_job_artifacts(
        job_dir,
        job_id="job-500",
        status="completed",
        selected_name="notify.xyz",
    )

    notifications: list[dict[str, object]] = []

    def fake_notify(cfg_arg, *, organized_count: int, skipped_count: int, root: Path) -> bool:
        notifications.append(
            {
                "cfg": cfg_arg,
                "organized_count": organized_count,
                "skipped_count": skipped_count,
                "root": root,
            }
        )
        return True

    monkeypatch.setattr(organize_module, "notify_organize_summary", fake_notify)

    result = organize_module.organize_job_dir(cfg, job_dir, notify_summary=True)

    assert result["action"] == "organized"
    assert result["target_dir"] == str((organized_root / "standard" / "notify" / "job-500").resolve())
    assert notifications == [
        {
            "cfg": cfg,
            "organized_count": 1,
            "skipped_count": 0,
            "root": job_dir,
        }
    ]


def test_cmd_organize_dry_run_accepts_job_dir_scope(tmp_path: Path, capsys) -> None:
    config_path, allowed_root, organized_root = _write_config(tmp_path)
    job_dir = allowed_root / "job-single"
    _write_job_artifacts(
        job_dir,
        job_id="job-600",
        status="completed",
        selected_name="single.xyz",
    )

    rc = organize_module.cmd_organize(
        Namespace(
            config=str(config_path),
            job_dir=str(job_dir),
            root=None,
            apply=False,
        )
    )

    captured = capsys.readouterr().out
    planned_target = organized_root / "standard" / "single" / "job-600"
    assert rc == 0
    assert "action: dry_run" in captured
    assert "to_organize: 1" in captured
    assert "skipped: 0" in captured
    assert f"job-600: {job_dir.resolve()} -> {planned_target.resolve()}" in captured


def test_cmd_organize_raises_when_scope_does_not_resolve_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, _, _ = _write_config(tmp_path)

    monkeypatch.setattr(organize_module, "_resolve_scope", lambda cfg, args: (None, None))

    rc = organize_module.cmd_organize(
        Namespace(
            config=str(config_path),
            job_dir=None,
            root=None,
            apply=False,
        )
    )

    assert rc == 0


def test_cmd_organize_apply_mode_reports_failures_and_notifies_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    config_path, allowed_root, organized_root = _write_config(tmp_path)
    success_job_dir = allowed_root / "job-success"
    failure_job_dir = allowed_root / "job-failure"
    _write_job_artifacts(
        success_job_dir,
        job_id="job-700",
        status="completed",
        selected_name="water.xyz",
    )
    _write_job_artifacts(
        failure_job_dir,
        job_id="job-800",
        status="completed",
        selected_name="methane.xyz",
    )

    notifications: list[dict[str, object]] = []
    original_organize_job_dir = organize_module.organize_job_dir

    def fake_notify(cfg_arg, *, organized_count: int, skipped_count: int, root: Path) -> bool:
        notifications.append(
            {
                "cfg": cfg_arg,
                "organized_count": organized_count,
                "skipped_count": skipped_count,
                "root": root,
            }
        )
        return True

    def flaky_organize_job_dir(cfg_arg, job_dir: Path, *, notify_summary: bool = False) -> dict[str, str]:
        if job_dir.resolve() == failure_job_dir.resolve():
            raise RuntimeError("forced failure")
        return original_organize_job_dir(cfg_arg, job_dir, notify_summary=notify_summary)

    monkeypatch.setattr(organize_module, "notify_organize_summary", fake_notify)
    monkeypatch.setattr(organize_module, "organize_job_dir", flaky_organize_job_dir)

    rc = organize_module.cmd_organize(
        Namespace(
            config=str(config_path),
            job_dir=None,
            root=str(allowed_root),
            apply=True,
        )
    )

    captured = capsys.readouterr().out
    success_target = (organized_root / "standard" / "water" / "job-700").resolve()
    assert rc == 1
    assert "action: apply" in captured
    assert "organized: 1" in captured
    assert "skipped: 0" in captured
    assert "failed: 1" in captured
    assert f"job-700: {success_target}" in captured
    assert "failed: job-800 (forced failure)" in captured
    assert success_target.exists()
    assert failure_job_dir.exists()
    assert notifications == [
        {
            "cfg": load_config(str(config_path)),
            "organized_count": 1,
            "skipped_count": 1,
            "root": allowed_root.resolve(),
        }
    ]
