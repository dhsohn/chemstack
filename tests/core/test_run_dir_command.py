from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from chemstack.core.commands import run_dir
from chemstack.core.queue import DuplicateQueueEntryError


def _cfg(allowed_root: Path, *, workflow_root: Path | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        workflow_root=str(workflow_root or ""),
        runtime=SimpleNamespace(allowed_root=str(allowed_root)),
    )


def test_load_yaml_job_manifest_handles_missing_invalid_and_mapping(tmp_path: Path) -> None:
    assert run_dir.load_yaml_job_manifest(
        tmp_path,
        "manifest.yaml",
        invalid_message="invalid {path}",
    ) == {}

    with pytest.raises(ValueError, match="missing"):
        run_dir.load_yaml_job_manifest(
            tmp_path,
            "manifest.yaml",
            missing_message="missing {path}",
            invalid_message="invalid {path}",
        )

    manifest = tmp_path / "manifest.yaml"
    manifest.write_text("- not-a-mapping\n", encoding="utf-8")
    with pytest.raises(ValueError, match="invalid"):
        run_dir.load_yaml_job_manifest(tmp_path, "manifest.yaml", invalid_message="invalid {path}")

    manifest.write_text("job_id: job-1\npriority: 4\n", encoding="utf-8")
    assert run_dir.load_yaml_job_manifest(
        tmp_path,
        "manifest.yaml",
        invalid_message="invalid {path}",
    ) == {"job_id": "job-1", "priority": 4}


def test_resolve_engine_job_dir_uses_workflow_internal_allowed_root(tmp_path: Path) -> None:
    workflow_root = tmp_path / "workflows"
    job_dir = workflow_root / "run-1" / "02_xtb" / "job-1"
    job_dir.mkdir(parents=True)
    seen: list[tuple[str, str, str]] = []

    def validate_job_dir(raw: str, allowed_root: str, *, label: str) -> Path:
        seen.append((raw, allowed_root, label))
        return Path(raw).resolve()

    resolved = run_dir.resolve_engine_job_dir(
        _cfg(tmp_path / "ignored", workflow_root=workflow_root),
        str(job_dir),
        engine="xtb",
        workflow_error_message="not in workflow",
        validate_job_dir_fn=validate_job_dir,
    )

    assert resolved == job_dir.resolve()
    assert seen == [(str(job_dir), str((workflow_root / "run-1" / "02_xtb").resolve()), "Job directory")]


def test_resolve_engine_job_dir_rejects_path_outside_workflow_root(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not in workflow"):
        run_dir.resolve_engine_job_dir(
            _cfg(tmp_path / "allowed", workflow_root=tmp_path / "workflow"),
            str(tmp_path / "outside" / "job"),
            engine="xtb",
            workflow_error_message="not in workflow",
            validate_job_dir_fn=lambda *_args, **_kwargs: pytest.fail("should not validate"),
        )


def test_cmd_engine_run_dir_enqueues_records_and_prints(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    cfg = _cfg(tmp_path / "allowed")
    job_dir = tmp_path / "allowed" / "job-1"
    queue_root = tmp_path / "queue"
    recorded: list[tuple[Any, run_dir.EngineRunDirSubmission, Any]] = []

    submission = run_dir.EngineRunDirSubmission(
        queue_root=queue_root,
        app_name="chemstack.xtb",
        task_id="job-1",
        task_kind="run_dir",
        engine="xtb",
        priority=3,
        metadata={"job_dir": str(job_dir)},
        context={"source": "test"},
    )

    def enqueue(root: Path, **kwargs: Any) -> SimpleNamespace:
        assert root == queue_root
        assert kwargs == {
            "app_name": "chemstack.xtb",
            "task_id": "job-1",
            "task_kind": "run_dir",
            "engine": "xtb",
            "priority": 3,
            "metadata": {"job_dir": str(job_dir)},
        }
        return SimpleNamespace(queue_id="q-1", priority=3)

    exit_code = run_dir.cmd_engine_run_dir(
        SimpleNamespace(config=None, path=str(job_dir)),
        load_config_fn=lambda _config: cfg,
        resolve_job_dir_fn=lambda cfg_obj, raw: Path(raw),
        load_manifest_fn=lambda path: {"job_id": path.name},
        build_submission_fn=lambda _cfg, _job_dir, _manifest, _args: submission,
        record_queued_fn=lambda *items: recorded.append(items),
        print_queued_fn=lambda item, entry: run_dir.print_queued_common(item, entry, job_dir=job_dir),
        enqueue_fn=enqueue,
    )

    assert exit_code == 0
    assert recorded == [(cfg, submission, SimpleNamespace(queue_id="q-1", priority=3))]
    assert capsys.readouterr().out.splitlines() == [
        "status: queued",
        f"job_dir: {job_dir}",
        "job_id: job-1",
        "queue_id: q-1",
        "priority: 3",
    ]


def test_cmd_engine_run_dir_reports_duplicate_queue_entry(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    submission = run_dir.EngineRunDirSubmission(
        queue_root=tmp_path,
        app_name="app",
        task_id="job-1",
        task_kind="run_dir",
        engine="xtb",
        priority=10,
        metadata={},
        context={},
    )

    def enqueue(_root: Path, **_kwargs: Any) -> None:
        raise DuplicateQueueEntryError("already queued")

    exit_code = run_dir.cmd_engine_run_dir(
        SimpleNamespace(config=None, path=str(tmp_path)),
        load_config_fn=lambda _config: SimpleNamespace(),
        resolve_job_dir_fn=lambda _cfg, raw: Path(raw),
        load_manifest_fn=lambda _path: {},
        build_submission_fn=lambda *_args: submission,
        record_queued_fn=lambda *_args: pytest.fail("should not record duplicate"),
        print_queued_fn=lambda *_args: pytest.fail("should not print queued duplicate"),
        enqueue_fn=enqueue,
    )

    assert exit_code == 1
    assert capsys.readouterr().out == "error: already queued\n"
