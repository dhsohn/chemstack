from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from chemstack.core.commands import run_dir


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


def test_build_engine_run_dir_submission_from_spec_adds_common_payloads(
    tmp_path: Path,
) -> None:
    job_dir = tmp_path / "job-1"
    metadata = {"job_dir": str(job_dir), "mode": "nci"}
    context = {"job_dir": job_dir, "mode": "nci"}

    submission = run_dir.build_engine_run_dir_submission_from_spec(
        spec=run_dir.EngineSubmissionSpec(
            queue_root=tmp_path / "queue",
            app_name="chemstack_crest",
            task_id="job-1",
            task_kind="crest_conformer_search",
            engine="crest",
            metadata=metadata,
            context=context,
        ),
        args=SimpleNamespace(priority="6"),
        manifest={"mode": "nci"},
        resource_request={"max_cores": 4, "max_memory_gb": 16},
    )

    assert submission.queue_root == tmp_path / "queue"
    assert submission.app_name == "chemstack_crest"
    assert submission.task_id == "job-1"
    assert submission.task_kind == "crest_conformer_search"
    assert submission.engine == "crest"
    assert submission.priority == 6
    assert submission.metadata == {
        "job_dir": str(job_dir),
        "mode": "nci",
        "manifest_present": "true",
        "resource_request": {"max_cores": 4, "max_memory_gb": 16},
        "resource_actual": {"max_cores": 4, "max_memory_gb": 16},
    }
    assert submission.context == {
        "job_dir": job_dir,
        "mode": "nci",
        "resource_request": {"max_cores": 4, "max_memory_gb": 16},
    }
    assert metadata == {"job_dir": str(job_dir), "mode": "nci"}
    assert context == {"job_dir": job_dir, "mode": "nci"}


def test_build_engine_queued_record_applies_shared_resource_fields(
    tmp_path: Path,
) -> None:
    resource_request = {"max_cores": 4, "max_memory_gb": 16}
    submission = run_dir.EngineRunDirSubmission(
        queue_root=tmp_path,
        app_name="app",
        task_id="job-1",
        task_kind="run_dir",
        engine="crest",
        priority=5,
        metadata={},
        context={"job_dir": tmp_path / "job-1", "resource_request": resource_request},
    )
    state_payload = {"status": "queued", "resource_request": resource_request}
    index_fields = {"mode": "nci", "selected_input_xyz": "input.xyz"}
    notification_fields = {"mode": "nci", "selected_xyz": "input.xyz"}

    record = run_dir.build_engine_queued_record(
        submission=submission,
        state_payload=state_payload,
        index_fields=index_fields,
        notification_fields=notification_fields,
    )

    assert record.state_payload == state_payload
    assert record.state_payload is not state_payload
    assert record.index_fields == {
        "mode": "nci",
        "selected_input_xyz": "input.xyz",
        "resource_request": resource_request,
        "resource_actual": resource_request,
    }
    assert record.notification_fields == notification_fields
    assert index_fields == {"mode": "nci", "selected_input_xyz": "input.xyz"}
    assert notification_fields == {"mode": "nci", "selected_xyz": "input.xyz"}


def test_record_queued_common_applies_shared_fields(tmp_path: Path) -> None:
    cfg = SimpleNamespace(name="config")
    job_dir = tmp_path / "job-1"
    submission = run_dir.EngineRunDirSubmission(
        queue_root=tmp_path,
        app_name="app",
        task_id="job-1",
        task_kind="run_dir",
        engine="xtb",
        priority=5,
        metadata={},
        context={"job_dir": job_dir},
    )
    entry = SimpleNamespace(queue_id="q-1")
    calls: dict[str, Any] = {}

    def build_record(
        submission_arg: run_dir.EngineRunDirSubmission,
        entry_arg: Any,
    ) -> run_dir.EngineQueuedRecord:
        calls["build"] = (submission_arg, entry_arg)
        return run_dir.EngineQueuedRecord(
            state_payload={"status": "queued"},
            index_fields={"selected_input_xyz": "input.xyz"},
            notification_fields={"selected_xyz": "input.xyz"},
        )

    def write_state(path: Path, payload: dict[str, Any]) -> None:
        calls["state"] = (path, payload)

    def upsert_job_record(cfg_arg: Any, **kwargs: Any) -> None:
        calls["index"] = (cfg_arg, kwargs)

    def notify_job_queued(cfg_arg: Any, **kwargs: Any) -> None:
        calls["notify"] = (cfg_arg, kwargs)

    run_dir.record_queued_common(
        cfg,
        submission,
        entry,
        build_record_fn=build_record,
        write_state_fn=write_state,
        upsert_job_record_fn=upsert_job_record,
        notify_job_queued_fn=notify_job_queued,
    )

    assert calls["build"] == (submission, entry)
    assert calls["state"] == (job_dir, {"status": "queued"})
    assert calls["index"] == (
        cfg,
        {
            "job_id": "job-1",
            "status": "queued",
            "job_dir": job_dir,
            "selected_input_xyz": "input.xyz",
        },
    )
    assert calls["notify"] == (
        cfg,
        {
            "job_id": "job-1",
            "queue_id": "q-1",
            "job_dir": job_dir,
            "selected_xyz": "input.xyz",
        },
    )


def test_engine_run_dir_queued_recorder_from_callbacks_applies_shared_fields(
    tmp_path: Path,
) -> None:
    cfg = SimpleNamespace(name="config")
    job_dir = tmp_path / "job-1"
    submission = run_dir.EngineRunDirSubmission(
        queue_root=tmp_path,
        app_name="app",
        task_id="job-1",
        task_kind="run_dir",
        engine="crest",
        priority=5,
        metadata={},
        context={"job_dir": job_dir},
    )
    entry = SimpleNamespace(queue_id="q-1")
    calls: dict[str, Any] = {}

    def build_record(
        submission_arg: run_dir.EngineRunDirSubmission,
        entry_arg: Any,
    ) -> run_dir.EngineQueuedRecord:
        calls["build"] = (submission_arg, entry_arg)
        return run_dir.EngineQueuedRecord(
            state_payload={"status": "queued"},
            index_fields={"mode": "nci"},
            notification_fields={"mode": "nci"},
        )

    recorder = run_dir.engine_run_dir_queued_recorder_from_callbacks(
        run_dir.EngineQueuedRecordCallbacks(
            build_record=build_record,
            write_state=lambda path, payload: calls.setdefault("state", (path, payload)),
            upsert_job_record=lambda cfg_arg, **kwargs: calls.setdefault(
                "index",
                (cfg_arg, kwargs),
            ),
            notify_job_queued=lambda cfg_arg, **kwargs: calls.setdefault(
                "notify",
                (cfg_arg, kwargs),
            ),
        ),
        module_name="chemstack.demo.submission",
    )

    recorder(cfg, submission, entry)

    assert recorder.__name__ == "_record_queued"
    assert recorder.__module__ == "chemstack.demo.submission"
    assert calls["build"] == (submission, entry)
    assert calls["state"] == (job_dir, {"status": "queued"})
    assert calls["index"] == (
        cfg,
        {"job_id": "job-1", "status": "queued", "job_dir": job_dir, "mode": "nci"},
    )
    assert calls["notify"] == (
        cfg,
        {"job_id": "job-1", "queue_id": "q-1", "job_dir": job_dir, "mode": "nci"},
    )


def test_engine_queued_record_callbacks_from_namespace_maps_legacy_symbols() -> None:
    namespace = {
        "_queued_record": lambda *_args: "record",
        "write_state": lambda *_args: "state",
        "upsert_job_record": lambda *_args, **_kwargs: "index",
        "notify_job_queued": lambda *_args, **_kwargs: "notify",
    }

    callbacks = run_dir.engine_queued_record_callbacks_from_namespace(namespace)

    assert callbacks.build_record is namespace["_queued_record"]
    assert callbacks.write_state is namespace["write_state"]
    assert callbacks.upsert_job_record is namespace["upsert_job_record"]
    assert callbacks.notify_job_queued is namespace["notify_job_queued"]


def test_engine_run_dir_queued_recorder_preserves_namespace_late_lookup(
    tmp_path: Path,
) -> None:
    cfg = SimpleNamespace(name="config")
    job_dir = tmp_path / "job-1"
    submission = run_dir.EngineRunDirSubmission(
        queue_root=tmp_path,
        app_name="app",
        task_id="job-1",
        task_kind="run_dir",
        engine="xtb",
        priority=5,
        metadata={},
        context={"job_dir": job_dir},
    )
    entry = SimpleNamespace(queue_id="q-1")
    calls: dict[str, Any] = {}
    namespace: dict[str, Any] = {
        "__name__": "chemstack.demo.legacy_submission",
        "_queued_record": lambda *_args: pytest.fail("old callback should not be used"),
        "write_state": lambda path, payload: calls.setdefault("state", (path, payload)),
        "upsert_job_record": lambda cfg_arg, **kwargs: calls.setdefault(
            "index",
            (cfg_arg, kwargs),
        ),
        "notify_job_queued": lambda cfg_arg, **kwargs: calls.setdefault(
            "notify",
            (cfg_arg, kwargs),
        ),
    }

    recorder = run_dir.engine_run_dir_queued_recorder(namespace)

    def replacement_record(
        submission_arg: run_dir.EngineRunDirSubmission,
        entry_arg: Any,
    ) -> run_dir.EngineQueuedRecord:
        calls["build"] = (submission_arg, entry_arg)
        return run_dir.EngineQueuedRecord(
            state_payload={"status": "queued"},
            index_fields={"job_type": "path"},
            notification_fields={"job_type": "path"},
        )

    namespace["_queued_record"] = replacement_record
    recorder(cfg, submission, entry)

    assert recorder.__module__ == "chemstack.demo.legacy_submission"
    assert calls["build"] == (submission, entry)
    assert calls["state"] == (job_dir, {"status": "queued"})
    assert calls["index"] == (
        cfg,
        {"job_id": "job-1", "status": "queued", "job_dir": job_dir, "job_type": "path"},
    )
    assert calls["notify"] == (
        cfg,
        {"job_id": "job-1", "queue_id": "q-1", "job_dir": job_dir, "job_type": "path"},
    )
