from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from chemstack.xtb import queue_admission


def _cfg() -> SimpleNamespace:
    return SimpleNamespace(
        runtime=SimpleNamespace(
            admission_root="/tmp/admission",
            admission_limit=2,
            max_concurrent=5,
        )
    )


def test_reserve_admission_slot_uses_xtb_worker_identity() -> None:
    calls: list[tuple[str, int, str, str]] = []

    def reserve_slot(root: str, limit: int, *, source: str, app_name: str) -> str:
        calls.append((root, limit, source, app_name))
        return "slot-1"

    assert queue_admission.reserve_admission_slot(_cfg(), reserve_slot_fn=reserve_slot) == "slot-1"
    assert calls == [("/tmp/admission", 2, "chemstack.xtb.queue_worker", "chemstack_xtb")]


def test_start_background_job_process_builds_xtb_child_command(tmp_path: Path) -> None:
    entry = SimpleNamespace(queue_id="queue-1")
    process = object()
    commands: list[list[str]] = []

    def build_command(**kwargs: Any) -> list[str]:
        assert kwargs == {
            "config_path": "/tmp/chemstack.yaml",
            "queue_root": tmp_path / "queue",
            "queue_id": "queue-1",
            "admission_token": "slot-1",
        }
        return ["python", "-m", "chemstack.xtb.worker_execution"]

    def start_process(command: list[str]) -> object:
        commands.append(command)
        return process

    assert (
        queue_admission.start_background_job_process(
            config_path="/tmp/chemstack.yaml",
            queue_root=tmp_path / "queue",
            entry=entry,
            admission_root="/tmp/admission",
            admission_token="slot-1",
            start_background_process_fn=start_process,
            build_worker_child_command_fn=build_command,
        )
        is process
    )
    assert commands == [["python", "-m", "chemstack.xtb.worker_execution"]]


def test_attach_started_process_records_owner_and_marks_missing_slot(tmp_path: Path) -> None:
    entry = SimpleNamespace(queue_id="queue-1", metadata={"job_dir": str(tmp_path / "job")})
    terminated: list[Any] = []
    failed: list[dict[str, Any]] = []
    process = SimpleNamespace(pid=1234)

    assert queue_admission.attach_started_process(
        admission_root="/tmp/admission",
        queue_root=tmp_path / "queue",
        entry=entry,
        process=process,
        admission_token="slot-1",
        activate_reserved_slot_fn=lambda *args, **kwargs: object(),
        terminate_process_fn=lambda proc: terminated.append(proc),
        mark_entry_failed_and_release_fn=lambda *args, **kwargs: failed.append(
            {"args": args, "kwargs": kwargs}
        ),
        mark_failed_fn=lambda *_args, **_kwargs: None,
    )
    assert terminated == []
    assert failed == []

    assert not queue_admission.attach_started_process(
        admission_root="/tmp/admission",
        queue_root=tmp_path / "queue",
        entry=entry,
        process=process,
        admission_token="slot-2",
        activate_reserved_slot_fn=lambda *args, **kwargs: None,
        terminate_process_fn=lambda proc: terminated.append(proc),
        mark_entry_failed_and_release_fn=lambda *args, **kwargs: failed.append(
            {"args": args, "kwargs": kwargs}
        ),
        mark_failed_fn=lambda *_args, **_kwargs: None,
    )
    assert terminated == [process]
    assert failed[0]["kwargs"]["error"] == "admission_slot_missing"


def test_finalize_worker_start_error_releases_slot_and_writes_failure(
    tmp_path: Path,
) -> None:
    cfg = object()
    entry = SimpleNamespace(queue_id="queue-1")
    job_dir = tmp_path / "job"
    selected_xyz = job_dir / "input.xyz"
    released: list[str] = []
    built: list[dict[str, Any]] = []
    finalized: list[dict[str, Any]] = []

    def build_terminal_result(entry_obj: Any, **kwargs: Any) -> SimpleNamespace:
        built.append({"entry": entry_obj, **kwargs})
        return SimpleNamespace(status=kwargs["status"], reason=kwargs["reason"])

    def finalize_execution_result(cfg_obj: Any, **kwargs: Any) -> None:
        finalized.append({"cfg": cfg_obj, **kwargs})

    queue_admission.finalize_worker_start_error(
        cfg,
        queue_root=tmp_path / "queue",
        entry=entry,
        admission_token="slot-1",
        exc=OSError("boom"),
        release_admission_slot_fn=lambda token: released.append(token),
        build_terminal_result_fn=build_terminal_result,
        finalize_execution_result_fn=finalize_execution_result,
        job_dir_fn=lambda _entry: job_dir,
        selected_xyz_fn=lambda _entry: selected_xyz,
        job_type_fn=lambda _entry: "path_search",
        reaction_key_fn=lambda _entry, _job_dir: "rxn-1",
        input_summary_fn=lambda _entry: {"candidate_count": 1},
        entry_resource_request_fn=lambda _cfg, _entry: {"max_cores": 4, "max_memory_gb": 8},
    )

    assert released == ["slot-1"]
    assert built[0]["reason"] == "worker_start_error:boom"
    assert built[0]["job_dir"] == job_dir
    assert built[0]["selected_xyz"] == selected_xyz
    assert finalized[0]["cfg"] is cfg
    assert finalized[0]["queue_root"] == tmp_path / "queue"
    assert finalized[0]["result"].status == "failed"
    assert finalized[0]["emit_output"] is True
