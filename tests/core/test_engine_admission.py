from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from chemstack.core.queue import engine_admission


def test_start_engine_child_process_can_include_or_omit_admission_root(tmp_path: Path) -> None:
    entry = SimpleNamespace(queue_id="queue-1")
    commands: list[dict[str, Any]] = []

    def build_command(**kwargs: Any) -> list[str]:
        commands.append(kwargs)
        return ["python", "-m", "worker"]

    assert engine_admission.start_engine_child_process(
        config_path="/tmp/chemstack.yaml",
        queue_root=tmp_path / "queue",
        entry=entry,
        admission_root="/tmp/admission",
        admission_token="slot-1",
        start_background_process_fn=lambda command: command,
        build_worker_child_command_fn=build_command,
        include_admission_root=True,
    ) == ["python", "-m", "worker"]

    assert engine_admission.start_engine_child_process(
        config_path="/tmp/chemstack.yaml",
        queue_root=tmp_path / "queue",
        entry=entry,
        admission_root="/tmp/admission",
        admission_token="slot-2",
        start_background_process_fn=lambda command: command,
        build_worker_child_command_fn=build_command,
        include_admission_root=False,
    ) == ["python", "-m", "worker"]

    assert commands == [
        {
            "config_path": "/tmp/chemstack.yaml",
            "queue_root": tmp_path / "queue",
            "queue_id": "queue-1",
            "admission_token": "slot-1",
            "admission_root": "/tmp/admission",
        },
        {
            "config_path": "/tmp/chemstack.yaml",
            "queue_root": tmp_path / "queue",
            "queue_id": "queue-1",
            "admission_token": "slot-2",
        },
    ]


def test_attach_started_process_records_owner_and_marks_missing_slot(tmp_path: Path) -> None:
    entry = SimpleNamespace(queue_id="queue-1", metadata={"job_dir": str(tmp_path / "job")})
    process = SimpleNamespace(pid=321)
    activated: list[dict[str, Any]] = []

    def activate_reserved_slot(root: str, token: str, **kwargs: Any) -> object:
        activated.append({"root": root, "token": token, **kwargs})
        return object()

    assert engine_admission.attach_started_process(
        admission_root="/tmp/admission",
        queue_root=tmp_path / "queue",
        entry=entry,
        process=process,
        admission_token="slot-1",
        activate_reserved_slot_fn=activate_reserved_slot,
        terminate_process_fn=lambda _process: None,
        mark_entry_failed_and_release_fn=lambda *args, **kwargs: None,
        mark_failed_fn=lambda *args, **kwargs: None,
        source="source",
    )

    assert activated == [
        {
            "root": "/tmp/admission",
            "token": "slot-1",
            "owner_pid": 321,
            "source": "source",
            "queue_id": "queue-1",
            "work_dir": str(tmp_path / "job"),
        }
    ]

    terminated: list[Any] = []
    failed: list[dict[str, Any]] = []
    assert not engine_admission.attach_started_process(
        admission_root="/tmp/admission",
        queue_root=tmp_path / "queue",
        entry=entry,
        process=process,
        admission_token="slot-2",
        activate_reserved_slot_fn=lambda *args, **kwargs: None,
        terminate_process_fn=terminated.append,
        mark_entry_failed_and_release_fn=lambda *args, **kwargs: failed.append(
            {"args": args, "kwargs": kwargs}
        ),
        mark_failed_fn=lambda *args, **kwargs: None,
        source="source",
    )

    assert terminated == [process]
    assert failed[0]["args"] == (tmp_path / "queue", entry, "slot-2")
    assert failed[0]["kwargs"]["error"] == "admission_slot_missing"
