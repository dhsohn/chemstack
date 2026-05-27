from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack import cli as chemstack_cli
from chemstack.orca.commands import queue as queue_cmd


def test_queue_engine_worker_dispatches_orca_worker(monkeypatch) -> None:
    worker_calls: list[Any] = []

    def _worker(args: Any) -> int:
        worker_calls.append(args)
        return 41

    monkeypatch.setattr(queue_cmd, "cmd_queue_worker", _worker)

    assert (
        chemstack_cli.main(
            [
                "--config",
                "/tmp/chemstack.yaml",
                "queue",
                "engine-worker",
                "orca",
                "--no-auto-organize",
            ]
        )
        == 41
    )

    assert len(worker_calls) == 1
    assert worker_calls[0].engine == "orca"
    assert worker_calls[0].config == str(Path("/tmp/chemstack.yaml").resolve())
    assert worker_calls[0].auto_organize is False
    assert worker_calls[0].no_auto_organize is True
