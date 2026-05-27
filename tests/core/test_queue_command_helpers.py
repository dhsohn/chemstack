from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from chemstack.core.commands import queue as queue_cmd


def test_run_queue_worker_command_uses_existing_pid_reporter(
    capsys: pytest.CaptureFixture[str],
) -> None:
    reports: list[int] = []

    def worker_factory(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail("worker should not be constructed when a live pid exists")

    result = queue_cmd.run_queue_worker_command(
        SimpleNamespace(config="chemstack.yaml"),
        load_config_fn=lambda _config: SimpleNamespace(),
        config_path_fn=lambda _args: "chemstack.yaml",
        worker_factory=worker_factory,
        existing_pid_fn=lambda _cfg: 12345,
        existing_pid_report_fn=reports.append,
    )

    assert result == 1
    assert reports == [12345]
    assert capsys.readouterr().out == ""
