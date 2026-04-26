from __future__ import annotations

from argparse import Namespace
from typing import Any

import pytest

from chemstack.orca import _internal_cli as cli


def test_build_parser_supports_orca_internal_queue_commands() -> None:
    parser = cli.build_parser()

    worker_args = parser.parse_args(
        ["--config", "/tmp/chemstack.yaml", "queue", "worker", "--no-auto-organize"]
    )
    cancel_args = parser.parse_args(
        ["--config", "/tmp/chemstack.yaml", "queue", "cancel", "q-123"]
    )

    assert worker_args.command == "queue"
    assert worker_args.queue_command == "worker"
    assert worker_args.auto_organize is False
    assert worker_args.no_auto_organize is True
    assert cancel_args.queue_command == "cancel"
    assert cancel_args.target == "q-123"

    with pytest.raises(SystemExit):
        parser.parse_args(["queue", "worker", "--auto-organize", "--no-auto-organize"])


def test_main_dispatches_orca_internal_queue_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    worker_calls: list[Any] = []
    cancel_calls: list[Any] = []

    def _worker(args: Any) -> int:
        worker_calls.append(args)
        return 41

    def _cancel(args: Any) -> int:
        cancel_calls.append(args)
        return 42

    monkeypatch.setattr(cli, "cmd_queue_worker", _worker)
    monkeypatch.setattr(cli, "cmd_queue_cancel", _cancel)

    assert cli.main(["queue", "worker", "--auto-organize"]) == 41
    assert cli.main(["queue", "cancel", "job-123"]) == 42

    assert len(worker_calls) == 1
    assert worker_calls[0].queue_command == "worker"
    assert worker_calls[0].auto_organize is True
    assert len(cancel_calls) == 1
    assert cancel_calls[0].queue_command == "cancel"
    assert cancel_calls[0].target == "job-123"


def test_orca_internal_queue_helpers_delegate_to_queue_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli.queue_cmd, "cmd_queue_worker", lambda args: 43)
    monkeypatch.setattr(cli.queue_cmd, "cmd_queue_cancel", lambda args: 44)

    assert cli.cmd_queue_worker(Namespace(queue_command="worker")) == 43
    assert cli.cmd_queue_cancel(Namespace(queue_command="cancel")) == 44


def test_orca_internal_queue_rejects_unknown_subcommand() -> None:
    with pytest.raises(ValueError, match="Unsupported queue subcommand: noop"):
        cli._cmd_queue(Namespace(queue_command="noop"))
