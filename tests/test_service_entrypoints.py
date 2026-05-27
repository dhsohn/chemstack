from __future__ import annotations

from argparse import Namespace

from chemstack import cli as chemstack_cli
from chemstack import cli_handlers
from chemstack import cli_workers
from chemstack.core.app_ids import CHEMSTACK_CONFIG_ENV_VAR
from chemstack.flow import cli_workflow
from chemstack.flow import telegram_bot


def test_bot_module_main_uses_shared_config(monkeypatch) -> None:
    sentinel = object()
    captured: dict[str, object | None] = {}

    monkeypatch.setenv(CHEMSTACK_CONFIG_ENV_VAR, "/tmp/chemstack.yaml")

    def _fake_settings(config_path=None):
        captured["config_path"] = config_path
        return sentinel

    monkeypatch.setattr(telegram_bot, "settings_from_config", _fake_settings)

    def _fake_run_bot(settings=None):
        captured["settings"] = settings
        return 7

    monkeypatch.setattr(telegram_bot, "run_bot", _fake_run_bot)

    result = telegram_bot.main()

    assert result == 7
    assert captured == {
        "config_path": "/tmp/chemstack.yaml",
        "settings": sentinel,
    }


def test_queue_worker_direct_cli_uses_default_apps(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_cmd_queue_worker(args: Namespace) -> int:
        captured["args"] = args
        return 11

    monkeypatch.setattr(cli_workers, "cmd_queue_worker", _fake_cmd_queue_worker)

    result = chemstack_cli.main(["--config", "/tmp/chemstack.yaml", "queue", "worker"])

    assert result == 11
    args = captured["args"]
    assert isinstance(args, Namespace)
    assert args.app is None
    assert args.global_config == "/tmp/chemstack.yaml"
    assert args.json is False


def test_workflow_worker_module_main_uses_dedicated_parser(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_cmd_workflow_worker(args: Namespace) -> int:
        captured["args"] = args
        return 17

    monkeypatch.setattr(
        cli_workflow,
        "cmd_workflow_worker",
        _fake_cmd_workflow_worker,
    )

    result = cli_workflow.main(
        [
            "--workflow-root",
            "/tmp/workflows",
            "--chemstack-config",
            "/tmp/chemstack.yaml",
            "--once",
        ]
    )

    assert result == 17
    args = captured["args"]
    assert isinstance(args, Namespace)
    assert args.workflow_root == "/tmp/workflows"
    assert args.chemstack_config == "/tmp/chemstack.yaml"
    assert args.once is True


def test_summary_direct_cli_runs_combined_summary(monkeypatch) -> None:
    captured: dict[str, object | None] = {}

    def _fake_cmd_summary(args: Namespace) -> int:
        captured["args"] = args
        return 13

    monkeypatch.setattr(cli_handlers, "cmd_summary", _fake_cmd_summary)

    result = chemstack_cli.main(["summary"])

    assert result == 13
    args = captured["args"]
    assert isinstance(args, Namespace)
    assert args.no_send is False
