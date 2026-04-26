from __future__ import annotations

from types import SimpleNamespace

from chemstack.core.app_ids import CHEMSTACK_CONFIG_ENV_VAR
from chemstack.services import bot as bot_service
from chemstack.services import queue_worker as queue_worker_service
from chemstack.services import summary as summary_service


def test_bot_service_main_uses_shared_config(monkeypatch) -> None:
    sentinel = object()
    captured: dict[str, object | None] = {}

    monkeypatch.setenv(CHEMSTACK_CONFIG_ENV_VAR, "/tmp/chemstack.yaml")

    def _fake_settings(config_path=None):
        captured["config_path"] = config_path
        return sentinel

    monkeypatch.setattr(bot_service, "settings_from_config", _fake_settings)

    def _fake_run_bot(settings=None):
        captured["settings"] = settings
        return 7

    monkeypatch.setattr(bot_service, "run_bot", _fake_run_bot)

    result = bot_service.main()

    assert result == 7
    assert captured == {
        "config_path": "/tmp/chemstack.yaml",
        "settings": sentinel,
    }


def test_queue_worker_service_main_uses_default_apps(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setenv(CHEMSTACK_CONFIG_ENV_VAR, "/tmp/chemstack.yaml")

    def _fake_cmd_queue_worker(args: SimpleNamespace) -> int:
        captured["args"] = args
        return 11

    monkeypatch.setattr(queue_worker_service.unified_cli, "cmd_queue_worker", _fake_cmd_queue_worker)

    result = queue_worker_service.main()

    assert result == 11
    args = captured["args"]
    assert isinstance(args, SimpleNamespace)
    assert args.app is None
    assert args.chemstack_config == "/tmp/chemstack.yaml"
    assert args.json is False


def test_summary_service_main_runs_combined_summary(monkeypatch) -> None:
    captured: dict[str, object | None] = {}

    def _fake_main(argv=None):
        captured["argv"] = argv
        return 13

    monkeypatch.setattr(summary_service.unified_cli, "main", _fake_main)

    result = summary_service.main()

    assert result == 13
    assert captured["argv"] == ["summary"]
