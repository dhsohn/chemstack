from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from chemstack import cli as top_cli


def test_top_level_cli_delegate_with_deps_uses_current_monkeypatched_globals(
    monkeypatch,
) -> None:
    seen: list[tuple[Any, str | None]] = []

    def fake_discover(explicit: Any, *, config_path: str | None = None) -> str:
        seen.append((explicit, config_path))
        return "/tmp/current-workflows"

    monkeypatch.setattr(top_cli, "_discover_workflow_root", fake_discover)

    result = top_cli._workflow_root_for_args(
        SimpleNamespace(workflow_root="/tmp/ignored", chemstack_config=None, config=None)
    )

    assert result == "/tmp/current-workflows"
    assert seen == [("/tmp/ignored", None)]


def test_top_level_run_dir_delegate_uses_current_monkeypatched_command_globals(
    monkeypatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "workflow"
    target.mkdir()
    (target / "flow.yaml").write_text("workflow_type: conformer_screening\n", encoding="utf-8")
    seen: list[str] = []

    def fake_workflow_run_dir(args: Any) -> int:
        seen.append(args.workflow_dir)
        return 77

    monkeypatch.setattr(top_cli, "cmd_workflow_run_dir", fake_workflow_run_dir)
    monkeypatch.setattr(
        top_cli,
        "cmd_orca_run_dir",
        lambda args: (_ for _ in ()).throw(AssertionError("ORCA should not run")),
    )

    result = top_cli.cmd_run_dir(SimpleNamespace(path=str(target)))

    assert result == 77
    assert seen == [str(target)]
