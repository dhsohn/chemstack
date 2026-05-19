from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from chemstack.flow import cli


def test_flow_cli_delegate_with_deps_resolves_current_get_workflow(monkeypatch) -> None:
    seen: list[dict[str, Any]] = []

    def fake_get_workflow(**kwargs: Any) -> dict[str, Any]:
        seen.append(kwargs)
        return {
            "summary": {
                "workflow_id": "wf_dynamic",
                "template_name": "reaction_ts_search",
                "status": "running",
                "source_job_id": "source",
                "reaction_key": "rxn",
                "workspace_dir": "/tmp/wf_dynamic",
                "stage_count": 0,
                "stage_summaries": [],
            }
        }

    monkeypatch.setattr(cli, "get_workflow", fake_get_workflow)

    assert cli.cmd_workflow_get(SimpleNamespace(target="wf_dynamic", workflow_root="/tmp/wf", json=True)) == 0

    assert seen == [
        {
            "target": "wf_dynamic",
            "workflow_root": "/tmp/wf",
            "sync_registry": True,
        }
    ]


def test_flow_cli_run_dir_delegate_uses_current_private_helpers(monkeypatch, tmp_path) -> None:
    target = tmp_path / "input"
    target.mkdir()
    seen: list[str] = []

    monkeypatch.setattr(cli.Path, "resolve", lambda self: self)
    monkeypatch.setattr(cli.Path, "is_dir", lambda self: True)
    monkeypatch.setattr(cli.Path, "is_file", lambda self: False)

    def fake_create(args: Any, workflow_dir: Any) -> dict[str, Any]:
        seen.append(str(workflow_dir))
        return {"workflow_id": "wf_created", "template_name": "conformer", "stages": []}

    monkeypatch.setattr(cli, "_create_run_dir_workflow", fake_create)

    assert cli.cmd_run_dir(SimpleNamespace(workflow_dir=str(target), json=True)) == 0
    assert seen == [str(target)]
