from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from chemstack import activity_view


def test_default_visible_items_hide_internal_workflow_children() -> None:
    items: list[dict[str, Any]] = [
        {"activity_id": "wf_1", "kind": "workflow"},
        {
            "activity_id": "crest_1",
            "kind": "job",
            "engine": "crest",
            "metadata": {"workflow_id": "wf_1"},
        },
        {
            "activity_id": "xtb_1",
            "kind": "job",
            "engine": "xtb",
            "metadata": {"workflow_id": "wf_1"},
        },
        {
            "activity_id": "orca_1",
            "kind": "job",
            "engine": "orca",
            "metadata": {"workflow_id": "wf_1"},
        },
        {"activity_id": "engine_job", "kind": "job", "engine": "xtb", "metadata": {}},
    ]

    visible = activity_view.queue_list_default_visible_items(items)

    assert [item["activity_id"] for item in visible] == ["wf_1", "orca_1", "engine_job"]
    assert visible[1]["parent_workflow_id"] == "wf_1"


def test_queue_list_display_rows_groups_children_under_workflow_once() -> None:
    workflow: dict[str, Any] = {
        "activity_id": "wf_1",
        "kind": "workflow",
        "metadata": {"template_name": "reaction_ts_search"},
    }
    visible_items: list[dict[str, Any]] = [
        {
            "activity_id": "orca_1",
            "kind": "job",
            "engine": "orca",
            "metadata": {"workflow_id": "wf_1"},
        },
        workflow,
        {
            "activity_id": "orca_2",
            "kind": "job",
            "engine": "orca",
            "metadata": {"workflow_id": "wf_1"},
        },
        {"activity_id": "engine_job", "kind": "job", "engine": "crest", "metadata": {}},
    ]

    rows = activity_view.queue_list_display_rows(
        all_items=[workflow],
        visible_items=visible_items,
        show_workflow_context=True,
        visible_workflow_child_engines=["orca"],
    )

    assert [(indent, item["activity_id"]) for indent, item in rows] == [
        (0, "wf_1"),
        (1, "orca_1"),
        (1, "orca_2"),
        (0, "engine_job"),
    ]


def test_activity_with_parent_hint_extracts_workflow_id_from_runtime_path() -> None:
    item = {
        "activity_id": "xtb_child",
        "kind": "job",
        "metadata": {"job_dir": "/tmp/root/wf_path/02_xtb/job_1"},
    }

    assert activity_view.activity_with_parent_hint(item)["parent_workflow_id"] == "wf_path"


def test_count_global_active_simulations_uses_orca_runtime_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str | None]] = []
    admission_root = Path("/tmp/chemstack-admission")

    def fake_sibling_runtime_paths(
        config_path: str, *, engine: str | None = None
    ) -> dict[str, Path]:
        calls.append((config_path, engine))
        return {"admission_root": admission_root}

    monkeypatch.setattr(activity_view, "sibling_runtime_paths", fake_sibling_runtime_paths)
    monkeypatch.setattr(activity_view, "active_slot_count", lambda root: 5)

    assert (
        activity_view.count_global_active_simulations(
            [{"activity_id": "running_1"}], config_path="/tmp/chemstack.yaml"
        )
        == 5
    )
    assert calls == [("/tmp/chemstack.yaml", "orca")]
