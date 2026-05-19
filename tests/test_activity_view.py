from __future__ import annotations

from typing import Any

from chemstack import activity_view


def test_queue_list_default_visible_items_hides_xtb_crest_children_but_keeps_orca_children() -> None:
    items: list[dict[str, Any]] = [
        {"activity_id": "wf_1", "kind": "workflow"},
        {"activity_id": "crest_1", "kind": "job", "engine": "crest", "metadata": {"workflow_id": "wf_1"}},
        {"activity_id": "xtb_1", "kind": "job", "engine": "xtb", "metadata": {"workflow_id": "wf_1"}},
        {"activity_id": "orca_1", "kind": "job", "engine": "orca", "metadata": {"workflow_id": "wf_1"}},
        {"activity_id": "standalone", "kind": "job", "engine": "xtb", "metadata": {}},
    ]

    visible = activity_view.queue_list_default_visible_items(items)

    assert [item["activity_id"] for item in visible] == ["wf_1", "orca_1", "standalone"]
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
        {"activity_id": "standalone", "kind": "job", "engine": "crest", "metadata": {}},
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
        (0, "standalone"),
    ]


def test_activity_with_parent_hint_extracts_workflow_id_from_runtime_path() -> None:
    item = {
        "activity_id": "xtb_child",
        "kind": "job",
        "metadata": {"job_dir": "/tmp/root/workflow_jobs/wf_path/.chemstack/xtb/jobs/job_1"},
    }

    assert activity_view.activity_with_parent_hint(item)["parent_workflow_id"] == "wf_path"
