# ruff: noqa: E402

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from chemstack.core.queue.types import QueueEntry, QueueStatus

from chemstack.flow import activity, cli, operations


def test_list_activities_merges_workflows_and_standalone_sources(monkeypatch) -> None:
    workflow_record = SimpleNamespace(
        workflow_id="wf-2",
        template_name="reaction_ts_search",
        status="running",
        source_job_id="",
        source_job_type="",
        reaction_key="rxn-2",
        requested_at="2026-04-20T10:00:00+00:00",
        workspace_dir="/tmp/wf/wf-2",
        workflow_file="/tmp/wf/wf-2/workflow.json",
        stage_count=2,
        updated_at="2026-04-20T10:06:00+00:00",
    )

    monkeypatch.setattr(activity, "list_workflow_registry", lambda workflow_root: [workflow_record])
    monkeypatch.setattr(
        activity,
        "list_workflow_summaries",
        lambda workflow_root: [
            {
                "workflow_id": "wf-2",
                "stage_summaries": [
                    {
                        "stage_id": "crest.reactant",
                        "status": "completed",
                        "task_status": "completed",
                        "engine": "crest",
                        "reaction_dir": "",
                    },
                    {
                        "stage_id": "xtb.path",
                        "status": "running",
                        "task_status": "running",
                        "engine": "xtb",
                        "reaction_dir": "/tmp/xtb_jobs/rxn-2",
                    },
                ],
            }
        ],
    )
    monkeypatch.setattr(
        activity,
        "sibling_runtime_paths",
        lambda config_path: {
            "allowed_root": Path("/tmp/crest_root" if "crest" in config_path else "/tmp/xtb_root"),
        },
    )

    crest_entry = QueueEntry(
        queue_id="crest-q-1",
        app_name="crest_auto",
        task_id="crest-job-1",
        task_kind="crest_run_dir",
        engine="crest",
        status=QueueStatus.PENDING,
        priority=10,
        enqueued_at="2026-04-20T10:01:00+00:00",
        metadata={"job_dir": "/tmp/crest_root/jobs/mol-a"},
    )
    xtb_entry = QueueEntry(
        queue_id="xtb-q-1",
        app_name="xtb_auto",
        task_id="xtb-job-1",
        task_kind="xtb_run_dir",
        engine="xtb",
        status=QueueStatus.RUNNING,
        priority=5,
        enqueued_at="2026-04-20T10:02:00+00:00",
        started_at="2026-04-20T10:03:00+00:00",
        metadata={"job_dir": "/tmp/xtb_root/jobs/rxn-a", "reaction_key": "rxn-a"},
    )
    monkeypatch.setattr(
        activity,
        "list_queue",
        lambda root: [crest_entry] if str(root).endswith("crest_root") else [xtb_entry],
    )
    monkeypatch.setattr(
        activity,
        "_orca_records",
        lambda **kwargs: [
            activity.ActivityRecord(
                activity_id="orca-q-1",
                kind="job",
                engine="orca",
                status="running",
                label="ts-run-1",
                source="chemstack_orca",
                submitted_at="2026-04-20T10:04:00+00:00",
                updated_at="2026-04-20T10:07:00+00:00",
                cancel_target="orca-q-1",
                aliases=("orca-q-1",),
                metadata={},
            )
        ],
    )

    payload = activity.list_activities(
        workflow_root="/tmp/wf",
        crest_auto_config="/tmp/crest.yaml",
        xtb_auto_config="/tmp/xtb.yaml",
        orca_auto_config="/tmp/orca.yaml",
    )

    assert payload["count"] == 4
    assert [item["activity_id"] for item in payload["activities"]] == [
        "orca-q-1",
        "wf-2",
        "xtb-q-1",
        "crest-q-1",
    ]
    workflow_item = next(item for item in payload["activities"] if item["activity_id"] == "wf-2")
    assert workflow_item["engine"] == "workflow"
    assert workflow_item["label"] == "/tmp/xtb_jobs/rxn-2"
    assert workflow_item["metadata"]["current_engine"] == "xtb"


def test_list_activities_treats_submission_failed_stage_as_terminal_for_current_stage(monkeypatch) -> None:
    workflow_record = SimpleNamespace(
        workflow_id="wf-3",
        template_name="reaction_ts_search",
        status="running",
        source_job_id="",
        source_job_type="",
        reaction_key="rxn-3",
        requested_at="2026-04-20T10:00:00+00:00",
        workspace_dir="/tmp/wf/wf-3",
        workflow_file="/tmp/wf/wf-3/workflow.json",
        stage_count=2,
        updated_at="2026-04-20T10:06:00+00:00",
    )

    monkeypatch.setattr(activity, "list_workflow_registry", lambda workflow_root: [workflow_record])
    monkeypatch.setattr(
        activity,
        "list_workflow_summaries",
        lambda workflow_root: [
            {
                "workflow_id": "wf-3",
                "stage_summaries": [
                    {
                        "stage_id": "orca.submit",
                        "status": "submission_failed",
                        "task_status": "submission_failed",
                        "engine": "orca",
                        "reaction_dir": "/tmp/orca_jobs/rxn-3/failed-submit",
                    },
                    {
                        "stage_id": "xtb.path",
                        "status": "running",
                        "task_status": "running",
                        "engine": "xtb",
                        "reaction_dir": "/tmp/xtb_jobs/rxn-3",
                    },
                ],
            }
        ],
    )
    monkeypatch.setattr(
        activity,
        "_resolve_activity_sources",
        lambda **kwargs: ("/tmp/wf", None, None, None),
    )
    monkeypatch.setattr(activity, "list_queue", lambda root: [])
    monkeypatch.setattr(activity, "_orca_records", lambda **kwargs: [])

    payload = activity.list_activities(workflow_root="/tmp/wf")

    workflow_item = payload["activities"][0]
    assert workflow_item["activity_id"] == "wf-3"
    assert workflow_item["label"] == "/tmp/xtb_jobs/rxn-3"
    assert workflow_item["metadata"]["current_engine"] == "xtb"
    assert workflow_item["metadata"]["current_stage_id"] == "xtb.path"
    assert workflow_item["metadata"]["current_stage_status"] == "running"


def test_cancel_activity_routes_workflow_targets(monkeypatch) -> None:
    monkeypatch.setattr(
        activity,
        "_collect_activity_records",
        lambda **kwargs: [
            activity.ActivityRecord(
                activity_id="wf-9",
                kind="workflow",
                engine="xtb",
                status="running",
                label="wf-9",
                source="chem_flow",
                submitted_at="2026-04-20T10:00:00+00:00",
                updated_at="2026-04-20T10:00:00+00:00",
                cancel_target="wf-9",
                aliases=("wf-9", "/tmp/wf/wf-9"),
                metadata={},
            )
        ],
    )
    monkeypatch.setattr(
        operations,
        "cancel_workflow",
        lambda **kwargs: {"workflow_id": "wf-9", "status": "cancelled", "cancelled": []},
    )

    payload = activity.cancel_activity(target="wf-9", workflow_root="/tmp/wf")

    assert payload["activity_id"] == "wf-9"
    assert payload["status"] == "cancelled"
    assert payload["source"] == "chem_flow"


def test_cancel_activity_routes_xtb_targets(monkeypatch) -> None:
    monkeypatch.setattr(
        activity,
        "_collect_activity_records",
        lambda **kwargs: [
            activity.ActivityRecord(
                activity_id="xtb-q-1",
                kind="job",
                engine="xtb",
                status="running",
                label="rxn-a",
                source="xtb_auto",
                submitted_at="2026-04-20T10:00:00+00:00",
                updated_at="2026-04-20T10:01:00+00:00",
                cancel_target="xtb-q-1",
                aliases=("xtb-q-1", "xtb-job-1"),
                metadata={},
            )
        ],
    )
    monkeypatch.setattr(
        activity,
        "cancel_xtb_target",
        lambda **kwargs: {"status": "cancel_requested", "queue_id": kwargs["target"]},
    )

    payload = activity.cancel_activity(target="xtb-job-1", xtb_auto_config="/tmp/xtb.yaml")

    assert payload["activity_id"] == "xtb-q-1"
    assert payload["status"] == "cancel_requested"
    assert payload["source"] == "xtb_auto"


def test_activity_helper_edges_and_discovery_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert activity._coerce_mapping({"a": 1}) == {"a": 1}
    assert activity._coerce_mapping(["not", "mapping"]) == {}
    assert activity._resolve_existing_path("") is None

    existing = tmp_path / "config.yaml"
    existing.write_text("workflow:\n  root: /tmp/wf\n", encoding="utf-8")
    assert activity._resolve_existing_path(str(existing)) == existing.resolve()

    assert activity._discover_workflow_root(tmp_path / "wf") == str((tmp_path / "wf").resolve())
    monkeypatch.setenv(activity.CHEMSTACK_CONFIG_ENV_VAR, str(existing))
    assert activity._discover_sibling_config(None, app_name="xtb_auto") == str(existing.resolve())
    assert activity._discover_sibling_config(str(existing), app_name="crest_auto") == str(existing.resolve())

    monkeypatch.setenv(activity.CHEMSTACK_REPO_ROOT_ENV_VAR, str(tmp_path / "repo"))
    assert activity._discover_orca_repo_root(None) == str((tmp_path / "repo").resolve())
    assert activity._discover_orca_repo_root(str(tmp_path / "explicit")) == str(
        (tmp_path / "explicit").resolve()
    )

    assert activity._shared_config_hint("", None, " /tmp/shared.yaml ") == "/tmp/shared.yaml"
    assert activity._parse_iso("") < activity._parse_iso("2026-04-26T00:00:00Z")
    assert activity._parse_iso("bad") < activity._parse_iso("2026-04-26T00:00:00+09:00")
    assert activity._parse_iso("2026-04-26T00:00:00").tzinfo is not None
    assert activity._unique_texts([" a ", "", "a", "b"]) == ("a", "b")
    assert activity._mapping_text({"key": " value "}, "key") == "value"
    assert activity._path_aliases("", root=tmp_path) == ()


def test_runtime_path_and_engine_root_edges(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowed = tmp_path / "allowed"
    calls: list[tuple[str, str | None]] = []

    def fake_sibling_runtime_paths(config_path: str, *, engine: str | None = None) -> dict[str, Path]:
        calls.append((config_path, engine))
        if engine == "xtb":
            raise TypeError("unexpected keyword argument 'engine'")
        if engine == "bad":
            raise TypeError("other type error")
        return {"allowed_root": allowed}

    monkeypatch.setattr(activity, "sibling_runtime_paths", fake_sibling_runtime_paths)
    assert activity._runtime_paths_for_engine("/tmp/cfg.yaml", engine="xtb") == {"allowed_root": allowed}
    assert calls[-1] == ("/tmp/cfg.yaml", None)

    with pytest.raises(TypeError):
        activity._runtime_paths_for_engine("/tmp/cfg.yaml", engine="bad")

    monkeypatch.setattr(activity, "sibling_runtime_paths", lambda config_path, *, engine: {"allowed_root": allowed})
    assert activity._engine_queue_roots("/tmp/cfg.yaml", engine="orca") == (allowed,)
    monkeypatch.setattr(activity, "shared_workflow_root_from_config", lambda config_path: "")
    assert activity._engine_queue_roots("/tmp/cfg.yaml", engine="xtb") == (allowed,)

    workspace_a = tmp_path / "wf" / "a"
    workspace_b = tmp_path / "wf" / "b"
    runtime_a = tmp_path / "runtime-a"
    runtime_b = tmp_path / "runtime-b"
    monkeypatch.setattr(activity, "shared_workflow_root_from_config", lambda config_path: str(tmp_path / "wf"))
    monkeypatch.setattr(
        activity,
        "iter_workflow_runtime_workspaces",
        lambda workflow_root, *, engine: [workspace_a, workspace_b, workspace_a],
    )
    monkeypatch.setattr(
        activity,
        "workflow_workspace_internal_engine_paths",
        lambda workspace_dir, *, engine: {
            "allowed_root": runtime_a if workspace_dir == workspace_a else runtime_b
        },
    )
    assert activity._engine_queue_roots("/tmp/cfg.yaml", engine="crest") == (runtime_a, runtime_b)


def test_orca_fallback_queue_records_cover_file_edges(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowed = tmp_path / "orca"
    allowed.mkdir()
    monkeypatch.setattr(
        activity,
        "sibling_runtime_paths",
        lambda config_path, *, engine: {"allowed_root": allowed},
    )

    assert activity._fallback_orca_queue_records(config_path="/tmp/cfg.yaml") == []
    (allowed / "queue.json").write_text("{not json", encoding="utf-8")
    assert activity._fallback_orca_queue_records(config_path="/tmp/cfg.yaml") == []
    (allowed / "queue.json").write_text(json.dumps({"not": "a-list"}), encoding="utf-8")
    assert activity._fallback_orca_queue_records(config_path="/tmp/cfg.yaml") == []

    reaction_dir = allowed / "wf" / "rxn-1"
    payload = [
        "skip",
        {
            "queue_id": "q-1",
            "run_id": "run-1",
            "status": "running",
            "cancel_requested": True,
            "task_kind": "orca_run",
            "priority": 5,
            "enqueued_at": "2026-04-26T00:00:00+00:00",
            "metadata": {
                "workflow_id": "wf-1",
                "reaction_dir": str(reaction_dir),
                "job_type": "opt",
                "selected_inp": "rxn.inp",
            },
        },
    ]
    (allowed / "queue.json").write_text(json.dumps(payload), encoding="utf-8")

    rows = activity._fallback_orca_queue_records(config_path="/tmp/cfg.yaml")

    assert len(rows) == 1
    row = rows[0]
    assert row.activity_id == "q-1"
    assert row.status == "cancel_requested"
    assert row.label == "rxn-1"
    assert "wf/rxn-1" in row.aliases
    assert row.metadata["priority"] == 5


def test_orca_records_merge_queue_entries_and_snapshots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowed = tmp_path / "orca"
    allowed.mkdir()
    reaction_dir = allowed / "rxn-1"
    reaction_dir.mkdir()
    orphan_dir = allowed / "orphan"
    orphan_dir.mkdir()
    entries = [
        {
            "queue_id": "q-1",
            "task_id": "task-1",
            "run_id": "run-1",
            "reaction_dir": str(reaction_dir),
            "status": "running",
            "cancel_requested": False,
            "task_kind": "orca_run",
            "priority": 3,
            "enqueued_at": "2026-04-26T00:00:00+00:00",
            "started_at": "2026-04-26T00:01:00+00:00",
            "metadata": {"workflow_id": "wf-1", "job_type": "opt", "selected_inp": "rxn.inp"},
        },
        {
            "queue_id": "q-2",
            "task_id": "task-2",
            "run_id": "",
            "reaction_dir": str(tmp_path / "missing"),
            "status": "running",
            "cancel_requested": True,
            "task_kind": "orca_run",
            "priority": 4,
            "enqueued_at": "2026-04-26T00:02:00+00:00",
            "metadata": {},
        },
    ]
    snapshots = [
        SimpleNamespace(
            key="snap-1",
            run_id="run-1",
            reaction_dir=reaction_dir,
            status="completed",
            name="tracked-name",
            completed_at="2026-04-26T01:00:00+00:00",
            updated_at="",
            started_at="2026-04-26T00:00:00+00:00",
            attempts=2,
            selected_inp_name="rxn.inp",
            job_type="opt",
        ),
        SimpleNamespace(
            key="snap-2",
            run_id="run-2",
            reaction_dir=orphan_dir,
            status="failed",
            name="",
            completed_at="",
            updated_at="2026-04-26T02:00:00+00:00",
            started_at="2026-04-26T01:30:00+00:00",
            attempts=1,
            selected_inp_name="orphan.inp",
            job_type="sp",
        ),
    ]
    reconciled: list[Path] = []

    queue_store = SimpleNamespace(
        reconcile_orphaned_running_entries=lambda root: reconciled.append(root),
        list_queue=lambda root: entries,
        queue_entry_metadata=lambda entry: entry.get("metadata", {}),
        queue_entry_run_id=lambda entry: entry.get("run_id", ""),
        queue_entry_status=lambda entry: entry.get("status", ""),
        queue_entry_reaction_dir=lambda entry: entry.get("reaction_dir", ""),
        queue_entry_id=lambda entry: entry.get("queue_id", ""),
        queue_entry_task_id=lambda entry: entry.get("task_id", ""),
        queue_entry_priority=lambda entry: entry.get("priority", 0),
    )
    run_snapshot = SimpleNamespace(collect_run_snapshots=lambda root: snapshots)
    monkeypatch.setattr(activity, "sibling_runtime_paths", lambda config_path, *, engine: {"allowed_root": allowed})
    monkeypatch.setattr(activity, "_import_orca_runtime_modules", lambda repo_root: (queue_store, run_snapshot))

    rows = activity._orca_records(config_path="/tmp/cfg.yaml", repo_root="/tmp/repo")

    assert reconciled == [allowed]
    by_id = {row.activity_id: row for row in rows}
    assert by_id["q-1"].status == "completed"
    assert by_id["q-1"].label == "tracked-name"
    assert by_id["q-2"].status == "cancel_requested"
    assert by_id["run-2"].status == "failed"
    assert by_id["run-2"].metadata["selected_inp_name"] == "orphan.inp"


def test_orca_matching_helpers_cover_empty_and_active_dir_paths(tmp_path: Path) -> None:
    reaction_dir = tmp_path / "rxn"
    reaction_dir.mkdir()
    snapshot = SimpleNamespace(run_id="run-1", reaction_dir=reaction_dir)
    queue_store = SimpleNamespace(
        queue_entry_run_id=lambda entry: entry.get("run_id", ""),
        queue_entry_status=lambda entry: entry.get("status", ""),
        queue_entry_reaction_dir=lambda entry: entry.get("reaction_dir", ""),
    )

    assert activity._orca_snapshot_matches_entry(queue_store, {"run_id": "run-1"}, {"run-1": snapshot}, {}) is snapshot
    assert activity._orca_snapshot_matches_entry(queue_store, {"status": "completed"}, {}, {}) is None
    assert activity._orca_snapshot_matches_entry(queue_store, {"status": "running", "reaction_dir": ""}, {}, {}) is None
    assert (
        activity._orca_snapshot_matches_entry(
            queue_store,
            {"status": "running", "reaction_dir": str(reaction_dir)},
            {},
            {str(reaction_dir.resolve()): snapshot},
        )
        is snapshot
    )
    assert activity._orca_queue_represents_snapshot(queue_store, {"run_id": "run-1"}, snapshot) is True
    assert activity._orca_queue_represents_snapshot(queue_store, {"status": "completed"}, snapshot) is False
    assert (
        activity._orca_queue_represents_snapshot(
            queue_store,
            {"status": "running", "reaction_dir": str(reaction_dir)},
            snapshot,
        )
        is True
    )


def test_repo_import_helpers_cover_missing_and_module_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = tmp_path / "missing"
    assert activity._ensure_repo_on_syspath(str(missing), fallback_name="unknown") is None

    repo = tmp_path / "repo"
    src = repo / "src"
    src.mkdir(parents=True)
    assert activity._ensure_repo_on_syspath(str(repo), fallback_name="unknown") == repo.resolve()
    assert str(repo.resolve()) in sys.path
    assert str(src.resolve()) in sys.path

    monkeypatch.setattr(activity, "_ensure_repo_on_syspath", lambda repo_root, *, fallback_name: None)
    assert activity._import_orca_runtime_modules(None) is None

    monkeypatch.setattr(activity, "_ensure_repo_on_syspath", lambda repo_root, *, fallback_name: repo)
    monkeypatch.setattr(
        activity.importlib,
        "import_module",
        lambda name: (_ for _ in ()).throw(ModuleNotFoundError(name)),
    )
    assert activity._import_orca_runtime_modules(str(repo)) is None


def test_match_activity_record_and_cancel_error_edges(monkeypatch: pytest.MonkeyPatch) -> None:
    records = [
        activity.ActivityRecord("a", "job", "xtb", "running", "A", "xtb_auto", "", "", "target", aliases=("same",)),
        activity.ActivityRecord("b", "job", "xtb", "running", "B", "xtb_auto", "", "", "target", aliases=("same",)),
    ]
    with pytest.raises(ValueError, match="empty"):
        activity._match_activity_record(records, "")
    with pytest.raises(ValueError, match="Ambiguous activity target"):
        activity._match_activity_record(records, "target")
    with pytest.raises(ValueError, match="Ambiguous activity target"):
        activity._match_activity_record(records, "same")
    with pytest.raises(LookupError, match="not found"):
        activity._match_activity_record(records, "missing")

    def collect_one(record: activity.ActivityRecord) -> None:
        monkeypatch.setattr(activity, "_collect_activity_records", lambda **kwargs: [record])

    monkeypatch.setattr(activity, "_resolve_activity_sources", lambda **kwargs: (None, None, None, None))

    collect_one(activity.ActivityRecord("crest", "job", "crest", "running", "C", "crest_auto", "", "", "crest-q"))
    with pytest.raises(ValueError, match="crest_auto_config"):
        activity.cancel_activity(target="crest-q")

    collect_one(activity.ActivityRecord("xtb", "job", "xtb", "running", "X", "xtb_auto", "", "", "xtb-q"))
    with pytest.raises(ValueError, match="xtb_auto_config"):
        activity.cancel_activity(target="xtb-q")

    collect_one(activity.ActivityRecord("orca", "job", "orca", "running", "O", activity.CHEMSTACK_ORCA_SOURCE, "", "", "orca-q"))
    with pytest.raises(ValueError, match="chemstack_config"):
        activity.cancel_activity(target="orca-q")

    collect_one(activity.ActivityRecord("bad", "job", "x", "running", "B", "unknown", "", "", "bad-q"))
    with pytest.raises(ValueError, match="Unsupported activity source"):
        activity.cancel_activity(target="bad-q")


def test_cancel_activity_routes_crest_and_orca_targets(monkeypatch: pytest.MonkeyPatch) -> None:
    records = {
        "crest-q": activity.ActivityRecord("crest-q", "job", "crest", "running", "C", "crest_auto", "", "", "crest-q"),
        "orca-q": activity.ActivityRecord("orca-q", "job", "orca", "running", "O", activity.CHEMSTACK_ORCA_SOURCE, "", "", "orca-q"),
    }
    monkeypatch.setattr(activity, "_collect_activity_records", lambda **kwargs: list(records.values()))
    monkeypatch.setattr(activity, "cancel_crest_target", lambda **kwargs: {"status": "cancel_requested", **kwargs})
    monkeypatch.setattr(activity, "cancel_orca_target", lambda **kwargs: {"status": "", **kwargs})
    monkeypatch.setattr(activity, "_discover_orca_repo_root", lambda explicit: "/tmp/orca-repo")

    crest_payload = activity.cancel_activity(target="crest-q", crest_auto_config="/tmp/crest.yaml")
    orca_payload = activity.cancel_activity(target="orca-q", orca_auto_config="/tmp/orca.yaml")

    assert crest_payload["status"] == "cancel_requested"
    assert crest_payload["result"]["config_path"] == str(Path("/tmp/crest.yaml").resolve())
    assert orca_payload["status"] == "failed"
    assert orca_payload["result"]["repo_root"] == "/tmp/orca-repo"


def test_clear_activities_clears_workflow_and_engine_terminal_sources(monkeypatch) -> None:
    import chemstack.orca.commands.list_runs as orca_list_runs

    captured_statuses: tuple[str, ...] = ()

    def fake_clear_terminal_workflow_registry(workflow_root, statuses=None):
        nonlocal captured_statuses
        captured_statuses = tuple(sorted(statuses or ()))
        return 2

    monkeypatch.setattr(activity, "clear_terminal_workflow_registry", fake_clear_terminal_workflow_registry)
    monkeypatch.setattr(
        activity,
        "_engine_queue_roots",
        lambda config_path, *, engine: (
            (Path("/tmp/xtb_root_a"), Path("/tmp/xtb_root_b"))
            if engine == "xtb"
            else (Path("/tmp/crest_root"),)
        ),
    )

    cleared_roots: list[str] = []

    def fake_clear_queue_terminal(root: Path) -> int:
        cleared_roots.append(str(root))
        if "xtb_root_a" in str(root):
            return 1
        if "xtb_root_b" in str(root):
            return 2
        return 3

    monkeypatch.setattr(activity, "clear_queue_terminal", fake_clear_queue_terminal)
    monkeypatch.setattr(
        activity,
        "sibling_runtime_paths",
        lambda config_path, *, engine="orca": {"allowed_root": Path("/tmp/orca_root")},
    )
    monkeypatch.setattr(orca_list_runs, "clear_terminal_entries", lambda allowed_root: (4, 5))

    payload = activity.clear_activities(
        workflow_root="/tmp/workflows",
        crest_auto_config="/tmp/chemstack.yaml",
        xtb_auto_config="/tmp/chemstack.yaml",
        orca_auto_config="/tmp/chemstack.yaml",
    )

    assert payload["total_cleared"] == 17
    assert payload["cleared"] == {
        "workflows": 2,
        "xtb_queue_entries": 3,
        "crest_queue_entries": 3,
        "orca_queue_entries": 4,
        "orca_run_states": 5,
    }
    assert "submission_failed" in captured_statuses
    assert cleared_roots == ["/tmp/xtb_root_a", "/tmp/xtb_root_b", "/tmp/crest_root"]


def test_clear_activities_keeps_cleared_terminal_workflows_hidden_from_listing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(activity, "_resolve_activity_sources", lambda **kwargs: (str(tmp_path), None, None, None))

    workspace = tmp_path / "wf_completed"
    workspace.mkdir(parents=True)
    (workspace / "workflow.json").write_text(
        json.dumps(
            {
                "workflow_id": "wf_completed",
                "template_name": "reaction_ts_search",
                "status": "completed",
                "source_job_id": "job-1",
                "source_job_type": "reaction_ts_search",
                "reaction_key": "rxn-1",
                "requested_at": "2026-04-20T10:00:00+00:00",
                "stages": [],
                "metadata": {},
            }
        ),
        encoding="utf-8",
    )

    initial = activity.list_activities(workflow_root=tmp_path)
    assert [item["activity_id"] for item in initial["activities"]] == ["wf_completed"]

    cleared = activity.clear_activities(workflow_root=tmp_path)
    assert cleared["cleared"]["workflows"] == 1

    after_clear = activity.list_activities(workflow_root=tmp_path)
    assert after_clear["activities"] == []


def test_cmd_activity_list_text_output(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "list_activities",
        lambda **kwargs: {
            "count": 2,
            "activities": [
                {
                    "activity_id": "wf-1",
                    "engine": "xtb",
                    "status": "running",
                    "label": "rxn-a",
                    "source": "chem_flow",
                },
                {
                    "activity_id": "xtb-q-1",
                    "engine": "xtb",
                    "status": "pending",
                    "label": "rxn-b",
                    "source": "xtb_auto",
                },
            ],
        },
    )

    assert cli.cmd_activity_list(
        SimpleNamespace(
            workflow_root="/tmp/wf",
            limit=0,
            refresh=False,
            crest_auto_config=None,
            xtb_auto_config="/tmp/xtb.yaml",
            orca_auto_config=None,
            orca_auto_repo_root=None,
            json=False,
        )
    ) == 0

    stdout = capsys.readouterr().out
    assert "activity_count: 2" in stdout
    assert "- wf-1 engine=xtb status=running label=rxn-a source=chem_flow" in stdout
    assert "- xtb-q-1 engine=xtb status=pending label=rxn-b source=xtb_auto" in stdout


def test_cmd_activity_cancel_json_and_error_paths(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "cancel_activity",
        lambda **kwargs: {
            "activity_id": "crest-q-1",
            "engine": "crest",
            "source": "crest_auto",
            "label": "mol-a",
            "status": "cancel_requested",
            "cancel_target": "crest-q-1",
        },
    )
    args = SimpleNamespace(
        target="crest-q-1",
        workflow_root=None,
        crest_auto_config="/tmp/crest.yaml",
        crest_auto_executable="crest_auto",
        crest_auto_repo_root=None,
        xtb_auto_config=None,
        xtb_auto_executable="xtb_auto",
        xtb_auto_repo_root=None,
        orca_auto_config=None,
        orca_auto_executable="orca_auto",
        orca_auto_repo_root=None,
        json=True,
    )
    assert cli.cmd_activity_cancel(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "cancel_requested"

    def fake_cancel_activity(**kwargs: Any) -> dict[str, Any]:
        raise LookupError("Activity target not found: missing")

    monkeypatch.setattr(cli, "cancel_activity", fake_cancel_activity)
    args.json = False
    args.target = "missing"
    assert cli.cmd_activity_cancel(args) == 1
    assert "error: Activity target not found: missing" in capsys.readouterr().out


def test_build_parser_parses_top_level_activity_commands() -> None:
    parser = cli.build_parser()

    list_args = parser.parse_args(
        [
            "list",
            "--workflow-root",
            "/tmp/wf",
            "--chemstack-config",
            "/tmp/chemstack.yaml",
            "--json",
        ]
    )
    assert list_args.command == "list"
    assert list_args.workflow_root == "/tmp/wf"
    assert list_args.chemstack_config == "/tmp/chemstack.yaml"
    assert list_args.func is cli.cmd_activity_list

    cancel_args = parser.parse_args(
        [
            "cancel",
            "xtb-q-1",
            "--chemstack-config",
            "/tmp/chemstack.yaml",
        ]
    )
    assert cancel_args.command == "cancel"
    assert cancel_args.target == "xtb-q-1"
    assert cancel_args.chemstack_config == "/tmp/chemstack.yaml"
    assert cancel_args.func is cli.cmd_activity_cancel

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "cancel",
                "xtb-q-1",
                "--crest-executable",
                "crest-bin",
                "--xtb-executable",
                "xtb-bin",
                "--orca-auto-executable",
                "orca-auto-bin",
            ]
        )


def test_build_parser_rejects_removed_activity_alias_flags() -> None:
    parser = cli.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["cancel", "xtb-q-1", "--chemstack-executable", "orca_auto"])


def test_list_activities_autodiscovers_defaults_when_no_args(monkeypatch) -> None:
    monkeypatch.setattr(activity, "_discover_workflow_root", lambda workflow_root: "/tmp/workflow_root")
    monkeypatch.setattr(
        activity,
        "_discover_sibling_config",
        lambda explicit, *, app_name: "/tmp/chemstack.yaml",
    )
    captured: dict[str, Any] = {}

    def fake_collect(**kwargs: Any) -> list[activity.ActivityRecord]:
        captured.update(kwargs)
        return []

    monkeypatch.setattr(activity, "_collect_activity_records", fake_collect)

    payload = activity.list_activities()

    assert payload["count"] == 0
    assert payload["sources"] == {
        "workflow_root": str(Path("/tmp/workflow_root").resolve()),
        "crest_auto_config": "/tmp/chemstack.yaml",
        "xtb_auto_config": "/tmp/chemstack.yaml",
        "orca_auto_config": "/tmp/chemstack.yaml",
    }
    assert captured["workflow_root"] == "/tmp/workflow_root"
    assert captured["crest_auto_config"] == "/tmp/chemstack.yaml"
    assert captured["xtb_auto_config"] == "/tmp/chemstack.yaml"
    assert captured["orca_auto_config"] == "/tmp/chemstack.yaml"


def test_cancel_activity_autodiscovers_defaults(monkeypatch) -> None:
    monkeypatch.setattr(activity, "_discover_workflow_root", lambda workflow_root: "/tmp/workflow_root")
    monkeypatch.setattr(
        activity,
        "_discover_sibling_config",
        lambda explicit, *, app_name: "/tmp/chemstack.yaml",
    )
    monkeypatch.setattr(
        activity,
        "_collect_activity_records",
        lambda **kwargs: [
            activity.ActivityRecord(
                activity_id="wf-77",
                kind="workflow",
                engine="workflow",
                status="running",
                label="wf-77",
                source="chem_flow",
                submitted_at="2026-04-20T10:00:00+00:00",
                updated_at="2026-04-20T10:00:00+00:00",
                cancel_target="wf-77",
                aliases=("wf-77",),
                metadata={},
            )
        ],
    )
    captured: dict[str, Any] = {}

    def fake_cancel_workflow(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {"workflow_id": "wf-77", "status": "cancelled"}

    monkeypatch.setattr(operations, "cancel_workflow", fake_cancel_workflow)

    payload = activity.cancel_activity(target="wf-77")

    assert payload["status"] == "cancelled"
    assert captured["workflow_root"] == "/tmp/workflow_root"
    assert captured["crest_auto_config"] == "/tmp/chemstack.yaml"
    assert captured["xtb_auto_config"] == "/tmp/chemstack.yaml"
    assert captured["orca_auto_config"] == "/tmp/chemstack.yaml"


def test_build_parser_accepts_one_line_activity_commands() -> None:
    parser = cli.build_parser()

    list_args = parser.parse_args(["list"])
    assert list_args.command == "list"
    assert list_args.workflow_root is None
    assert list_args.func is cli.cmd_activity_list

    cancel_args = parser.parse_args(["cancel", "wf-1"])
    assert cancel_args.command == "cancel"
    assert cancel_args.target == "wf-1"
    assert cancel_args.func is cli.cmd_activity_cancel
