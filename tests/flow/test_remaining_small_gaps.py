from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from chemstack.core.indexing import JobLocationRecord
from chemstack.core.utils.coercion import normalize_text
from chemstack.flow import _registry_notifications as registry_notifications
from chemstack.flow import (
    cli_workflow,
    engine_runtime,
    registry,
    registry_store,
    workflow_journal,
    xyz_utils,
)
from chemstack.flow.adapters import _engine_adapter_helpers as adapter_helpers
from chemstack.flow.adapters import _orca_local_lookup, _orca_path_helpers, _orca_tracking
from chemstack.flow.adapters import orca as orca_adapter
from chemstack.flow.adapters import xtb as xtb_adapter
from chemstack.flow.contracts.xtb import (
    XtbArtifactContract,
    XtbCandidateArtifact,
    XtbDownstreamPolicy,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


@pytest.mark.parametrize("missing_label", ["CREST", "xTB"])
def test_resolve_indexed_job_dir_edge_branches(tmp_path: Path, missing_label: str) -> None:
    valid_dir = tmp_path / "valid"
    valid_dir.mkdir()

    class _PathProxy:
        def __init__(self, raw: str) -> None:
            self.raw = raw

        def expanduser(self) -> "_PathProxy":
            return self

        def resolve(self) -> Path:
            if self.raw == "bad":
                raise OSError("bad")
            return Path(self.raw).resolve()

    record = JobLocationRecord(
        job_id="job_1",
        app_name="chemstack_test",
        job_type="test",
        status="running",
        original_run_dir="",
        organized_output_dir=str(valid_dir),
        latest_known_path="bad",
    )
    resolved_dir, resolved_record = adapter_helpers.resolve_indexed_job_dir(
        tmp_path,
        "job_1",
        resolve_job_location_fn=lambda index_root, target: record,
        direct_path_target_fn=lambda raw: adapter_helpers.direct_dir_target(
            raw, path_factory=_PathProxy
        ),
        missing_label=missing_label,
        path_factory=_PathProxy,
    )
    assert resolved_dir == valid_dir.resolve()
    assert resolved_record is record

    with pytest.raises(FileNotFoundError, match=f"{missing_label} job directory not found"):
        adapter_helpers.resolve_indexed_job_dir(
            tmp_path,
            "missing",
            resolve_job_location_fn=lambda index_root, target: None,
            direct_path_target_fn=lambda raw: adapter_helpers.direct_dir_target(
                raw, path_factory=_PathProxy
            ),
            missing_label=missing_label,
            path_factory=_PathProxy,
        )

    bad_json = tmp_path / "bad.json"
    bad_json.write_text("{broken", encoding="utf-8")
    assert adapter_helpers.load_json_dict(bad_json) == {}

    contract = XtbArtifactContract(
        job_id="xtb_job",
        job_type="path_search",
        status="completed",
        reason="ok",
        job_dir="/tmp/job",
        latest_known_path="/tmp/job",
        reaction_key="rxn",
        selected_input_xyz="/tmp/input.xyz",
        selected_candidate_paths=("/tmp/c1.xyz", "/tmp/c2.xyz"),
        candidate_details=(),
    )
    policy = XtbDownstreamPolicy(
        preferred_kinds=(),
        max_candidates=1,
        selected_only=False,
        allowed_kinds=(),
    )
    rows = xtb_adapter.select_xtb_downstream_inputs(contract, policy=policy, require_geometry=False)
    assert rows == ()


def test_xtb_select_downstream_inputs_breaks_after_max_candidates() -> None:
    contract = XtbArtifactContract(
        job_id="xtb_job",
        job_type="path_search",
        status="completed",
        reason="ok",
        job_dir="/tmp/job",
        latest_known_path="/tmp/job",
        reaction_key="rxn",
        selected_input_xyz="/tmp/input.xyz",
        selected_candidate_paths=(),
        candidate_details=(
            XtbCandidateArtifact(rank=1, kind="path", path="/tmp/first.xyz", selected=True, score=-1.0),
            XtbCandidateArtifact(rank=2, kind="path", path="/tmp/second.xyz", selected=False, score=-0.5),
        ),
    )
    policy = XtbDownstreamPolicy(
        preferred_kinds=(),
        max_candidates=1,
        selected_only=False,
        allowed_kinds=(),
    )

    rows = xtb_adapter.select_xtb_downstream_inputs(contract, policy=policy, require_geometry=False)

    assert len(rows) == 1
    assert rows[0].artifact_path == "/tmp/first.xyz"

    detail_contract = XtbArtifactContract(
        job_id="xtb_job",
        job_type="path_search",
        status="completed",
        reason="ok",
        job_dir="/tmp/job",
        latest_known_path="/tmp/job",
        reaction_key="rxn",
        selected_input_xyz="/tmp/input.xyz",
        selected_candidate_paths=(),
        candidate_details=(
            XtbCandidateArtifact(rank=1, kind="ts_guess", path="/tmp/d1.xyz", selected=True, score=-1.0),
            XtbCandidateArtifact(rank=2, kind="ts_guess", path="/tmp/d2.xyz", selected=True, score=-2.0),
        ),
    )
    detail_rows = xtb_adapter.select_xtb_downstream_inputs(detail_contract, policy=policy, require_geometry=False)
    assert len(detail_rows) == 1
    assert detail_rows[0].artifact_path == "/tmp/d1.xyz"


def test_registry_notification_and_resolution_remaining_edges(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sent: list[str] = []
    monkeypatch.setattr(registry_notifications, "journal_notification_enabled", lambda event_type: False)
    monkeypatch.setattr(
        registry_notifications,
        "telegram_transport_from_env",
        lambda: SimpleNamespace(send_text=lambda text: sent.append(text)),
    )
    workflow_journal._maybe_notify_journal_event({"event_type": "worker_started"}, tmp_path)
    assert sent == []

    monkeypatch.setattr(registry_notifications, "journal_notification_enabled", lambda event_type: True)
    monkeypatch.setattr(registry_notifications, "telegram_transport_from_env", lambda: None)
    workflow_journal._maybe_notify_journal_event({"event_type": "worker_started"}, tmp_path)
    assert sent == []

    journal_path = registry.workflow_journal_path(tmp_path)
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    journal_path.write_text('{"event_type":"a"}\n{"event_type":"b"}\n', encoding="utf-8")
    rows = registry.list_workflow_journal(tmp_path, limit=0)
    assert len(rows) == 2

    monkeypatch.setattr(
        registry_store,
        "list_workflow_registry",
        lambda workflow_root: [
            registry.WorkflowRegistryRecord(
                workflow_id="wf1",
                template_name="rxn",
                status="running",
                source_job_id="job",
                source_job_type="rxn",
                reaction_key="rxn",
                requested_at="2026-04-19T00:00:00+00:00",
                workspace_dir="/tmp/w1",
                workflow_file="/tmp/w1/workflow.json",
            )
        ],
    )
    assert registry.get_workflow_registry_record(tmp_path, "wf-missing") is None
    assert registry.resolve_workflow_registry_record(tmp_path, "wf-missing") is None

    class _BrokenPath:
        def __init__(self, raw: str) -> None:
            self.raw = raw

        def expanduser(self) -> "_BrokenPath":
            return self

        def resolve(self) -> Path:
            raise OSError("bad")

    monkeypatch.setattr(registry_store, "Path", _BrokenPath)
    monkeypatch.setattr(
        registry_store,
        "list_workflow_registry",
        lambda workflow_root: [
            registry.WorkflowRegistryRecord(
                workflow_id="wf1",
                template_name="rxn",
                status="running",
                source_job_id="job",
                source_job_type="rxn",
                reaction_key="rxn",
                requested_at="2026-04-19T00:00:00+00:00",
                workspace_dir="/tmp/w1",
                workflow_file="/tmp/w1/workflow.json",
            )
        ],
    )
    assert registry.resolve_workflow_registry_record(tmp_path, "/tmp/bad") is None


def test_cli_json_paths_worker_sleep_and_common_workflow_id_helpers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    slept: list[float] = []

    @contextmanager
    def _lock(path: object, timeout_seconds: float = 0.0):
        yield

    monkeypatch.setattr(cli_workflow, "file_lock", _lock)
    monkeypatch.setattr(cli_workflow, "workflow_worker_lock_path", lambda workflow_root: Path("/tmp/lock"))
    monkeypatch.setattr(cli_workflow, "write_workflow_worker_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli_workflow, "append_workflow_journal_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli_workflow, "advance_workflow_registry_once", lambda **kwargs: {"cycle_started_at": "t", "worker_session_id": "s", "discovered_count": 0, "advanced_count": 0, "skipped_count": 0, "failed_count": 0, "workflow_results": []})
    monkeypatch.setattr(cli_workflow, "_emit_worker_payload", lambda payload, json_mode, single_cycle: None)
    monkeypatch.setattr(cli_workflow, "now_utc_iso", lambda: "2026-04-19T00:00:00+00:00")
    monkeypatch.setattr(cli_workflow.time, "sleep", lambda seconds: slept.append(seconds))
    assert cli_workflow.cmd_workflow_worker(
        SimpleNamespace(
            once=False,
            max_cycles=2,
            interval_seconds=0.1,
            lock_timeout_seconds=1.0,
            refresh_registry=False,
            refresh_each_cycle=False,
            service_mode=False,
            json=False,
            workflow_root="/tmp/wf",
            worker_session_id="worker_1",
            lease_seconds=1.0,
            no_submit=False,
            crest_config=None,
            xtb_config=None,
            orca_config=None,
            orca_repo_root=None,
        )
    ) == 0
    assert slept == [0.1]

    assert normalize_text(None, none="None") == "None"


def test_engine_config_common_and_xyz_tail_branches(
    tmp_path: Path,
) -> None:
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text("allowed_root: /tmp/no_runtime\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Missing runtime section"):
        engine_runtime.engine_runtime_paths(str(cfg))

    xyz_path = tmp_path / "trail.xyz"
    xyz_path.write_text("2\ncomment\nH 0 0 0\nH 0 0 0.7\n\n\n", encoding="utf-8")
    frames = xyz_utils.load_xyz_frames(xyz_path)
    assert len(frames) == 1


def test_load_orca_artifact_contract_falls_back_to_plain_target_when_no_path_resolves(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowed_root = tmp_path / "orca_runs"
    queue_dir = allowed_root / "queued_only"

    monkeypatch.setattr(_orca_tracking, "load_orca_contract_payload_impl", lambda **kwargs: None)
    monkeypatch.setattr(_orca_tracking, "tracked_runtime_context_impl", lambda **kwargs: None)
    monkeypatch.setattr(
        _orca_tracking,
        "tracked_artifact_context_impl",
        lambda **kwargs: (None, None, {}, {}, {}),
    )
    monkeypatch.setattr(_orca_local_lookup, "resolve_job_dir_impl", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(
        _orca_local_lookup,
        "find_queue_entry_impl",
        lambda **kwargs: {
            "queue_id": "q_01",
            "task_id": "job_01",
            "reaction_dir": str(queue_dir),
            "status": "queued",
        },
    )
    monkeypatch.setattr(_orca_path_helpers, "resolve_candidate_path_impl", lambda raw: None)
    monkeypatch.setattr(_orca_path_helpers, "direct_dir_target_impl", lambda target: None)
    monkeypatch.setattr(_orca_local_lookup, "find_organized_record_impl", lambda **kwargs: None)
    monkeypatch.setattr(_orca_local_lookup, "record_organized_dir_impl", lambda record: None)
    monkeypatch.setattr(
        _orca_local_lookup, "organized_dir_from_record_impl", lambda organized_root, record: None
    )
    monkeypatch.setattr(_orca_local_lookup, "load_json_dict_impl", lambda path: {})
    monkeypatch.setattr(
        _orca_local_lookup,
        "load_tracked_organized_ref_impl",
        lambda tracked_record, current_dir: {},
    )

    contract = orca_adapter.load_orca_artifact_contract(
        target="job_01",
        orca_allowed_root=allowed_root,
    )

    assert contract.latest_known_path == "job_01"
