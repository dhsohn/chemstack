# ruff: noqa: E402

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from chemstack.flow import cli, registry, xyz_utils
from chemstack.flow.adapters import crest as crest_adapter
from chemstack.flow.adapters import orca as orca_adapter
from chemstack.flow.adapters import xtb as xtb_adapter
from chemstack.flow.contracts import CrestArtifactContract, WorkflowStageInput, orca as orca_contracts
from chemstack.flow.contracts.xtb import XtbArtifactContract, XtbCandidateArtifact, XtbDownstreamPolicy
from chemstack.flow.submitters import common
from chemstack.flow.workflows import conformer_screening


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def test_crest_and_xtb_resolve_job_dir_edge_branches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
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

    record = SimpleNamespace(latest_known_path="bad", organized_output_dir=str(valid_dir), original_run_dir="")
    monkeypatch.setattr(crest_adapter, "resolve_job_location", lambda index_root, target: record)
    monkeypatch.setattr(crest_adapter, "Path", _PathProxy)
    resolved_dir, resolved_record = crest_adapter._resolve_job_dir(tmp_path, "job_1")
    assert resolved_dir == valid_dir.resolve()
    assert resolved_record is record

    monkeypatch.setattr(crest_adapter, "resolve_job_location", lambda index_root, target: None)
    monkeypatch.setattr(crest_adapter, "_direct_path_target", lambda target: None)
    with pytest.raises(FileNotFoundError, match="CREST job directory not found"):
        crest_adapter._resolve_job_dir(tmp_path, "missing")

    monkeypatch.setattr(xtb_adapter, "resolve_job_location", lambda index_root, target: record)
    monkeypatch.setattr(xtb_adapter, "Path", _PathProxy)
    resolved_dir, resolved_record = xtb_adapter._resolve_job_dir(tmp_path, "job_2")
    assert resolved_dir == valid_dir.resolve()
    assert resolved_record is record

    bad_json = tmp_path / "bad.json"
    bad_json.write_text("{broken", encoding="utf-8")
    assert xtb_adapter._load_json_dict(bad_json) == {}

    monkeypatch.setattr(xtb_adapter, "resolve_job_location", lambda index_root, target: None)
    monkeypatch.setattr(xtb_adapter, "_direct_path_target", lambda target: None)
    with pytest.raises(FileNotFoundError, match="xTB job directory not found"):
        xtb_adapter._resolve_job_dir(tmp_path, "missing")

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
    policy = XtbDownstreamPolicy(preferred_kinds=(), max_candidates=1, selected_only=False, allowed_kinds=(), fallback_to_selected_paths=True)
    rows = xtb_adapter.select_xtb_downstream_inputs(contract, policy=policy, require_geometry=False)
    assert len(rows) == 1
    assert rows[0].artifact_path == "/tmp/c1.xyz"


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
    policy = XtbDownstreamPolicy(preferred_kinds=(), max_candidates=1, selected_only=False, allowed_kinds=(), fallback_to_selected_paths=False)

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
    monkeypatch.setattr(registry, "_journal_notification_enabled", lambda event_type: False)
    monkeypatch.setattr(registry, "_telegram_transport_from_env", lambda: SimpleNamespace(send_text=lambda text: sent.append(text)))
    registry._maybe_notify_journal_event({"event_type": "worker_started"}, tmp_path)
    assert sent == []

    monkeypatch.setattr(registry, "_journal_notification_enabled", lambda event_type: True)
    monkeypatch.setattr(registry, "_telegram_transport_from_env", lambda: None)
    registry._maybe_notify_journal_event({"event_type": "worker_started"}, tmp_path)
    assert sent == []

    journal_path = registry.workflow_journal_path(tmp_path)
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    journal_path.write_text('{"event_type":"a"}\n{"event_type":"b"}\n', encoding="utf-8")
    rows = registry.list_workflow_journal(tmp_path, limit=0)
    assert len(rows) == 2

    monkeypatch.setattr(
        registry,
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

    monkeypatch.setattr(registry, "Path", _BrokenPath)
    monkeypatch.setattr(
        registry,
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
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    payload = {"workflow_id": "wf1", "status": "running", "stages": [{}]}
    monkeypatch.setattr(cli, "advance_materialized_workflow", lambda **kwargs: payload)
    assert cli.cmd_workflow_advance(SimpleNamespace(target="wf1", workflow_root="/tmp/wf", json=True, no_submit=False)) == 0
    assert json.loads(capsys.readouterr().out)["workflow_id"] == "wf1"

    monkeypatch.setattr(cli, "get_workflow_runtime_status", lambda **kwargs: {"worker_state": {"status": "running"}})
    assert cli.cmd_workflow_runtime_status(SimpleNamespace(workflow_root="/tmp/wf", json=True)) == 0
    assert json.loads(capsys.readouterr().out)["worker_state"]["status"] == "running"

    monkeypatch.setattr(cli, "get_workflow_journal", lambda **kwargs: {"events": []})
    assert cli.cmd_workflow_journal(SimpleNamespace(workflow_root="/tmp/wf", limit=1, json=True)) == 0
    assert json.loads(capsys.readouterr().out)["events"] == []

    monkeypatch.setattr(cli, "submit_reaction_ts_search_workflow", lambda **kwargs: {"workflow_id": "wf_submit"})
    assert cli.cmd_workflow_submit_reaction_ts_search(SimpleNamespace(target="wf1", workflow_root="/tmp/wf", orca_auto_config="/tmp/cfg", orca_auto_executable="orca_auto", orca_auto_repo_root=None, resubmit=False, json=True)) == 0
    assert json.loads(capsys.readouterr().out)["workflow_id"] == "wf_submit"

    monkeypatch.setattr(cli, "list_workflows", lambda **kwargs: {"count": 0, "workflows": []})
    assert cli.cmd_workflow_list(SimpleNamespace(workflow_root="/tmp/wf", limit=0, refresh=False, json=True)) == 0
    assert json.loads(capsys.readouterr().out)["count"] == 0

    monkeypatch.setattr(cli, "get_workflow", lambda **kwargs: {"summary": {"workflow_id": "wf_get"}})
    assert cli.cmd_workflow_get(SimpleNamespace(target="wf_get", workflow_root="/tmp/wf", json=True)) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["workflow_id"] == "wf_get"

    monkeypatch.setattr(cli, "get_workflow_artifacts", lambda **kwargs: {"artifact_count": 0})
    assert cli.cmd_workflow_artifacts(SimpleNamespace(target="wf_art", workflow_root="/tmp/wf", json=True)) == 0
    assert json.loads(capsys.readouterr().out)["artifact_count"] == 0

    monkeypatch.setattr(cli, "cancel_workflow", lambda **kwargs: {"workflow_id": "wf_cancel"})
    assert cli.cmd_workflow_cancel(SimpleNamespace(target="wf_cancel", workflow_root="/tmp/wf", crest_auto_config=None, crest_auto_executable="crest_auto", crest_auto_repo_root=None, xtb_auto_config=None, xtb_auto_executable="xtb_auto", xtb_auto_repo_root=None, orca_auto_config=None, orca_auto_executable="orca_auto", orca_auto_repo_root=None, json=True)) == 0
    assert json.loads(capsys.readouterr().out)["workflow_id"] == "wf_cancel"

    monkeypatch.setattr(cli, "reindex_workflow_registry", lambda workflow_root: [])
    assert cli.cmd_workflow_reindex(SimpleNamespace(workflow_root="/tmp/wf", json=True)) == 0
    assert json.loads(capsys.readouterr().out)["count"] == 0

    slept: list[float] = []

    @contextmanager
    def _lock(path: object, timeout_seconds: float = 0.0):
        yield

    monkeypatch.setattr(cli, "file_lock", _lock)
    monkeypatch.setattr(cli, "workflow_worker_lock_path", lambda workflow_root: Path("/tmp/lock"))
    monkeypatch.setattr(cli, "write_workflow_worker_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "append_workflow_journal_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "advance_workflow_registry_once", lambda **kwargs: {"cycle_started_at": "t", "worker_session_id": "s", "discovered_count": 0, "advanced_count": 0, "skipped_count": 0, "failed_count": 0, "workflow_results": []})
    monkeypatch.setattr(cli, "_emit_worker_payload", lambda payload, json_mode, single_cycle: None)
    monkeypatch.setattr(cli, "now_utc_iso", lambda: "2026-04-19T00:00:00+00:00")
    monkeypatch.setattr(cli.time, "sleep", lambda seconds: slept.append(seconds))
    assert cli.cmd_workflow_worker(
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
            crest_auto_config=None,
            crest_auto_executable="crest_auto",
            crest_auto_repo_root=None,
            xtb_auto_config=None,
            xtb_auto_executable="xtb_auto",
            xtb_auto_repo_root=None,
            orca_auto_config=None,
            orca_auto_executable="orca_auto",
            orca_auto_repo_root=None,
        )
    ) == 0
    assert slept == [0.1]

    monkeypatch.setattr(conformer_screening, "timestamped_token", lambda prefix: f"{prefix}_x")
    assert conformer_screening._workflow_id(
        CrestArtifactContract(
            job_id="crest_job",
            mode="nci",
            status="done",
            reason="ok",
            job_dir="/tmp/job",
            latest_known_path="/tmp/job",
        )
    ) == "wf_conformer_screening_x"
    assert orca_contracts._normalize_text(None) == "None"


def test_conformer_screening_common_and_xyz_tail_branches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text("allowed_root: /tmp/no_runtime\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Missing runtime section"):
        common.sibling_allowed_root(str(cfg))
    with pytest.raises(ValueError, match="Missing runtime section"):
        common.sibling_runtime_paths(str(cfg))

    monkeypatch.setattr(
        conformer_screening,
        "select_crest_downstream_inputs",
        lambda contract, policy: (
            WorkflowStageInput(
                source_job_id="crest_job",
                source_job_type="crest_nci",
                reaction_key="mol_1",
                selected_input_xyz="/tmp/conf_1.xyz",
                rank=1,
                kind="retained_conformer",
                artifact_path="/tmp/conf_1.xyz",
                selected=True,
                metadata={},
            ),
        ),
    )
    contract = CrestArtifactContract(
        job_id="crest_job",
        mode="nci",
        status="done",
        reason="ok",
        job_dir="/tmp/job",
        latest_known_path="/tmp/job",
        molecule_key="mol_1",
    )
    payload = conformer_screening.build_conformer_screening_plan(contract, workspace_root=None)
    assert payload["stages"] == []
    assert payload["metadata"]["workspace_dir"] == ""

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

    monkeypatch.setattr(orca_adapter, "_tracked_contract_payload", lambda **kwargs: None)
    monkeypatch.setattr(orca_adapter, "_tracked_runtime_context", lambda **kwargs: None)
    monkeypatch.setattr(orca_adapter, "_tracked_artifact_context", lambda **kwargs: (None, None, {}, {}, {}))
    monkeypatch.setattr(orca_adapter, "_resolve_job_dir", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(
        orca_adapter,
        "_find_queue_entry",
        lambda **kwargs: {
            "queue_id": "q_01",
            "task_id": "job_01",
            "reaction_dir": str(queue_dir),
            "status": "queued",
        },
    )
    monkeypatch.setattr(orca_adapter, "_resolve_candidate_path", lambda raw: None)
    monkeypatch.setattr(orca_adapter, "_direct_dir_target", lambda target: None)
    monkeypatch.setattr(orca_adapter, "_find_organized_record", lambda **kwargs: None)
    monkeypatch.setattr(orca_adapter, "_record_organized_dir", lambda record: None)
    monkeypatch.setattr(orca_adapter, "_organized_dir_from_record", lambda organized_root, record: None)
    monkeypatch.setattr(orca_adapter, "_load_json_dict", lambda path: {})
    monkeypatch.setattr(orca_adapter, "_load_tracked_organized_ref", lambda tracked_record, current_dir: {})

    contract = orca_adapter.load_orca_artifact_contract(
        target="job_01",
        orca_allowed_root=allowed_root,
    )

    assert contract.latest_known_path == "job_01"
