# ruff: noqa: E402

from __future__ import annotations

import json
import runpy
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from chemstack.flow import cli, operations, registry, runtime, state, xyz_utils
from chemstack.flow.adapters import crest as crest_adapter
from chemstack.flow.adapters import xtb as xtb_adapter
from chemstack.flow.contracts.crest import CrestArtifactContract, CrestDownstreamPolicy, to_workflow_stage_inputs
from chemstack.flow.contracts.orca import OrcaArtifactContract
from chemstack.flow.contracts.workflow import WorkflowArtifactRef, WorkflowTask
from chemstack.flow.contracts.xtb import (
    WorkflowStageInput,
    XtbArtifactContract,
    XtbCandidateArtifact,
    XtbDownstreamPolicy,
    _coerce_resource_dict,
)
from chemstack.flow.submitters import common
from chemstack.flow.workflows import orca_stage_utils, reaction_ts_search


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _write_xyz(path: Path, comment: str = "comment") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"2\n{comment}\nH 0 0 0\nH 0 0 0.7\n", encoding="utf-8")


def _append_then_return_failure(calls: list[dict[str, Any]], **kwargs: Any) -> None:
    calls.append(kwargs)
    pytest.fail("unexpected materialization")


def _record_call(calls: list[tuple[str, dict[str, Any]]], name: str, **kwargs: Any) -> dict[str, str]:
    calls.append((name, kwargs))
    return {"ok": name}


def test_materialize_orca_stage_generates_default_input_file(tmp_path: Path) -> None:
    source_xyz = tmp_path / "source.xyz"
    _write_xyz(source_xyz, comment="source")

    materialized = orca_stage_utils.materialize_orca_stage(
        workspace_dir=tmp_path,
        stage_root_name="stage_root",
        stage_key="candidate_01",
        source_artifact_path=str(source_xyz),
        candidate_kind="ts_guess",
        route_line="! ignored when template is present",
        charge=-1,
        multiplicity=2,
        max_cores=6,
        max_memory_gb=18,
        xyz_filename="ts_guess.xyz",
        inp_filename="ts_guess.inp",
    )

    selected_inp = Path(materialized.selected_inp)
    rendered = selected_inp.read_text(encoding="utf-8")
    assert selected_inp.name == "ts_guess.inp"
    assert "! ignored when template is present" in rendered
    assert "nprocs 6" in rendered
    assert "%maxcore 3072" in rendered
    assert "* xyzfile -1 2 ts_guess.xyz" in rendered
    assert Path(materialized.selected_xyz).name == "ts_guess.xyz"


def test_build_orca_enqueue_payload_includes_resource_override_flags() -> None:
    payload = orca_stage_utils.build_orca_enqueue_payload(
        workflow_id="wf_orca",
        stage_id="orca_01",
        reaction_dir="/tmp/rxn",
        selected_inp="/tmp/rxn/input.inp",
        priority=5,
        resource_request={"max_cores": 14, "max_memory_gb": 56},
        source_job_id="xtb_job_01",
        reaction_key="rxn_01",
    )

    assert payload["command_argv"] == [
        "python",
        "-m",
        "chemstack.cli",
        "--config",
        "<chemstack_config>",
        "run-dir",
        "/tmp/rxn",
        "--priority",
        "5",
        "--max-cores",
        "14",
        "--max-memory-gb",
        "56",
    ]
    assert "--max-cores 14" in payload["command"]
    assert "--max-memory-gb 56" in payload["command"]
    assert payload["max_cores"] == 14
    assert payload["max_memory_gb"] == 56


def test_crest_and_xtb_adapter_helper_edges(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert crest_adapter._direct_path_target("   ") is None

    class _BrokenPath:
        def expanduser(self) -> "_BrokenPath":
            raise OSError("bad")

    monkeypatch.setattr(crest_adapter, "Path", lambda raw: _BrokenPath())
    assert crest_adapter._direct_path_target("/tmp/ignored") is None
    monkeypatch.undo()

    assert crest_adapter._retained_paths({"retained_conformer_paths": "bad"}) == ()
    assert crest_adapter._retained_paths({"retained_conformer_paths": ["", "  ", "/tmp/conf.xyz"]}) == ("/tmp/conf.xyz",)

    index_root = tmp_path / "crest_index"
    _write_json(
        index_root / "job_locations.json",
        [
            {
                "job_id": "crest_missing_payload",
                "app_name": "crest_auto",
                "job_type": "standard",
                "status": "completed",
                "latest_known_path": str(tmp_path / "crest_job"),
            }
        ],
    )
    (tmp_path / "crest_job").mkdir()
    with pytest.raises(FileNotFoundError, match="CREST artifact files not found"):
        crest_adapter.load_crest_artifact_contract(crest_index_root=index_root, target="crest_missing_payload")

    assert xtb_adapter._job_type_from_record(None, "fallback") == "fallback"
    assert xtb_adapter._load_json_dict(tmp_path / "missing_xtb.json") == {}
    assert xtb_adapter._direct_path_target("   ") is None

    monkeypatch.setattr(xtb_adapter, "Path", lambda raw: _BrokenPath())
    assert xtb_adapter._direct_path_target("/tmp/ignored") is None
    monkeypatch.undo()

    xtb_index = tmp_path / "xtb_index"
    _write_json(
        xtb_index / "job_locations.json",
        [
            {
                "job_id": "xtb_missing_payload",
                "app_name": "xtb_auto",
                "job_type": "xtb_path_search",
                "status": "completed",
                "latest_known_path": str(tmp_path / "xtb_job"),
            }
        ],
    )
    (tmp_path / "xtb_job").mkdir()
    with pytest.raises(FileNotFoundError, match="xTB artifact files not found"):
        xtb_adapter.load_xtb_artifact_contract(xtb_index_root=xtb_index, target="xtb_missing_payload")

    contract = XtbArtifactContract(
        job_id="xtb_job",
        job_type="path_search",
        status="completed",
        reason="ok",
        job_dir="/tmp/job",
        latest_known_path="/tmp/job",
        reaction_key="rxn",
        selected_input_xyz="/tmp/input.xyz",
        selected_candidate_paths=("/tmp/fallback.xyz", "/tmp/skip.xyz"),
        candidate_details=(),
    )
    policy = XtbDownstreamPolicy(
        preferred_kinds=(),
        max_candidates=1,
        selected_only=False,
        allowed_kinds=("ts_guess",),
        fallback_to_selected_paths=True,
    )
    monkeypatch.setattr(xtb_adapter, "has_xyz_geometry", lambda path: path.endswith("fallback.xyz"))
    rows = xtb_adapter.select_xtb_downstream_inputs(contract, policy=policy, require_geometry=True)
    assert len(rows) == 1
    assert rows[0].artifact_path == "/tmp/fallback.xyz"


def test_runtime_edge_branches_cover_normalize_invalid_stage_and_lease_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    assert runtime._normalize_text(None) == ""

    payload = {"metadata": {}, "stages": ["bad", {"status": "completed", "task": {"status": "completed"}}]}
    monkeypatch.setattr(runtime, "load_workflow_payload", lambda workspace_dir: payload)
    monkeypatch.setattr(runtime, "workflow_has_active_downstream", lambda current_payload: False)
    assert runtime._workflow_needs_terminal_sync("/tmp/wf") is False

    records = [SimpleNamespace(workflow_id="wf1", status="running", template_name="rxn", workspace_dir="/tmp/wf1", stage_count=1)]
    state_calls: list[dict[str, Any]] = []
    journal_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(runtime, "list_workflow_registry", lambda root: records)
    monkeypatch.setattr(runtime, "reindex_workflow_registry", lambda root: records)
    monkeypatch.setattr(runtime, "advance_workflow", lambda **kwargs: {"workflow_id": "wf1", "status": "completed"})
    monkeypatch.setattr(runtime, "_workflow_needs_terminal_sync", lambda workspace_dir: False)
    monkeypatch.setattr(runtime, "write_workflow_worker_state", lambda root, **kwargs: state_calls.append(kwargs))
    monkeypatch.setattr(runtime, "append_workflow_journal_event", lambda root, **kwargs: journal_calls.append(kwargs))
    monkeypatch.setattr(runtime, "now_utc_iso", lambda: "2026-04-19T03:00:00+00:00")

    fake_datetime: Any = ModuleType("datetime")

    class _DateTime:
        @staticmethod
        def now(tz: object) -> object:
            class _Stamp:
                def __add__(self, other: object) -> "_Stamp":
                    return self

                def isoformat(self) -> str:
                    return "lease-ok"

            return _Stamp()

    class _Timedelta:
        def __init__(self, seconds: float) -> None:
            self.seconds = seconds

    class _Timezone:
        utc = object()

    fake_datetime.datetime = _DateTime
    fake_datetime.timedelta = _Timedelta
    fake_datetime.timezone = _Timezone
    monkeypatch.setitem(sys.modules, "datetime", fake_datetime)
    result = runtime.advance_workflow_registry_once(workflow_root=tmp_path / "root_ok", lease_seconds=5)
    assert result["worker_session_id"].startswith("wf_worker")
    assert state_calls[0]["lease_expires_at"] == "lease-ok"

    class _BrokenDateTime:
        @staticmethod
        def now(tz: object) -> object:
            raise RuntimeError("boom")

    fake_datetime.datetime = _BrokenDateTime
    runtime.advance_workflow_registry_once(workflow_root=tmp_path / "root_fail", lease_seconds=5)
    assert state_calls[-2]["lease_expires_at"] == ""


def test_registry_edge_branches_cover_invalid_inputs_and_direct_file_matching(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    assert registry._coerce_counts("bad") == {}
    assert registry._coerce_counts({"": 1, "ok": "2", "bad": "x"}) == {"ok": 2}

    monkeypatch.delenv("CHEM_FLOW_NOTIFY_EVENT_TYPES", raising=False)
    assert registry._notification_event_types_from_env()
    monkeypatch.setenv("CHEM_FLOW_NOTIFY_DISABLED", "yes")
    assert registry._journal_notification_enabled("workflow_status_changed") is False
    monkeypatch.delenv("CHEM_FLOW_NOTIFY_DISABLED", raising=False)

    sent: list[str] = []
    monkeypatch.setattr(registry, "_journal_notification_enabled", lambda event_type: True)

    class _TelegramTransport:
        def send_text(self, text: str) -> None:
            sent.append(text)

    monkeypatch.setattr(registry, "_telegram_transport_from_env", lambda: _TelegramTransport())
    registry._maybe_notify_journal_event({"event_type": "worker_started"}, tmp_path)
    assert sent and "worker_started" in sent[0]

    class _BrokenTelegramTransport:
        def send_text(self, text: str) -> None:
            raise RuntimeError("boom")

    monkeypatch.setattr(registry, "_telegram_transport_from_env", lambda: _BrokenTelegramTransport())
    registry._maybe_notify_journal_event({"event_type": "worker_started"}, tmp_path)

    reg_path = registry._registry_path(tmp_path)
    reg_path.parent.mkdir(parents=True, exist_ok=True)
    reg_path.write_text("{broken", encoding="utf-8")
    assert registry._load_records(tmp_path) == []
    reg_path.write_text(json.dumps({"bad": True}), encoding="utf-8")
    assert registry._load_records(tmp_path) == []

    assert registry.list_workflow_journal(tmp_path, limit=0) == []
    state_path = registry.workflow_worker_state_path(tmp_path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text("{broken", encoding="utf-8")
    assert registry.load_workflow_worker_state(tmp_path) == {}
    state_path.write_text(json.dumps(["bad"]), encoding="utf-8")
    assert registry.load_workflow_worker_state(tmp_path) == {}

    assert registry.get_workflow_registry_record(tmp_path, "   ") is None

    workflow_file = tmp_path / "wf" / "workflow.json"
    workflow_file.parent.mkdir(parents=True, exist_ok=True)
    workflow_file.write_text("{}", encoding="utf-8")
    record = registry.WorkflowRegistryRecord(
        workflow_id="wf1",
        template_name="rxn",
        status="running",
        source_job_id="job",
        source_job_type="rxn",
        reaction_key="rxn",
        requested_at="2026-04-19T00:00:00+00:00",
        workspace_dir=str(workflow_file.parent),
        workflow_file=str(workflow_file),
    )
    monkeypatch.setattr(registry, "list_workflow_registry", lambda workflow_root: [record])
    assert registry.resolve_workflow_registry_record(tmp_path, str(workflow_file)) is record


def test_operations_wrapper_edges_cover_remaining_forwarders(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(operations, "create_conformer_screening_workflow_impl", lambda **kwargs: _record_call(calls, "conf", **kwargs))

    assert operations.create_conformer_screening_workflow(input_xyz="i.xyz", workflow_root="/tmp/root") == {"ok": "conf"}
    assert [name for name, _ in calls] == ["conf"]


def test_contract_submitter_common_state_and_xtb_contract_edges(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    task = WorkflowTask.from_raw(task_id=" t1 ", engine=" ", task_kind=" ", resource_request={"": 1, "cores": "bad"})
    assert task.engine == "unknown"
    assert task.task_kind == "task"
    assert task.resource_request == {"cores": 0}
    assert task.to_dict()["payload"] == {}
    assert task.to_dict()["enqueue_payload"] == {}
    assert task.to_dict()["submission_result"] == {}
    assert task.to_dict()["metadata"] == {}

    artifact = WorkflowArtifactRef(kind="kind", path="/tmp/x")
    assert artifact.to_dict()["metadata"] == {}

    crest_contract = CrestArtifactContract(job_id="job", mode="nci", status="done", reason="ok", job_dir="/tmp/job", latest_known_path="/tmp/job")
    assert crest_contract.to_dict()["resource_request"] == {}
    rows = to_workflow_stage_inputs(
        CrestArtifactContract(
            job_id="job",
            mode="nci",
            status="done",
            reason="ok",
            job_dir="/tmp/job",
            latest_known_path="/tmp/job",
            retained_conformer_paths=("", "/tmp/conf.xyz"),
        ),
        policy=CrestDownstreamPolicy.build(max_candidates=5),
    )
    assert len(rows) == 1
    assert rows[0].artifact_path == "/tmp/conf.xyz"

    orca_contract = OrcaArtifactContract(run_id="run", status="done", reason="ok", state_status="done", reaction_dir="/tmp/r", latest_known_path="/tmp/r")
    assert orca_contract.to_dict()["final_result"] == {}

    candidate = XtbCandidateArtifact.from_raw({"path": "/tmp/cand.xyz", "score": "bad"})
    assert candidate.score is None
    assert candidate.to_dict()["metadata"] == {}
    assert _coerce_resource_dict("bad") == {}
    stage_input = WorkflowStageInput(
        source_job_id="job",
        source_job_type="xtb",
        reaction_key="rxn",
        selected_input_xyz="/tmp/in.xyz",
        rank=1,
        kind="candidate",
        artifact_path="/tmp/cand.xyz",
    )
    assert "score" not in stage_input.to_dict()

    assert common.normalize_text(None) == ""
    cfg = tmp_path / "bad.yaml"
    cfg.write_text("- x\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid sibling app config file"):
        common.sibling_allowed_root(str(cfg))
    with pytest.raises(ValueError, match="Invalid sibling app config file"):
        common.sibling_runtime_paths(str(cfg))

    missing_wf = tmp_path / "missing"
    with pytest.raises(FileNotFoundError, match="workflow file not found"):
        state.load_workflow_payload(missing_wf)
    monkeypatch.setattr(
        state,
        "Path",
        lambda raw: SimpleNamespace(expanduser=lambda: SimpleNamespace(resolve=lambda: (_ for _ in ()).throw(OSError("bad")))),
    )
    with pytest.raises(FileNotFoundError):
        state.resolve_workflow_workspace(target="wf1", workflow_root=None)
    monkeypatch.undo()
    assert state.iter_workflow_workspaces(tmp_path / "missing_root") == []


def test_xyz_reaction_ts_orca_stage_cli_and_mcp_edges(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    class _Pattern:
        def search(self, text: str) -> object:
            class _Match:
                def group(self, index: int) -> str:
                    return "bad"

            return _Match()

    monkeypatch.setattr(xyz_utils, "_ENERGY_PATTERNS", (_Pattern(),))
    assert xyz_utils._parse_energy("energy: bad") is None
    assert xyz_utils._line_has_xyz_tokens("H 0.0") is False

    class _BrokenResolved:
        def exists(self) -> bool:
            return True

        def is_file(self) -> bool:
            return True

        def read_text(self, encoding: str = "utf-8", errors: str = "ignore") -> str:
            raise OSError("boom")

    monkeypatch.setattr(xyz_utils, "Path", lambda raw: SimpleNamespace(expanduser=lambda: SimpleNamespace(resolve=lambda: _BrokenResolved())))
    assert xyz_utils.load_xyz_frames("/tmp/x.xyz") == ()
    monkeypatch.undo()

    xyz_path = tmp_path / "one.xyz"
    _write_xyz(xyz_path, "energy: -1.25")
    frame, meta = xyz_utils.choose_orca_geometry_frame(xyz_path)
    assert frame is not None
    assert meta["selected_frame_energy"] == -1.25

    contract = XtbArtifactContract(
        job_id="job",
        job_type="path_search",
        status="completed",
        reason="ok",
        job_dir="/tmp/job",
        latest_known_path="/tmp/job",
        reaction_key="rxn",
        selected_input_xyz="/tmp/in.xyz",
        selected_candidate_paths=(),
        candidate_details=(),
    )
    monkeypatch.setattr(reaction_ts_search, "timestamped_token", lambda prefix: f"{prefix}_x")
    assert reaction_ts_search._workflow_id(contract) == "wf_reaction_ts_x"
    assert "did not produce a ts_guess candidate" in reaction_ts_search._reaction_ts_guess_error(contract)

    bad_contract = XtbArtifactContract(
        job_id="job",
        job_type="path_search",
        status="completed",
        reason="ok",
        job_dir="/tmp/job",
        latest_known_path="/tmp/job",
        reaction_key="rxn",
        selected_input_xyz="/tmp/in.xyz",
        selected_candidate_paths=(),
        candidate_details=(
            XtbCandidateArtifact(rank=1, kind="ts_guess", path="/tmp/bad.xyz", selected=True),
        ),
    )
    monkeypatch.setattr(
        reaction_ts_search,
        "choose_orca_geometry_frame",
        lambda path, candidate_kind: (None, {"selection_reason": "ts_guess_requires_single_frame"}),
    )
    assert "not a single-geometry XYZ file" in reaction_ts_search._reaction_ts_guess_error(bad_contract)
    monkeypatch.setattr(
        reaction_ts_search,
        "choose_orca_geometry_frame",
        lambda path, candidate_kind: (None, {"selection_reason": "invalid_or_empty_xyz"}),
    )
    assert "empty or not a valid XYZ geometry" in reaction_ts_search._reaction_ts_guess_error(bad_contract)

    with pytest.raises(FileNotFoundError, match="xTB candidate artifact not found"):
        reaction_ts_search._materialize_orca_stage(
            workspace_dir=tmp_path,
            index=1,
            candidate=WorkflowStageInput(
                source_job_id="job",
                source_job_type="path_search",
                reaction_key="rxn",
                selected_input_xyz="/tmp/in.xyz",
                rank=1,
                kind="ts_guess",
                artifact_path=str(tmp_path / "missing.xyz"),
                selected=True,
                metadata={},
            ),
            contract=XtbArtifactContract(
                job_id="job",
                job_type="path_search",
                status="completed",
                reason="ok",
                job_dir="/tmp/job",
                latest_known_path="/tmp/job",
                reaction_key="rxn",
                selected_input_xyz="/tmp/in.xyz",
                selected_candidate_paths=(),
                candidate_details=(),
            ),
            orca_payload=reaction_ts_search.OrcaStagePayload(
                stage_id="s1",
                engine="orca",
                task_kind="optts_freq",
                selected_input_xyz="/tmp/in.xyz",
                selected_input_label="in.xyz",
                source_job_id="job",
                source_job_type="path_search",
                reaction_key="rxn",
                workflow_id="wf",
                template_name="reaction_ts_search",
                resource_request={},
                metadata={},
            ),
            route_line="! OptTS",
            charge=0,
            multiplicity=1,
            max_cores=1,
            max_memory_gb=1,
        )

    with pytest.raises(FileNotFoundError, match="ORCA stage source artifact not found"):
        orca_stage_utils.materialize_orca_stage(
            workspace_dir=tmp_path,
            stage_root_name="stage",
            stage_key="key",
            source_artifact_path=str(tmp_path / "missing.xyz"),
            candidate_kind="candidate",
            route_line="! Opt",
            charge=0,
            multiplicity=1,
            max_cores=1,
            max_memory_gb=1,
        )

    assert cli._normalize_text(None) == ""
    monkeypatch.setattr(sys, "argv", ["chem_flow", "--help"])
    monkeypatch.delitem(sys.modules, "chemstack.flow.cli", raising=False)
    with pytest.raises(SystemExit) as cli_exit:
        runpy.run_module("chemstack.flow.cli", run_name="__main__")
    assert cli_exit.value.code == 0
