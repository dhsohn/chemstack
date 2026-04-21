from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.core.indexing import JobLocationRecord

from chemstack.flow.adapters import orca as orca_adapter


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(item, ensure_ascii=True) for item in records) + "\n",
        encoding="utf-8",
    )


def test_tracked_artifact_context_skips_invalid_results_and_uses_later_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_dir = tmp_path / "rxn_artifact"
    artifact_dir.mkdir()
    record = SimpleNamespace(job_id="job_artifact")

    def load_job_artifact_context(_index_root: Path, target: str) -> SimpleNamespace:
        if target == "raise":
            raise RuntimeError("broken helper")
        if target == "invalid":
            return SimpleNamespace(job_dir=None, state=["bad"], report="bad", organized_ref=0)
        return SimpleNamespace(
            job_dir=artifact_dir,
            record=record,
            state={"status": "running"},
            report={"status": "completed"},
            organized_ref={"run_id": "run_artifact"},
        )

    tracking_module = SimpleNamespace(load_job_artifact_context=load_job_artifact_context)
    monkeypatch.setattr(orca_adapter, "_orca_auto_tracking_module", lambda: tracking_module)

    job_dir, tracked_record, state, report, organized_ref = orca_adapter._tracked_artifact_context(
        index_root=tmp_path / "orca_runs",
        targets=("raise", "invalid", "good"),
    )

    assert job_dir == artifact_dir.resolve()
    assert tracked_record is record
    assert state == {"status": "running"}
    assert report == {"status": "completed"}
    assert organized_ref == {"run_id": "run_artifact"}


@pytest.mark.parametrize(
    "payload",
    [
        ["not-a-dict"],
        {},
        {"status": "   ", "queue_id": ""},
    ],
)
def test_tracked_contract_payload_rejects_invalid_returns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload: object,
) -> None:
    tracking_module = SimpleNamespace(load_orca_contract_payload=lambda *_args, **_kwargs: payload)
    monkeypatch.setattr(orca_adapter, "_orca_auto_tracking_module", lambda: tracking_module)

    assert (
        orca_adapter._tracked_contract_payload(
            index_root=tmp_path / "orca_runs",
            organized_root=tmp_path / "orca_outputs",
            target="job_invalid_payload",
            queue_id="",
            run_id="",
            reaction_dir="",
        )
        is None
    )


def test_find_queue_entry_matches_reaction_dir_from_file_target(tmp_path: Path) -> None:
    allowed_root = tmp_path / "orca_runs"
    reaction_dir = tmp_path / "rxn_queue_file"
    reaction_dir.mkdir()
    inp = reaction_dir / "job_step.inp"
    inp.write_text("! Opt\n", encoding="utf-8")

    _write_json(
        allowed_root / "queue.json",
        [
            {
                "queue_id": "q_file_target",
                "task_id": "job_file_target",
                "run_id": "run_file_target",
                "reaction_dir": str(reaction_dir),
                "status": "running",
            }
        ],
    )

    entry = orca_adapter._find_queue_entry(
        allowed_root=allowed_root,
        target=str(inp),
        queue_id="",
        run_id="",
        reaction_dir="",
    )

    assert entry is not None
    assert entry["queue_id"] == "q_file_target"


def test_find_organized_record_and_dir_resolution_support_file_targets(tmp_path: Path) -> None:
    organized_root = tmp_path / "orca_outputs"
    organized_dir = organized_root / "opt" / "H2" / "run_file_target"
    organized_dir.mkdir(parents=True)
    inp = organized_dir / "job_step.inp"
    inp.write_text("! Opt\n", encoding="utf-8")

    record: dict[str, object] = {
        "run_id": "run_file_target",
        "organized_path": "opt/H2/run_file_target",
    }
    _write_jsonl(organized_root / "index" / "records.jsonl", [record])

    found = orca_adapter._find_organized_record(
        organized_root=organized_root,
        target=str(inp),
        run_id="",
        reaction_dir="",
    )

    assert found == record
    assert orca_adapter._organized_dir_from_record(organized_root, found) == organized_dir.resolve()
    assert (
        orca_adapter._organized_dir_from_record(
            organized_root,
            {
                "reaction_dir": str(organized_dir),
                "organized_path": "ignored/by/reaction-dir",
            },
        )
        == organized_dir.resolve()
    )


def test_load_tracked_organized_ref_reads_stub_only_when_current_dir_differs(tmp_path: Path) -> None:
    stub_dir = tmp_path / "rxn_stub"
    organized_dir = tmp_path / "organized" / "run_stub"
    stub_dir.mkdir(parents=True)
    organized_dir.mkdir(parents=True)

    payload = {
        "run_id": "run_stub",
        "organized_output_dir": str(organized_dir),
        "selected_input_xyz": str(organized_dir / "source.xyz"),
    }
    _write_json(stub_dir / "organized_ref.json", payload)

    record = JobLocationRecord(
        job_id="job_stub",
        app_name="orca_auto",
        job_type="orca_opt",
        status="running",
        original_run_dir=str(stub_dir),
    )

    assert orca_adapter._load_tracked_organized_ref(record, organized_dir) == payload
    assert orca_adapter._load_tracked_organized_ref(record, stub_dir) == {}


def test_load_orca_artifact_contract_uses_runtime_context_fast_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowed_root = tmp_path / "orca_runs"
    organized_root = tmp_path / "orca_outputs"
    artifact_dir = allowed_root / "rxn_runtime"
    organized_dir = organized_root / "opt" / "H2" / "run_runtime"
    artifact_dir.mkdir(parents=True)
    organized_dir.mkdir(parents=True)

    inp = organized_dir / "job_step.inp"
    source_xyz = organized_dir / "source.xyz"
    final_out = organized_dir / "final.out"
    final_xyz = organized_dir / "final.xyz"
    inp.write_text("! Opt\n* xyzfile 0 1 source.xyz\n", encoding="utf-8")
    source_xyz.write_text("2\nsource\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    final_out.write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")
    final_xyz.write_text("2\noptimized\nH 0 0 0\nH 0 0 0.75\n", encoding="utf-8")

    _write_json(artifact_dir / "run_state.json", {"helper": "runtime"})
    _write_json(artifact_dir / "run_report.json", {"helper": "runtime"})
    (artifact_dir / "run_report.md").write_text("# runtime report\n", encoding="utf-8")

    tracked_record = SimpleNamespace(
        app_name="orca_auto",
        status="running",
        selected_input_xyz="",
        latest_known_path=str(organized_dir),
        organized_output_dir=str(organized_dir),
        original_run_dir=str(artifact_dir),
        resource_request={"max_cores": "8", "max_memory_gb": "16"},
        resource_actual={"max_cores": "6", "max_memory_gb": "12"},
    )
    state = {
        "run_id": "run_runtime",
        "status": "running",
        "selected_inp": str(inp),
        "attempts": [{"index": 2, "analyzer_status": "completed"}],
        "max_retries": "4",
    }
    report = {
        "run_id": "run_runtime",
        "attempt_count": "2",
        "final_result": {
            "status": "completed",
            "analyzer_status": "completed",
            "reason": "normal_termination",
            "completed_at": "2026-04-19T00:30:00+00:00",
            "last_out_path": str(final_out),
        },
    }
    organized_ref = {
        "run_id": "run_runtime",
        "organized_output_dir": str(organized_dir),
        "selected_input_xyz": str(source_xyz),
    }
    queue_entry = {
        "queue_id": "q_runtime",
        "status": "pending",
        "cancel_requested": False,
    }
    tracking_module = SimpleNamespace(
        load_job_runtime_context=lambda *_args, **_kwargs: SimpleNamespace(
            artifact=SimpleNamespace(
                job_dir=artifact_dir,
                record=tracked_record,
                state=state,
                report=report,
                organized_ref=organized_ref,
            ),
            queue_entry=queue_entry,
            organized_dir=organized_dir,
        )
    )

    monkeypatch.setattr(orca_adapter, "_orca_auto_tracking_module", lambda: tracking_module)
    monkeypatch.setattr(
        orca_adapter,
        "_resolve_job_dir",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("job-dir fallback should not run")),
    )
    monkeypatch.setattr(
        orca_adapter,
        "_find_queue_entry",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("queue fallback should not run")),
    )
    monkeypatch.setattr(
        orca_adapter,
        "_tracked_artifact_context",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("artifact-context fallback should not run")),
    )
    monkeypatch.setattr(
        orca_adapter,
        "_find_organized_record",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("organized-record fallback should not run")),
    )

    contract = orca_adapter.load_orca_artifact_contract(
        target="job_runtime",
        orca_allowed_root=allowed_root,
        orca_organized_root=organized_root,
    )

    assert contract.run_id == "run_runtime"
    assert contract.status == "completed"
    assert contract.reason == "normal_termination"
    assert contract.queue_id == "q_runtime"
    assert contract.queue_status == "pending"
    assert contract.reaction_dir == str(artifact_dir.resolve())
    assert contract.latest_known_path == str(organized_dir.resolve())
    assert contract.organized_output_dir == str(organized_dir.resolve())
    assert contract.selected_inp == str(inp.resolve())
    assert contract.selected_input_xyz == str(source_xyz.resolve())
    assert contract.last_out_path == str(final_out.resolve())
    assert contract.optimized_xyz_path == str(final_xyz.resolve())
    assert contract.run_state_path == str((artifact_dir / "run_state.json").resolve())
    assert contract.report_json_path == str((artifact_dir / "run_report.json").resolve())
    assert contract.report_md_path == str((artifact_dir / "run_report.md").resolve())
    assert contract.attempt_count == 2
    assert contract.max_retries == 4
    assert contract.resource_request == {"max_cores": 8, "max_memory_gb": 16}
    assert contract.resource_actual == {"max_cores": 6, "max_memory_gb": 12}


def test_load_orca_artifact_contract_falls_back_from_invalid_runtime_context_to_file_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowed_root = tmp_path / "orca_runs"
    run_dir = allowed_root / "rxn_invalid_runtime"
    run_dir.mkdir(parents=True)

    inp = run_dir / "job_step.inp"
    source_xyz = run_dir / "source.xyz"
    final_out = run_dir / "final.out"
    final_xyz = run_dir / "final.xyz"
    inp.write_text("! Opt\n* xyzfile 0 1 source.xyz\n", encoding="utf-8")
    source_xyz.write_text("2\nsource\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    final_out.write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")
    final_xyz.write_text("2\noptimized\nH 0 0 0\nH 0 0 0.75\n", encoding="utf-8")

    _write_json(
        run_dir / "run_state.json",
        {
            "run_id": "run_invalid_runtime",
            "status": "completed",
            "selected_inp": "job_step.inp",
            "final_result": {
                "status": "completed",
                "analyzer_status": "completed",
                "reason": "normal_termination",
                "last_out_path": "final.out",
            },
        },
    )
    _write_json(
        run_dir / "run_report.json",
        {
            "run_id": "run_invalid_runtime",
            "status": "completed",
        },
    )

    tracking_module = SimpleNamespace(
        load_job_runtime_context=lambda *_args, **_kwargs: SimpleNamespace(
            artifact=SimpleNamespace(
                job_dir=None,
                record=None,
                state=["bad"],
                report="bad",
                organized_ref=0,
            ),
            queue_entry="bad",
            organized_dir=None,
        )
    )
    monkeypatch.setattr(orca_adapter, "_orca_auto_tracking_module", lambda: tracking_module)
    monkeypatch.setattr(
        orca_adapter,
        "resolve_job_location",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("skip index")),
    )

    contract = orca_adapter.load_orca_artifact_contract(
        target=str(inp),
        orca_allowed_root=allowed_root,
    )

    assert contract.run_id == "run_invalid_runtime"
    assert contract.status == "completed"
    assert contract.reaction_dir == str(run_dir.resolve())
    assert contract.latest_known_path == str(run_dir.resolve())
    assert contract.selected_inp == str(inp.resolve())
    assert contract.selected_input_xyz == str(source_xyz.resolve())
    assert contract.last_out_path == str(final_out.resolve())
    assert contract.optimized_xyz_path == str(final_xyz.resolve())
    assert contract.queue_id == ""
    assert contract.queue_status == ""
