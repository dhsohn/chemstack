from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

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


def _disable_tracking_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orca_adapter, "_tracked_contract_payload", lambda **kwargs: None)
    monkeypatch.setattr(orca_adapter, "_tracked_runtime_context", lambda **kwargs: None)
    monkeypatch.setattr(orca_adapter, "_tracked_artifact_context", lambda **kwargs: (None, None, {}, {}, {}))


def test_load_orca_artifact_contract_short_circuits_on_tracked_payload(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "run_id": " run_payload_1 ",
        "status": " completed ",
        "reason": " normal_termination ",
        "state_status": " completed ",
        "reaction_dir": f" {tmp_path / 'rxn_payload'} ",
        "latest_known_path": f" {tmp_path / 'outputs' / 'run_payload_1'} ",
        "organized_output_dir": f" {tmp_path / 'outputs' / 'run_payload_1'} ",
        "optimized_xyz_path": f" {tmp_path / 'outputs' / 'run_payload_1' / 'final.xyz'} ",
        "queue_id": " q_payload_1 ",
        "queue_status": " COMPLETED ",
        "cancel_requested": "yes",
        "selected_inp": f" {tmp_path / 'outputs' / 'run_payload_1' / 'rxn.inp'} ",
        "selected_input_xyz": f" {tmp_path / 'outputs' / 'run_payload_1' / 'rxn.xyz'} ",
        "analyzer_status": " completed ",
        "completed_at": " 2026-04-19T00:10:00+00:00 ",
        "last_out_path": f" {tmp_path / 'outputs' / 'run_payload_1' / 'rxn.out'} ",
        "run_state_path": f" {tmp_path / 'outputs' / 'run_payload_1' / 'run_state.json'} ",
        "report_json_path": f" {tmp_path / 'outputs' / 'run_payload_1' / 'run_report.json'} ",
        "report_md_path": f" {tmp_path / 'outputs' / 'run_payload_1' / 'run_report.md'} ",
        "attempt_count": "2",
        "max_retries": "3",
        "attempts": [{"attempt_number": 1, "analyzer_status": "completed"}, "skip"],
        "final_result": {"reason": "normal_termination"},
        "resource_request": {"max_cores": "8", "max_memory_gb": "16"},
        "resource_actual": {"max_cores": "6", "max_memory_gb": "12"},
    }

    monkeypatch.setattr(orca_adapter, "_tracked_contract_payload", lambda **kwargs: payload)
    monkeypatch.setattr(
        orca_adapter,
        "_tracked_runtime_context",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("tracked runtime fallback should not run")),
    )
    monkeypatch.setattr(
        orca_adapter,
        "_resolve_job_dir",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("job-location fallback should not run")),
    )

    contract = orca_adapter.load_orca_artifact_contract(
        target="job_payload_1",
        orca_allowed_root=tmp_path / "orca_runs",
        orca_organized_root=tmp_path / "orca_outputs",
    )

    assert contract.run_id == "run_payload_1"
    assert contract.status == "completed"
    assert contract.queue_status == "completed"
    assert contract.cancel_requested is True
    assert contract.attempt_count == 2
    assert contract.max_retries == 3
    assert contract.attempts == ({"attempt_number": 1, "analyzer_status": "completed"},)
    assert contract.resource_request == {"max_cores": 8, "max_memory_gb": 16}
    assert contract.resource_actual == {"max_cores": 6, "max_memory_gb": 12}


def test_load_orca_artifact_contract_falls_back_from_queue_stub_to_organized_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowed_root = tmp_path / "orca_runs"
    organized_root = tmp_path / "orca_outputs"
    stub_dir = allowed_root / "rxn_stub"
    organized_dir = organized_root / "opt" / "H2" / "run_queue_fallback"
    stub_dir.mkdir(parents=True)
    organized_dir.mkdir(parents=True)

    inp = organized_dir / "rxn.inp"
    xyz = organized_dir / "rxn.xyz"
    out = organized_dir / "rxn.out"
    inp.write_text("! Opt\n* xyzfile 0 1 rxn.xyz\n", encoding="utf-8")
    xyz.write_text("2\ncomment\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    out.write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")

    _write_json(
        allowed_root / "queue.json",
        [
            {
                "queue_id": "q_queue_fallback",
                "task_id": "job_queue_fallback",
                "run_id": "run_queue_fallback",
                "reaction_dir": str(stub_dir),
                "status": "running",
                "cancel_requested": False,
            }
        ],
    )
    _write_json(
        organized_dir / "run_state.json",
        {
            "run_id": "run_queue_fallback",
            "reaction_dir": str(organized_dir),
            "selected_inp": str(inp),
            "status": "completed",
            "attempts": [],
            "final_result": {
                "status": "completed",
                "analyzer_status": "completed",
                "reason": "normal_termination",
                "completed_at": "2026-04-19T00:20:00+00:00",
                "last_out_path": str(out),
            },
        },
    )
    _write_json(
        organized_dir / "run_report.json",
        {
            "run_id": "run_queue_fallback",
            "status": "completed",
            "selected_inp": str(inp),
            "final_result": {
                "status": "completed",
                "analyzer_status": "completed",
                "reason": "normal_termination",
                "completed_at": "2026-04-19T00:20:00+00:00",
                "last_out_path": str(out),
            },
        },
    )
    _write_jsonl(
        organized_root / "index" / "records.jsonl",
        [
            {
                "run_id": "run_queue_fallback",
                "organized_path": "opt/H2/run_queue_fallback",
            }
        ],
    )

    _disable_tracking_helpers(monkeypatch)

    contract = orca_adapter.load_orca_artifact_contract(
        target="job_queue_fallback",
        orca_allowed_root=allowed_root,
        orca_organized_root=organized_root,
    )

    assert contract.status == "completed"
    assert contract.queue_id == "q_queue_fallback"
    assert contract.queue_status == "running"
    assert contract.run_id == "run_queue_fallback"
    assert contract.reaction_dir == str(organized_dir.resolve())
    assert contract.latest_known_path == str(organized_dir.resolve())
    assert contract.organized_output_dir == str(organized_dir.resolve())
    assert contract.run_state_path == str((organized_dir / "run_state.json").resolve())
    assert contract.report_json_path == str((organized_dir / "run_report.json").resolve())
    assert contract.selected_inp == str(inp.resolve())
    assert contract.selected_input_xyz == str(xyz.resolve())


def test_load_orca_artifact_contract_resolves_selected_input_and_prefers_last_out_xyz(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowed_root = tmp_path / "orca_runs"
    run_dir = allowed_root / "rxn_paths"
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
            "run_id": "run_paths_1",
            "reaction_dir": str(run_dir),
            "status": "completed",
            "final_result": {
                "status": "completed",
                "analyzer_status": "completed",
                "reason": "normal_termination",
                "last_out_path": "final.out",
            },
        },
    )
    _write_json(
        allowed_root / "job_locations.json",
        [
            {
                "job_id": "job_paths_1",
                "app_name": "orca_auto",
                "job_type": "orca_opt",
                "status": "completed",
                "original_run_dir": str(run_dir),
                "molecule_key": "H2",
                "selected_input_xyz": "job_step.inp",
                "organized_output_dir": "",
                "latest_known_path": str(run_dir),
                "resource_request": {},
                "resource_actual": {},
            }
        ],
    )

    _disable_tracking_helpers(monkeypatch)

    contract = orca_adapter.load_orca_artifact_contract(
        target="job_paths_1",
        orca_allowed_root=allowed_root,
    )

    assert contract.selected_inp == str(inp.resolve())
    assert contract.selected_input_xyz == str(source_xyz.resolve())
    assert contract.last_out_path == str(final_out.resolve())
    assert contract.optimized_xyz_path == str(final_xyz.resolve())


@pytest.mark.parametrize(
    ("queue_request", "queue_actual", "record_request", "record_actual", "expected_request", "expected_actual"),
    [
        (
            {"max_cores": "8", "max_memory_gb": "16"},
            None,
            {"max_cores": 4},
            {"max_cores": "6", "max_memory_gb": "12"},
            {"max_cores": 8, "max_memory_gb": 16},
            {"max_cores": 6, "max_memory_gb": 12},
        ),
        (
            {"max_cores": "10"},
            None,
            {},
            {},
            {"max_cores": 10},
            {"max_cores": 10},
        ),
    ],
)
def test_load_orca_artifact_contract_propagates_resource_request_and_actual(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    queue_request: dict[str, object],
    queue_actual: dict[str, object] | None,
    record_request: dict[str, object],
    record_actual: dict[str, object],
    expected_request: dict[str, int],
    expected_actual: dict[str, int],
) -> None:
    allowed_root = tmp_path / "orca_runs"
    run_dir = allowed_root / "rxn_resources"
    run_dir.mkdir(parents=True)

    _write_json(
        run_dir / "run_state.json",
        {
            "run_id": "run_resources_1",
            "reaction_dir": str(run_dir),
            "status": "running",
        },
    )
    _write_json(
        allowed_root / "queue.json",
        [
            {
                "queue_id": "q_resources_1",
                "task_id": "job_resources_1",
                "run_id": "run_resources_1",
                "reaction_dir": str(run_dir),
                "status": "pending",
                "cancel_requested": False,
                "resource_request": queue_request,
                "resource_actual": queue_actual,
            }
        ],
    )
    _write_json(
        allowed_root / "job_locations.json",
        [
            {
                "job_id": "job_resources_1",
                "app_name": "orca_auto",
                "job_type": "orca_opt",
                "status": "queued",
                "original_run_dir": str(run_dir),
                "molecule_key": "H2",
                "selected_input_xyz": "",
                "organized_output_dir": "",
                "latest_known_path": str(run_dir),
                "resource_request": record_request,
                "resource_actual": record_actual,
            }
        ],
    )

    _disable_tracking_helpers(monkeypatch)

    contract = orca_adapter.load_orca_artifact_contract(
        target="job_resources_1",
        orca_allowed_root=allowed_root,
    )

    assert contract.resource_request == expected_request
    assert contract.resource_actual == expected_actual
