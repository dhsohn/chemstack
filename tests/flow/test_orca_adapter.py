from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow.adapters.orca import load_orca_artifact_contract


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(item, ensure_ascii=True) for item in records) + "\n",
        encoding="utf-8",
    )


def test_load_orca_artifact_contract_prefers_tracking_record_by_job_id(tmp_path: Path) -> None:
    allowed_root = tmp_path / "orca_runs"
    organized_root = tmp_path / "orca_outputs"
    original_dir = allowed_root / "rxn_original"
    organized_dir = organized_root / "opt" / "H2" / "run_hist_1"
    original_dir.mkdir(parents=True)
    organized_dir.mkdir(parents=True)

    inp = organized_dir / "rxn.inp"
    inp.write_text("! Opt\n* xyzfile 0 1 rxn.xyz\n", encoding="utf-8")
    xyz = organized_dir / "rxn.xyz"
    xyz.write_text("2\ncomment\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    out = organized_dir / "rxn.out"
    out.write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")

    _write_json(
        organized_dir / "run_state.json",
        {
            "job_id": "job_hist_1",
            "run_id": "run_hist_1",
            "reaction_dir": str(organized_dir),
            "selected_inp": str(inp),
            "status": "completed",
            "attempts": [],
            "final_result": {
                "status": "completed",
                "analyzer_status": "completed",
                "reason": "normal_termination",
                "completed_at": "2026-04-19T00:00:00+00:00",
                "last_out_path": str(out),
            },
        },
    )
    _write_json(
        organized_dir / "run_report.json",
        {
            "job_id": "job_hist_1",
            "run_id": "run_hist_1",
            "status": "completed",
            "selected_inp": str(inp),
            "final_result": {
                "status": "completed",
                "analyzer_status": "completed",
                "reason": "normal_termination",
                "completed_at": "2026-04-19T00:00:00+00:00",
                "last_out_path": str(out),
            },
        },
    )
    _write_json(
        original_dir / "organized_ref.json",
        {
            "job_id": "job_hist_1",
            "run_id": "run_hist_1",
            "original_run_dir": str(original_dir),
            "organized_output_dir": str(organized_dir),
            "selected_inp": str(inp),
            "selected_input_xyz": str(xyz),
        },
    )
    _write_json(
        allowed_root / "job_locations.json",
        [
            {
                "job_id": "job_hist_1",
                "app_name": "orca_auto",
                "job_type": "orca_opt",
                "status": "completed",
                "original_run_dir": str(original_dir),
                "molecule_key": "H2",
                "selected_input_xyz": str(inp),
                "organized_output_dir": str(organized_dir),
                "latest_known_path": str(organized_dir),
                "resource_request": {"max_cores": 8, "max_memory_gb": 16},
                "resource_actual": {"max_cores": 8, "max_memory_gb": 16},
            }
        ],
    )

    contract = load_orca_artifact_contract(
        target="job_hist_1",
        orca_allowed_root=allowed_root,
        orca_organized_root=organized_root,
    )

    assert contract.status == "completed"
    assert contract.run_id == "run_hist_1"
    assert contract.reaction_dir == str(organized_dir.resolve())
    assert contract.latest_known_path == str(organized_dir.resolve())
    assert contract.organized_output_dir == str(organized_dir.resolve())
    assert contract.selected_inp == str(inp.resolve())
    assert contract.selected_input_xyz == str(xyz.resolve())
    assert contract.last_out_path == str(out.resolve())


def test_load_orca_artifact_contract_matches_queue_task_id(tmp_path: Path) -> None:
    allowed_root = tmp_path / "orca_runs"
    organized_root = tmp_path / "orca_outputs"
    reaction_dir = allowed_root / "rxn_queue"
    reaction_dir.mkdir(parents=True)
    inp = reaction_dir / "rxn.inp"
    inp.write_text("! Opt\n* xyzfile 0 1 rxn.xyz\n", encoding="utf-8")

    _write_json(
        allowed_root / "queue.json",
        [
            {
                "queue_id": "q_123",
                "task_id": "job_q_123",
                "reaction_dir": str(reaction_dir),
                "status": "pending",
                "cancel_requested": False,
            }
        ],
    )
    _write_json(
        allowed_root / "job_locations.json",
        [
            {
                "job_id": "job_q_123",
                "app_name": "orca_auto",
                "job_type": "orca_opt",
                "status": "queued",
                "original_run_dir": str(reaction_dir),
                "molecule_key": "unknown",
                "selected_input_xyz": str(inp),
                "organized_output_dir": "",
                "latest_known_path": str(reaction_dir),
                "resource_request": {"max_cores": 8},
                "resource_actual": {"max_cores": 8},
            }
        ],
    )

    contract = load_orca_artifact_contract(
        target="job_q_123",
        orca_allowed_root=allowed_root,
        orca_organized_root=organized_root,
    )

    assert contract.status == "queued"
    assert contract.queue_id == "q_123"
    assert contract.queue_status == "pending"
    assert contract.reaction_dir == str(reaction_dir.resolve())
    assert contract.latest_known_path == str(reaction_dir.resolve())
    assert contract.selected_inp == str(inp.resolve())


def test_load_orca_artifact_contract_preserves_legacy_records_jsonl_fallback(tmp_path: Path) -> None:
    allowed_root = tmp_path / "orca_runs"
    organized_root = tmp_path / "orca_outputs"
    organized_dir = organized_root / "opt" / "H2" / "run_legacy_1"
    organized_dir.mkdir(parents=True)
    inp = organized_dir / "rxn.inp"
    inp.write_text("! Opt\n* xyzfile 0 1 rxn.xyz\n", encoding="utf-8")
    xyz = organized_dir / "rxn.xyz"
    xyz.write_text("2\ncomment\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")

    _write_json(
        organized_dir / "run_state.json",
        {
            "run_id": "run_legacy_1",
            "reaction_dir": str(organized_dir),
            "selected_inp": str(inp),
            "status": "completed",
            "attempts": [],
            "final_result": {
                "status": "completed",
                "analyzer_status": "completed",
                "reason": "normal_termination",
                "completed_at": "2026-04-19T00:00:00+00:00",
                "last_out_path": str(organized_dir / "rxn.out"),
            },
        },
    )
    _write_json(
        organized_dir / "run_report.json",
        {
            "run_id": "run_legacy_1",
            "status": "completed",
        },
    )
    _write_jsonl(
        organized_root / "index" / "records.jsonl",
        [
            {
                "run_id": "run_legacy_1",
                "reaction_dir": str(organized_dir),
                "organized_path": "opt/H2/run_legacy_1",
            }
        ],
    )

    contract = load_orca_artifact_contract(
        target="run_legacy_1",
        orca_allowed_root=allowed_root,
        orca_organized_root=organized_root,
    )

    assert contract.run_id == "run_legacy_1"
    assert contract.status == "completed"
    assert contract.reaction_dir == str(organized_dir.resolve())
    assert contract.selected_input_xyz == str(xyz.resolve())


def test_load_orca_artifact_contract_resolves_run_id_via_orca_tracking_without_records_jsonl(tmp_path: Path) -> None:
    allowed_root = tmp_path / "orca_runs"
    organized_root = tmp_path / "orca_outputs"
    original_dir = allowed_root / "rxn_original"
    organized_dir = organized_root / "opt" / "H2" / "run_hist_2"
    original_dir.mkdir(parents=True)
    organized_dir.mkdir(parents=True)

    inp = organized_dir / "rxn.inp"
    inp.write_text("! Opt\n* xyzfile 0 1 rxn.xyz\n", encoding="utf-8")
    xyz = organized_dir / "rxn.xyz"
    xyz.write_text("2\ncomment\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    out = organized_dir / "rxn.out"
    out.write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")

    _write_json(
        organized_dir / "run_state.json",
        {
            "job_id": "job_hist_2",
            "run_id": "run_hist_2",
            "reaction_dir": str(organized_dir),
            "selected_inp": str(inp),
            "status": "completed",
            "attempts": [],
            "final_result": {
                "status": "completed",
                "analyzer_status": "completed",
                "reason": "normal_termination",
                "completed_at": "2026-04-19T00:00:00+00:00",
                "last_out_path": str(out),
            },
        },
    )
    _write_json(
        organized_dir / "run_report.json",
        {
            "job_id": "job_hist_2",
            "run_id": "run_hist_2",
            "status": "completed",
            "selected_inp": str(inp),
            "final_result": {
                "status": "completed",
                "analyzer_status": "completed",
                "reason": "normal_termination",
                "completed_at": "2026-04-19T00:00:00+00:00",
                "last_out_path": str(out),
            },
        },
    )
    _write_json(
        original_dir / "organized_ref.json",
        {
            "job_id": "job_hist_2",
            "run_id": "run_hist_2",
            "original_run_dir": str(original_dir),
            "organized_output_dir": str(organized_dir),
            "selected_inp": str(inp),
            "selected_input_xyz": str(xyz),
        },
    )
    _write_json(
        allowed_root / "job_locations.json",
        [
            {
                "job_id": "job_hist_2",
                "app_name": "orca_auto",
                "job_type": "orca_opt",
                "status": "completed",
                "original_run_dir": str(original_dir),
                "molecule_key": "H2",
                "selected_input_xyz": str(inp),
                "organized_output_dir": str(organized_dir),
                "latest_known_path": str(organized_dir),
                "resource_request": {"max_cores": 8, "max_memory_gb": 16},
                "resource_actual": {"max_cores": 8, "max_memory_gb": 16},
            }
        ],
    )

    contract = load_orca_artifact_contract(
        target="run_hist_2",
        orca_allowed_root=allowed_root,
        orca_organized_root=organized_root,
    )

    assert contract.run_id == "run_hist_2"
    assert contract.status == "completed"
    assert contract.reaction_dir == str(organized_dir.resolve())
    assert contract.latest_known_path == str(organized_dir.resolve())
    assert contract.organized_output_dir == str(organized_dir.resolve())
    assert contract.selected_inp == str(inp.resolve())
    assert contract.selected_input_xyz == str(xyz.resolve())
    assert contract.last_out_path == str(out.resolve())


def test_load_orca_artifact_contract_prefers_orca_contract_payload_helper(tmp_path: Path) -> None:
    tracking_module = SimpleNamespace(
        load_orca_contract_payload=lambda *_args, **_kwargs: {
            "run_id": "run_helper_1",
            "status": "completed",
            "reason": "normal_termination",
            "state_status": "completed",
            "reaction_dir": str((tmp_path / "rxn_helper").resolve()),
            "latest_known_path": str((tmp_path / "rxn_helper").resolve()),
            "organized_output_dir": str((tmp_path / "outputs" / "run_helper_1").resolve()),
            "optimized_xyz_path": str((tmp_path / "outputs" / "run_helper_1" / "rxn.xyz").resolve()),
            "queue_id": "q_helper_1",
            "queue_status": "completed",
            "cancel_requested": False,
            "selected_inp": str((tmp_path / "outputs" / "run_helper_1" / "rxn.inp").resolve()),
            "selected_input_xyz": str((tmp_path / "outputs" / "run_helper_1" / "rxn.xyz").resolve()),
            "analyzer_status": "completed",
            "completed_at": "2026-04-19T00:10:00+00:00",
            "last_out_path": str((tmp_path / "outputs" / "run_helper_1" / "rxn.out").resolve()),
            "run_state_path": str((tmp_path / "outputs" / "run_helper_1" / "run_state.json").resolve()),
            "report_json_path": str((tmp_path / "outputs" / "run_helper_1" / "run_report.json").resolve()),
            "report_md_path": str((tmp_path / "outputs" / "run_helper_1" / "run_report.md").resolve()),
            "attempt_count": 2,
            "max_retries": 3,
            "attempts": [{"attempt_number": 1, "analyzer_status": "completed"}],
            "final_result": {"reason": "normal_termination"},
            "resource_request": {"max_cores": 8, "max_memory_gb": 16},
            "resource_actual": {"max_cores": 8, "max_memory_gb": 16},
        }
    )

    with patch("chemstack.flow.adapters.orca._orca_auto_tracking_module", return_value=tracking_module):
        contract = load_orca_artifact_contract(
            target="job_helper_1",
            orca_allowed_root=tmp_path / "orca_runs",
            orca_organized_root=tmp_path / "orca_outputs",
        )

    assert contract.run_id == "run_helper_1"
    assert contract.status == "completed"
    assert contract.queue_id == "q_helper_1"
    assert contract.attempt_count == 2
    assert contract.max_retries == 3
    assert contract.final_result["reason"] == "normal_termination"
