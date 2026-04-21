from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow.adapters import orca as orca_adapter


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


@pytest.mark.parametrize(
    ("queue_entry", "state", "report", "expected"),
    [
        (
            {"status": "running"},
            {"status": "running"},
            {
                "final_result": {
                    "status": "completed",
                    "analyzer_status": "completed",
                    "reason": "normal_termination",
                    "completed_at": "2026-04-19T00:10:00+00:00",
                }
            },
            ("completed", "completed", "normal_termination", "2026-04-19T00:10:00+00:00"),
        ),
        (
            {"status": "cancelled"},
            {},
            {},
            ("cancelled", "", "cancelled", ""),
        ),
        (
            {"status": "running", "cancel_requested": True},
            {},
            {},
            ("cancel_requested", "", "", ""),
        ),
        (
            {"status": "pending"},
            {},
            {},
            ("queued", "", "", ""),
        ),
        (
            {},
            {"status": "retrying"},
            {},
            ("running", "", "", ""),
        ),
        (
            {},
            {},
            {"status": "failed"},
            ("failed", "", "", ""),
        ),
        (
            {},
            {},
            {},
            ("unknown", "", "", ""),
        ),
    ],
)
def test_status_from_payloads_covers_priority_order(
    queue_entry: dict[str, object],
    state: dict[str, object],
    report: dict[str, object],
    expected: tuple[str, str, str, str],
) -> None:
    assert orca_adapter._status_from_payloads(queue_entry=queue_entry, state=state, report=report) == expected


def test_derive_selected_input_xyz_reads_xyzfile_reference(tmp_path: Path) -> None:
    inp = tmp_path / "rxn.inp"
    xyz = tmp_path / "rxn.xyz"
    xyz.write_text("2\ncomment\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    inp.write_text("! Opt\n* xyzfile 0 1 rxn.xyz\n", encoding="utf-8")

    assert orca_adapter._derive_selected_input_xyz(str(inp)) == str(xyz.resolve())


def test_prefer_orca_optimized_xyz_prefers_matching_input_stem(tmp_path: Path) -> None:
    current_dir = tmp_path / "run_dir"
    current_dir.mkdir()
    selected_inp = current_dir / "rxn.inp"
    selected_xyz = current_dir / "rxn_source.xyz"
    preferred_xyz = current_dir / "rxn.xyz"
    selected_inp.write_text("! Opt\n* xyzfile 0 1 rxn_source.xyz\n", encoding="utf-8")
    selected_xyz.write_text("2\ncomment\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    preferred_xyz.write_text("2\noptimized\nH 0 0 0\nH 0 0 0.75\n", encoding="utf-8")

    chosen = orca_adapter._prefer_orca_optimized_xyz(
        selected_inp=str(selected_inp),
        selected_input_xyz=str(selected_xyz),
        current_dir=current_dir,
        organized_dir=None,
        latest_known_path="",
        last_out_path="",
    )

    assert chosen == str(preferred_xyz.resolve())


def test_prefer_orca_optimized_xyz_falls_back_to_latest_non_source_xyz(tmp_path: Path) -> None:
    current_dir = tmp_path / "run_dir"
    current_dir.mkdir()
    source_xyz = current_dir / "source.xyz"
    source_xyz.write_text("2\nsource\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    older_xyz = current_dir / "older.xyz"
    newer_xyz = current_dir / "newer.xyz"
    older_xyz.write_text("2\nolder\nH 0 0 0\nH 0 0 0.75\n", encoding="utf-8")
    newer_xyz.write_text("2\nnewer\nH 0 0 0\nH 0 0 0.76\n", encoding="utf-8")
    older_xyz.touch()
    newer_xyz.touch()

    chosen = orca_adapter._prefer_orca_optimized_xyz(
        selected_inp="",
        selected_input_xyz=str(source_xyz),
        current_dir=current_dir,
        organized_dir=None,
        latest_known_path="",
        last_out_path="",
    )

    assert chosen == str(newer_xyz.resolve())


def test_attempt_helpers_prefer_report_values_and_coerce_attempt_rows() -> None:
    state = {
        "attempts": [
            {
                "index": 2,
                "inp_path": "/tmp/rxn.retry01.inp",
                "out_path": "/tmp/rxn.retry01.out",
                "return_code": 0,
                "analyzer_status": "completed",
                "analyzer_reason": "normal_termination",
                "markers": ["marker"],
                "patch_actions": ["patch"],
                "started_at": "2026-04-19T00:00:00+00:00",
                "ended_at": "2026-04-19T00:01:00+00:00",
            },
            "skip",
        ],
        "max_retries": 5,
    }
    report = {
        "attempt_count": "3",
        "max_retries": "7",
    }

    attempts = orca_adapter._coerce_attempts(state, report)

    assert orca_adapter._attempt_count(state, report) == 3
    assert orca_adapter._max_retries(state, report) == 7
    assert attempts == (
        {
            "index": 2,
            "attempt_number": 1,
            "inp_path": "/tmp/rxn.retry01.inp",
            "out_path": "/tmp/rxn.retry01.out",
            "return_code": 0,
            "analyzer_status": "completed",
            "analyzer_reason": "normal_termination",
            "markers": ["marker"],
            "patch_actions": ["patch"],
            "started_at": "2026-04-19T00:00:00+00:00",
            "ended_at": "2026-04-19T00:01:00+00:00",
        },
    )


@pytest.mark.parametrize(
    ("target", "queue_id", "run_id", "reaction_dir", "expected_queue_id"),
    [
        ("unused", "q_2", "", "", "q_2"),
        ("task_3", "", "", "", "q_3"),
        ("run_4", "", "", "", "q_4"),
        ("unused", "", "run_5", "", "q_5"),
        ("unused", "", "", "__TMP_RXN_6__", "q_6"),
    ],
)
def test_find_queue_entry_matches_multiple_identifier_types(
    tmp_path: Path,
    target: str,
    queue_id: str,
    run_id: str,
    reaction_dir: str,
    expected_queue_id: str,
) -> None:
    allowed_root = tmp_path / "orca_runs"
    _write_json(
        allowed_root / "queue.json",
        [
            {"queue_id": "q_1", "task_id": "task_1", "run_id": "run_1", "reaction_dir": str(tmp_path / "rxn_1")},
            {"queue_id": "q_2", "task_id": "task_2", "run_id": "run_2", "reaction_dir": str(tmp_path / "rxn_2")},
            {"queue_id": "q_3", "task_id": "task_3", "run_id": "run_3", "reaction_dir": str(tmp_path / "rxn_3")},
            {"queue_id": "q_4", "task_id": "task_4", "run_id": "run_4", "reaction_dir": str(tmp_path / "rxn_4")},
            {"queue_id": "q_5", "task_id": "task_5", "run_id": "run_5", "reaction_dir": str(tmp_path / "rxn_5")},
            {"queue_id": "q_6", "task_id": "task_6", "run_id": "run_6", "reaction_dir": str((tmp_path / "rxn_6").resolve())},
        ],
    )
    if reaction_dir == "__TMP_RXN_6__":
        reaction_dir = f" {tmp_path / 'rxn_6'} "

    entry = orca_adapter._find_queue_entry(
        allowed_root=allowed_root,
        target=target,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
    )

    assert entry is not None
    assert entry["queue_id"] == expected_queue_id


def test_load_orca_artifact_contract_rejects_non_orca_index_records(tmp_path: Path) -> None:
    allowed_root = tmp_path / "orca_runs"
    reaction_dir = allowed_root / "rxn_wrong_app"
    reaction_dir.mkdir(parents=True)
    _write_json(
        reaction_dir / "run_state.json",
        {
            "run_id": "run_wrong_app",
            "reaction_dir": str(reaction_dir),
            "status": "running",
        },
    )
    record = SimpleNamespace(
        job_id="job_wrong_app",
        app_name="crest_auto",
        job_type="orca_opt",
        status="running",
        original_run_dir=str(reaction_dir),
        molecule_key="bad",
        selected_input_xyz="",
        organized_output_dir="",
        latest_known_path=str(reaction_dir),
        resource_request={},
        resource_actual={},
    )

    orca_adapter._orca_auto_tracking_module.cache_clear()

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(orca_adapter, "_tracked_contract_payload", lambda **kwargs: None)
        monkeypatch.setattr(orca_adapter, "_tracked_runtime_context", lambda **kwargs: None)
        monkeypatch.setattr(orca_adapter, "_tracked_artifact_context", lambda **kwargs: (None, None, {}, {}, {}))
        monkeypatch.setattr(orca_adapter, "resolve_job_location", lambda index_root, target: record)

        with pytest.raises(ValueError, match="Expected chemstack_orca index record"):
            orca_adapter.load_orca_artifact_contract(
                target="job_wrong_app",
                orca_allowed_root=allowed_root,
            )
