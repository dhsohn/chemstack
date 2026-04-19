from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from core.config import AppConfig, CommonResourceConfig, PathsConfig, RuntimeConfig
import orca_auto.job_locations as job_locations_module
from orca_auto.job_locations import (
    index_root_for_cfg,
    load_job_artifact_context,
    load_job_artifacts,
    load_orca_contract_payload,
    load_job_runtime_context,
    record_from_artifacts,
    resolve_latest_job_dir,
    upsert_job_record,
)


def _load_job_locations(root: Path) -> list[dict[str, object]]:
    path = root / "job_locations.json"
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    return raw if isinstance(raw, list) else []


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(item, ensure_ascii=True) for item in records) + "\n",
        encoding="utf-8",
    )


def _make_cfg(root: Path) -> AppConfig:
    fake_orca = root / "fake_orca"
    fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")
    fake_orca.chmod(0o755)
    return AppConfig(
        runtime=RuntimeConfig(
            allowed_root=str(root / "runs"),
            organized_root=str(root / "outputs"),
        ),
        paths=PathsConfig(orca_executable=str(fake_orca)),
        resources=CommonResourceConfig(max_cores_per_task=8, max_memory_gb_per_task=16),
    )


def test_upsert_job_record_writes_allowed_root_index_and_resolves_latest_dir() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        cfg = _make_cfg(root)
        allowed_root = Path(cfg.runtime.allowed_root)
        allowed_root.mkdir(parents=True)
        job_dir = allowed_root / "rxn_a"
        job_dir.mkdir()
        inp = job_dir / "rxn.inp"
        inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
        state: dict[str, object] = {
            "job_id": "job_live_1",
            "run_id": "run_live_1",
            "reaction_dir": str(job_dir),
            "selected_inp": str(inp),
            "status": "queued",
            "attempts": [],
            "final_result": None,
        }
        (job_dir / "run_state.json").write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")
        (job_dir / "run_report.json").write_text(json.dumps({"job_id": "job_live_1", "status": "queued"}), encoding="utf-8")

        record = upsert_job_record(
            cfg,
            job_id="job_live_1",
            status="queued",
            job_dir=job_dir,
            job_type="opt",
            selected_input_xyz=str(inp),
            molecule_key="H2",
            resource_request={"max_cores": 8, "max_memory_gb": 16},
            resource_actual={"max_cores": 8, "max_memory_gb": 16},
        )

        assert record.job_id == "job_live_1"
        assert index_root_for_cfg(cfg) == allowed_root.resolve()
        assert resolve_latest_job_dir(index_root_for_cfg(cfg), "job_live_1") == job_dir.resolve()
        loaded = _load_job_locations(index_root_for_cfg(cfg))
        assert len(loaded) == 1
        assert loaded[0]["job_id"] == "job_live_1"
        assert loaded[0]["original_run_dir"] == str(job_dir.resolve())
        job_path, loaded_state, loaded_report = load_job_artifacts(index_root_for_cfg(cfg), "job_live_1")
        assert job_path == job_dir.resolve()
        assert loaded_state is not None and loaded_state["job_id"] == "job_live_1"
        assert loaded_report is not None and loaded_report["job_id"] == "job_live_1"


def test_record_from_artifacts_uses_run_id_fallback_and_organized_ref() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        original_dir = root / "runs" / "rxn_b"
        original_dir.mkdir(parents=True)
        organized_dir = root / "outputs" / "opt" / "H2" / "run_hist_1"
        organized_dir.mkdir(parents=True)
        selected_inp = organized_dir / "rxn.inp"
        selected_inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")

        state: dict[str, object] = {
            "run_id": "run_hist_1",
            "status": "completed",
            "selected_inp": str(selected_inp),
            "attempts": [],
            "final_result": None,
        }
        organized_ref = {
            "run_id": "run_hist_1",
            "original_run_dir": str(original_dir),
            "organized_output_dir": str(organized_dir),
            "status": "completed",
            "job_type": "opt",
            "selected_inp": str(selected_inp),
            "molecule_key": "H2",
            "resource_request": {"max_cores": 8, "max_memory_gb": 16},
            "resource_actual": {"max_cores": 8, "max_memory_gb": 16},
        }

        record = record_from_artifacts(
            job_dir=original_dir,
            state=state,
            report=None,
            organized_ref=organized_ref,
        )

        assert record is not None
        assert record.job_id == "run_hist_1"
        assert record.original_run_dir == str(original_dir.resolve())
        assert record.organized_output_dir == str(organized_dir.resolve())
        assert record.latest_known_path == str(organized_dir.resolve())
        assert record.job_type == "orca_opt"
        assert record.molecule_key == "H2"


def test_resolve_latest_job_dir_and_load_job_artifacts_cover_job_and_path_targets() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        allowed_root = root / "runs"
        organized_dir = root / "outputs" / "opt" / "H2" / "job_hist_1"
        original_dir = allowed_root / "rxn_hist_1"
        allowed_root.mkdir()
        original_dir.mkdir()
        organized_dir.mkdir(parents=True)

        inp = organized_dir / "rxn.inp"
        inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
        report = {"job_id": "job_hist_1", "run_id": "run_hist_1", "status": "completed"}
        state: dict[str, object] = {
            "job_id": "job_hist_1",
            "run_id": "run_hist_1",
            "reaction_dir": str(organized_dir),
            "selected_inp": str(inp),
            "status": "completed",
            "attempts": [],
            "final_result": None,
        }
        organized_ref = {
            "job_id": "job_hist_1",
            "run_id": "run_hist_1",
            "original_run_dir": str(original_dir),
            "organized_output_dir": str(organized_dir),
            "selected_inp": str(inp),
            "selected_input_xyz": str(inp),
        }

        _write_json(organized_dir / "run_state.json", state)
        _write_json(organized_dir / "run_report.json", report)
        _write_json(original_dir / "organized_ref.json", organized_ref)
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

        for target in ("job_hist_1", "run_hist_1", str(original_dir), str(organized_dir)):
            assert resolve_latest_job_dir(allowed_root, target) == organized_dir.resolve()
            job_path, loaded_state, loaded_report = load_job_artifacts(allowed_root, target)
            assert job_path == organized_dir.resolve()
            assert loaded_state is not None and loaded_state["run_id"] == "run_hist_1"
            assert loaded_report is not None and loaded_report["job_id"] == "job_hist_1"


def test_load_job_artifacts_follows_organized_ref_when_index_lookup_is_missing() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        allowed_root = root / "runs"
        original_dir = allowed_root / "rxn_hist_2"
        organized_dir = root / "outputs" / "opt" / "H2" / "job_hist_2"
        allowed_root.mkdir()
        original_dir.mkdir()
        organized_dir.mkdir(parents=True)

        state: dict[str, object] = {
            "job_id": "job_hist_2",
            "run_id": "run_hist_2",
            "reaction_dir": str(organized_dir),
            "selected_inp": str(organized_dir / "rxn.inp"),
            "status": "completed",
            "attempts": [],
            "final_result": None,
        }
        report = {"job_id": "job_hist_2", "run_id": "run_hist_2", "status": "completed"}
        organized_ref = {
            "job_id": "job_hist_2",
            "run_id": "run_hist_2",
            "original_run_dir": str(original_dir),
            "organized_output_dir": str(organized_dir),
        }

        _write_json(organized_dir / "run_state.json", state)
        _write_json(organized_dir / "run_report.json", report)
        _write_json(original_dir / "organized_ref.json", organized_ref)

        assert resolve_latest_job_dir(allowed_root, str(original_dir)) == organized_dir.resolve()
        job_path, loaded_state, loaded_report = load_job_artifacts(allowed_root, str(original_dir))
        assert job_path == organized_dir.resolve()
        assert loaded_state is not None and loaded_state["job_id"] == "job_hist_2"
        assert loaded_report is not None and loaded_report["run_id"] == "run_hist_2"


def test_load_job_artifact_context_includes_record_and_original_stub_for_run_id_target() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        allowed_root = root / "runs"
        original_dir = allowed_root / "rxn_hist_3"
        organized_dir = root / "outputs" / "opt" / "H2" / "job_hist_3"
        allowed_root.mkdir()
        original_dir.mkdir()
        organized_dir.mkdir(parents=True)

        inp = organized_dir / "rxn.inp"
        inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
        state: dict[str, object] = {
            "job_id": "job_hist_3",
            "run_id": "run_hist_3",
            "reaction_dir": str(organized_dir),
            "selected_inp": str(inp),
            "status": "completed",
            "attempts": [],
            "final_result": None,
        }
        report = {"job_id": "job_hist_3", "run_id": "run_hist_3", "status": "completed"}
        organized_ref = {
            "job_id": "job_hist_3",
            "run_id": "run_hist_3",
            "original_run_dir": str(original_dir),
            "organized_output_dir": str(organized_dir),
            "selected_inp": str(inp),
        }

        _write_json(organized_dir / "run_state.json", state)
        _write_json(organized_dir / "run_report.json", report)
        _write_json(original_dir / "organized_ref.json", organized_ref)
        _write_json(
            allowed_root / "job_locations.json",
            [
                {
                    "job_id": "job_hist_3",
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

        context = load_job_artifact_context(allowed_root, "run_hist_3")

        assert context.record is not None
        assert context.record.job_id == "job_hist_3"
        assert context.job_dir == organized_dir.resolve()
        assert context.state is not None and context.state["run_id"] == "run_hist_3"
        assert context.report is not None and context.report["job_id"] == "job_hist_3"
        assert context.organized_ref is not None
        assert context.organized_ref["original_run_dir"] == str(original_dir)


def test_load_job_runtime_context_exposes_queue_entry_and_organized_refresh() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        allowed_root = root / "runs"
        organized_root = root / "outputs"
        original_dir = allowed_root / "rxn_hist_4"
        organized_dir = organized_root / "opt" / "H2" / "job_hist_4"
        allowed_root.mkdir()
        original_dir.mkdir()
        organized_dir.mkdir(parents=True)

        inp = organized_dir / "rxn.inp"
        inp.write_text("! Opt\n* xyzfile 0 1 rxn.xyz\n", encoding="utf-8")
        _write_json(
            organized_dir / "run_state.json",
            {
                "job_id": "job_hist_4",
                "run_id": "run_hist_4",
                "reaction_dir": str(organized_dir),
                "selected_inp": str(inp),
                "status": "completed",
                "attempts": [],
                "final_result": None,
            },
        )
        _write_json(
            organized_dir / "run_report.json",
            {
                "job_id": "job_hist_4",
                "run_id": "run_hist_4",
                "status": "completed",
            },
        )
        _write_json(
            original_dir / "organized_ref.json",
            {
                "job_id": "job_hist_4",
                "run_id": "run_hist_4",
                "original_run_dir": str(original_dir),
                "organized_output_dir": str(organized_dir),
                "selected_inp": str(inp),
            },
        )
        _write_json(
            allowed_root / "queue.json",
            [
                {
                    "queue_id": "q_hist_4",
                    "task_id": "job_hist_4",
                    "run_id": "run_hist_4",
                    "reaction_dir": str(original_dir),
                    "status": "completed",
                    "cancel_requested": False,
                }
            ],
        )
        _write_json(
            allowed_root / "job_locations.json",
            [
                {
                    "job_id": "job_hist_4",
                    "app_name": "orca_auto",
                    "job_type": "orca_opt",
                    "status": "completed",
                    "original_run_dir": str(original_dir),
                    "molecule_key": "H2",
                    "selected_input_xyz": str(inp),
                    "organized_output_dir": "",
                    "latest_known_path": str(original_dir),
                    "resource_request": {"max_cores": 8, "max_memory_gb": 16},
                    "resource_actual": {"max_cores": 8, "max_memory_gb": 16},
                }
            ],
        )
        _write_jsonl(
            organized_root / "index" / "records.jsonl",
            [
                {
                    "run_id": "run_hist_4",
                    "reaction_dir": str(organized_dir),
                    "organized_path": "opt/H2/job_hist_4",
                }
            ],
        )

        context = load_job_runtime_context(
            allowed_root,
            "job_hist_4",
            organized_root=organized_root,
        )

        assert context.queue_entry is not None
        assert context.queue_entry["queue_id"] == "q_hist_4"
        assert context.organized_dir == organized_dir.resolve()
        assert context.artifact.job_dir == organized_dir.resolve()
        assert context.artifact.state is not None and context.artifact.state["run_id"] == "run_hist_4"
        assert context.artifact.report is not None and context.artifact.report["job_id"] == "job_hist_4"
        assert context.artifact.organized_ref is not None
        assert context.artifact.organized_ref["organized_output_dir"] == str(organized_dir)


def test_load_orca_contract_payload_returns_normalized_runtime_fields() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        allowed_root = root / "runs"
        organized_root = root / "outputs"
        original_dir = allowed_root / "rxn_hist_5"
        organized_dir = organized_root / "opt" / "H2" / "job_hist_5"
        allowed_root.mkdir()
        original_dir.mkdir()
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
                "job_id": "job_hist_5",
                "run_id": "run_hist_5",
                "reaction_dir": str(organized_dir),
                "selected_inp": str(inp),
                "max_retries": 3,
                "status": "completed",
                "attempts": [
                    {
                        "index": 2,
                        "inp_path": str(inp),
                        "out_path": str(out),
                        "return_code": 0,
                        "analyzer_status": "completed",
                        "analyzer_reason": "normal_termination",
                        "markers": [],
                        "patch_actions": [],
                    }
                ],
                "final_result": {
                    "status": "completed",
                    "analyzer_status": "completed",
                    "reason": "normal_termination",
                    "completed_at": "2026-04-19T00:10:00+00:00",
                    "last_out_path": str(out),
                },
            },
        )
        _write_json(
            organized_dir / "run_report.json",
            {
                "job_id": "job_hist_5",
                "run_id": "run_hist_5",
                "status": "completed",
                "selected_inp": str(inp),
                "attempt_count": 1,
                "max_retries": 3,
                "attempts": [
                    {
                        "index": 2,
                        "inp_path": str(inp),
                        "out_path": str(out),
                        "return_code": 0,
                        "analyzer_status": "completed",
                        "analyzer_reason": "normal_termination",
                        "markers": [],
                        "patch_actions": [],
                    }
                ],
                "final_result": {
                    "status": "completed",
                    "analyzer_status": "completed",
                    "reason": "normal_termination",
                    "completed_at": "2026-04-19T00:10:00+00:00",
                    "last_out_path": str(out),
                },
            },
        )
        _write_json(
            original_dir / "organized_ref.json",
            {
                "job_id": "job_hist_5",
                "run_id": "run_hist_5",
                "original_run_dir": str(original_dir),
                "organized_output_dir": str(organized_dir),
                "selected_inp": str(inp),
                "selected_input_xyz": str(xyz),
            },
        )
        _write_json(
            allowed_root / "queue.json",
            [
                {
                    "queue_id": "q_hist_5",
                    "task_id": "job_hist_5",
                    "run_id": "run_hist_5",
                    "reaction_dir": str(original_dir),
                    "status": "completed",
                    "cancel_requested": False,
                    "resource_request": {"max_cores": 8, "max_memory_gb": 16},
                    "resource_actual": {"max_cores": 8, "max_memory_gb": 16},
                }
            ],
        )
        _write_json(
            allowed_root / "job_locations.json",
            [
                {
                    "job_id": "job_hist_5",
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

        payload = load_orca_contract_payload(
            allowed_root,
            "job_hist_5",
            organized_root=organized_root,
        )

        assert payload["run_id"] == "run_hist_5"
        assert payload["status"] == "completed"
        assert payload["reason"] == "normal_termination"
        assert payload["reaction_dir"] == str(organized_dir.resolve())
        assert payload["latest_known_path"] == str(organized_dir.resolve())
        assert payload["organized_output_dir"] == str(organized_dir.resolve())
        assert payload["queue_id"] == "q_hist_5"
        assert payload["queue_status"] == "completed"
        assert payload["selected_inp"] == str(inp.resolve())
        assert payload["selected_input_xyz"] == str(xyz.resolve())
        assert payload["optimized_xyz_path"] == str(xyz.resolve())
        assert payload["last_out_path"] == str(out.resolve())
        assert payload["attempt_count"] == 1
        assert payload["max_retries"] == 3
        assert payload["resource_request"] == {"max_cores": 8, "max_memory_gb": 16}


def test_job_locations_falls_back_when_chem_core_is_unavailable() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        cfg = _make_cfg(root)
        allowed_root = Path(cfg.runtime.allowed_root)
        allowed_root.mkdir(parents=True)
        job_dir = allowed_root / "rxn_fallback"
        job_dir.mkdir()
        inp = job_dir / "rxn.inp"
        inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")

        def _missing_chem_core(_name: str) -> object:
            exc = ModuleNotFoundError("No module named 'chem_core'")
            exc.name = "chem_core"
            raise exc

        job_locations_module._chem_core_indexing_module.cache_clear()
        try:
            with patch.object(job_locations_module, "import_module", side_effect=_missing_chem_core):
                record = upsert_job_record(
                    cfg,
                    job_id="job_fallback_1",
                    status="queued",
                    job_dir=job_dir,
                    job_type="opt",
                    selected_input_xyz=str(inp),
                    molecule_key="H2",
                    resource_request={"max_cores": 8, "max_memory_gb": 16},
                    resource_actual={"max_cores": 8, "max_memory_gb": 16},
                )

                backend = job_locations_module._chem_core_indexing_module()
                assert backend.JOB_LOCATION_INDEX_FILE_NAME == "job_locations.json"
                assert record.job_id == "job_fallback_1"
                assert resolve_latest_job_dir(index_root_for_cfg(cfg), "job_fallback_1") == job_dir.resolve()

            loaded = _load_job_locations(index_root_for_cfg(cfg))
            assert len(loaded) == 1
            assert loaded[0]["job_id"] == "job_fallback_1"
        finally:
            job_locations_module._chem_core_indexing_module.cache_clear()
