from __future__ import annotations

import json
import tempfile
from pathlib import Path

from core.config import AppConfig, CommonResourceConfig, PathsConfig, RuntimeConfig
from orca_auto.job_locations import (
    index_root_for_cfg,
    load_job_artifacts,
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
