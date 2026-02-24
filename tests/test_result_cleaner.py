from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from core.result_cleaner import (
    CleanupPlan,
    CleanupSkipReason,
    _should_keep,
    check_cleanup_eligibility,
    compute_cleanup_plan,
    execute_cleanup,
    plan_cleanup_root_scan,
    plan_cleanup_single,
)

KEEP_EXT = {".inp", ".out", ".xyz", ".gbw", ".hess"}
KEEP_FN = {"run_state.json", "run_report.json", "run_report.md"}
REMOVE_PAT = ["*.retry*.inp", "*.retry*.out", "*_trj.xyz"]


def _write_state(d: Path, state: dict) -> None:
    (d / "run_state.json").write_text(
        json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8",
    )


def _make_completed_dir(root: Path, name: str) -> Path:
    d = root / name
    d.mkdir(parents=True, exist_ok=True)
    (d / "rxn.inp").write_text("! Opt\n* xyz 0 1\nH 0 0 0\n*\n", encoding="utf-8")
    (d / "rxn.out").write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")
    (d / "rxn.xyz").write_text("1\n\nH 0 0 0\n", encoding="utf-8")
    (d / "rxn.gbw").write_bytes(b"\x00" * 100)
    (d / "rxn.hess").write_text("hessian data\n", encoding="utf-8")
    (d / "run_report.json").write_text("{}", encoding="utf-8")
    (d / "run_report.md").write_text("# Report\n", encoding="utf-8")
    # junk files
    (d / "rxn.densities").write_bytes(b"\x00" * 500)
    (d / "rxn.engrad").write_text("engrad data\n", encoding="utf-8")
    (d / "rxn.tmp").write_text("tmp data\n", encoding="utf-8")
    (d / "rxn.prop").write_text("prop data\n", encoding="utf-8")
    (d / "rxn.scfp").write_text("scfp data\n", encoding="utf-8")
    (d / "rxn.opt").write_text("opt data\n", encoding="utf-8")
    (d / "rxn.retry01.inp").write_text("! Opt\n", encoding="utf-8")
    (d / "rxn.retry01.out").write_text("failed\n", encoding="utf-8")
    (d / "rxn_trj.xyz").write_text("trj data\n", encoding="utf-8")
    state = {
        "run_id": f"run_20260222_101530_{name[:8].ljust(8, '0')}",
        "reaction_dir": str(d),
        "selected_inp": str(d / "rxn.inp"),
        "status": "completed",
        "started_at": "2026-02-22T10:15:30+00:00",
        "updated_at": "2026-02-22T10:15:45+00:00",
        "max_retries": 5,
        "attempts": [{"index": 1, "inp_path": str(d / "rxn.inp"), "out_path": str(d / "rxn.out")}],
        "final_result": {
            "status": "completed",
            "analyzer_status": "completed",
            "reason": "normal_termination",
            "completed_at": "2026-02-22T10:15:45+00:00",
            "last_out_path": str(d / "rxn.out"),
        },
    }
    _write_state(d, state)
    return d


class TestShouldKeep(unittest.TestCase):

    def test_keep_by_extension(self) -> None:
        self.assertTrue(_should_keep(Path("rxn.inp"), KEEP_EXT, KEEP_FN, REMOVE_PAT))
        self.assertTrue(_should_keep(Path("rxn.out"), KEEP_EXT, KEEP_FN, REMOVE_PAT))
        self.assertTrue(_should_keep(Path("rxn.xyz"), KEEP_EXT, KEEP_FN, REMOVE_PAT))
        self.assertTrue(_should_keep(Path("rxn.gbw"), KEEP_EXT, KEEP_FN, REMOVE_PAT))
        self.assertTrue(_should_keep(Path("rxn.hess"), KEEP_EXT, KEEP_FN, REMOVE_PAT))

    def test_keep_by_filename(self) -> None:
        self.assertTrue(_should_keep(Path("run_state.json"), KEEP_EXT, KEEP_FN, REMOVE_PAT))
        self.assertTrue(_should_keep(Path("run_report.json"), KEEP_EXT, KEEP_FN, REMOVE_PAT))
        self.assertTrue(_should_keep(Path("run_report.md"), KEEP_EXT, KEEP_FN, REMOVE_PAT))

    def test_keep_overrides_remove_patterns_by_default(self) -> None:
        self.assertTrue(_should_keep(Path("rxn.retry01.inp"), KEEP_EXT, KEEP_FN, REMOVE_PAT))
        self.assertTrue(_should_keep(Path("rxn.retry01.out"), KEEP_EXT, KEEP_FN, REMOVE_PAT))
        self.assertTrue(_should_keep(Path("rxn_trj.xyz"), KEEP_EXT, KEEP_FN, REMOVE_PAT))

    def test_remove_patterns_override_keep_when_enabled(self) -> None:
        self.assertFalse(
            _should_keep(
                Path("rxn.retry01.inp"),
                KEEP_EXT,
                KEEP_FN,
                REMOVE_PAT,
                remove_overrides_keep=True,
            )
        )
        self.assertFalse(
            _should_keep(
                Path("rxn.retry01.out"),
                KEEP_EXT,
                KEEP_FN,
                REMOVE_PAT,
                remove_overrides_keep=True,
            )
        )
        self.assertFalse(
            _should_keep(
                Path("rxn_trj.xyz"),
                KEEP_EXT,
                KEEP_FN,
                REMOVE_PAT,
                remove_overrides_keep=True,
            )
        )

    def test_junk_removed(self) -> None:
        self.assertFalse(_should_keep(Path("rxn.densities"), KEEP_EXT, KEEP_FN, REMOVE_PAT))
        self.assertFalse(_should_keep(Path("rxn.tmp"), KEEP_EXT, KEEP_FN, REMOVE_PAT))
        self.assertFalse(_should_keep(Path("rxn.prop"), KEEP_EXT, KEEP_FN, REMOVE_PAT))

    def test_empty_remove_patterns(self) -> None:
        self.assertTrue(_should_keep(Path("rxn.retry01.inp"), KEEP_EXT, KEEP_FN, []))
        self.assertTrue(_should_keep(Path("rxn_trj.xyz"), KEEP_EXT, KEEP_FN, []))


class TestCheckCleanupEligibility(unittest.TestCase):

    def test_completed_is_eligible(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = _make_completed_dir(Path(td), "rxn1")
            state, skip = check_cleanup_eligibility(d)
            self.assertIsNotNone(state)
            self.assertIsNone(skip)

    def test_failed_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = Path(td) / "rxn1"
            d.mkdir()
            _write_state(d, {"run_id": "run_test", "status": "failed", "final_result": {}})
            state, skip = check_cleanup_eligibility(d)
            self.assertIsNone(state)
            self.assertIsNotNone(skip)
            self.assertEqual(skip.reason, "not_completed")

    def test_missing_state_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = Path(td) / "rxn1"
            d.mkdir()
            state, skip = check_cleanup_eligibility(d)
            self.assertIsNone(state)
            self.assertEqual(skip.reason, "state_missing_or_invalid")

    def test_invalid_run_id_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = Path(td) / "rxn1"
            d.mkdir()
            _write_state(d, {"run_id": "", "status": "completed"})
            state, skip = check_cleanup_eligibility(d)
            self.assertIsNone(state)
            self.assertEqual(skip.reason, "state_schema_invalid")


class TestComputeCleanupPlan(unittest.TestCase):

    def test_identifies_files_to_remove(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = _make_completed_dir(Path(td), "rxn1")
            state = json.loads((d / "run_state.json").read_text())
            plan = compute_cleanup_plan(d, state, KEEP_EXT, KEEP_FN, REMOVE_PAT)
            removed_names = {e.path.name for e in plan.files_to_remove}
            self.assertIn("rxn.densities", removed_names)
            self.assertIn("rxn.engrad", removed_names)
            self.assertIn("rxn.tmp", removed_names)
            self.assertIn("rxn.prop", removed_names)
            self.assertIn("rxn.scfp", removed_names)
            self.assertIn("rxn.opt", removed_names)
            self.assertNotIn("rxn.retry01.inp", removed_names)
            self.assertNotIn("rxn.retry01.out", removed_names)
            self.assertNotIn("rxn_trj.xyz", removed_names)

    def test_remove_patterns_apply_when_override_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = _make_completed_dir(Path(td), "rxn1")
            state = json.loads((d / "run_state.json").read_text())
            plan = compute_cleanup_plan(
                d,
                state,
                KEEP_EXT,
                KEEP_FN,
                REMOVE_PAT,
                remove_overrides_keep=True,
            )
            removed_names = {e.path.name for e in plan.files_to_remove}
            self.assertIn("rxn.retry01.inp", removed_names)
            self.assertIn("rxn.retry01.out", removed_names)
            self.assertIn("rxn_trj.xyz", removed_names)

    def test_keeps_essential_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = _make_completed_dir(Path(td), "rxn1")
            state = json.loads((d / "run_state.json").read_text())
            plan = compute_cleanup_plan(d, state, KEEP_EXT, KEEP_FN, REMOVE_PAT)
            removed_names = {e.path.name for e in plan.files_to_remove}
            for essential in ["rxn.inp", "rxn.out", "rxn.xyz", "rxn.gbw", "rxn.hess",
                              "run_state.json", "run_report.json", "run_report.md"]:
                self.assertNotIn(essential, removed_names, f"{essential} should be kept")

    def test_preserves_state_referenced_retry_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = _make_completed_dir(Path(td), "rxn1")
            state = json.loads((d / "run_state.json").read_text())
            state["selected_inp"] = "/legacy/path/rxn.retry01.inp"
            state["attempts"] = [
                {
                    "index": 1,
                    "inp_path": str(d / "rxn.inp"),
                    "out_path": str(d / "rxn.out"),
                },
                {
                    "index": 2,
                    "inp_path": "/legacy/path/rxn.retry01.inp",
                    "out_path": "/legacy/path/rxn.retry01.out",
                },
            ]
            state["final_result"]["last_out_path"] = "/legacy/path/rxn.retry01.out"

            plan = compute_cleanup_plan(d, state, KEEP_EXT, KEEP_FN, REMOVE_PAT)
            removed_names = {e.path.name for e in plan.files_to_remove}
            self.assertNotIn("rxn.retry01.inp", removed_names)
            self.assertNotIn("rxn.retry01.out", removed_names)

    def test_keep_count(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = _make_completed_dir(Path(td), "rxn1")
            state = json.loads((d / "run_state.json").read_text())
            plan = compute_cleanup_plan(d, state, KEEP_EXT, KEEP_FN, REMOVE_PAT)
            self.assertEqual(plan.keep_count, 11)

    def test_total_remove_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = _make_completed_dir(Path(td), "rxn1")
            state = json.loads((d / "run_state.json").read_text())
            plan = compute_cleanup_plan(d, state, KEEP_EXT, KEEP_FN, REMOVE_PAT)
            self.assertGreater(plan.total_remove_bytes, 0)

    def test_skips_subdirectories(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = _make_completed_dir(Path(td), "rxn1")
            subdir = d / "subdir"
            subdir.mkdir()
            (subdir / "junk.tmp").write_text("junk", encoding="utf-8")
            state = json.loads((d / "run_state.json").read_text())
            plan = compute_cleanup_plan(d, state, KEEP_EXT, KEEP_FN, REMOVE_PAT)
            removed_names = {e.path.name for e in plan.files_to_remove}
            self.assertNotIn("junk.tmp", removed_names)


class TestPlanCleanupSingle(unittest.TestCase):

    def test_nothing_to_clean(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = Path(td) / "rxn_clean"
            d.mkdir()
            (d / "rxn.inp").write_text("! Opt\n", encoding="utf-8")
            _write_state(d, {
                "run_id": "run_test", "status": "completed",
                "final_result": {"status": "completed"},
            })
            (d / "run_report.json").write_text("{}", encoding="utf-8")
            (d / "run_report.md").write_text("# R\n", encoding="utf-8")
            plan, skip = plan_cleanup_single(d, KEEP_EXT, KEEP_FN, REMOVE_PAT)
            self.assertIsNone(plan)
            self.assertIsNotNone(skip)
            self.assertEqual(skip.reason, "nothing_to_clean")


class TestExecuteCleanup(unittest.TestCase):

    def test_removes_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = _make_completed_dir(Path(td), "rxn1")
            state = json.loads((d / "run_state.json").read_text())
            plan = compute_cleanup_plan(d, state, KEEP_EXT, KEEP_FN, REMOVE_PAT)
            result = execute_cleanup(plan)
            self.assertGreater(result.files_removed, 0)
            self.assertGreater(result.bytes_freed, 0)
            self.assertEqual(len(result.errors), 0)
            self.assertTrue((d / "rxn.inp").exists())
            self.assertTrue((d / "rxn.out").exists())
            self.assertTrue((d / "rxn.gbw").exists())
            self.assertTrue((d / "run_state.json").exists())
            self.assertFalse((d / "rxn.densities").exists())
            self.assertFalse((d / "rxn.engrad").exists())
            self.assertTrue((d / "rxn.retry01.inp").exists())
            self.assertTrue((d / "rxn.retry01.out").exists())
            self.assertTrue((d / "rxn_trj.xyz").exists())


class TestPlanCleanupRootScan(unittest.TestCase):

    def test_scans_nested_directories(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _make_completed_dir(root / "opt" / "H2", "run_001")
            _make_completed_dir(root / "ts" / "H2O", "run_002")
            plans, skips = plan_cleanup_root_scan(root, KEEP_EXT, KEEP_FN, REMOVE_PAT)
            self.assertEqual(len(plans), 2)

    def test_skips_index_directory(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            idx = root / "index"
            idx.mkdir()
            _write_state(idx, {"run_id": "idx", "status": "completed"})
            plans, skips = plan_cleanup_root_scan(root, KEEP_EXT, KEEP_FN, REMOVE_PAT)
            self.assertEqual(len(plans), 0)

    def test_index_like_directory_name_is_not_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _make_completed_dir(root / "opt" / "my_index_case", "run_001")
            plans, skips = plan_cleanup_root_scan(root, KEEP_EXT, KEEP_FN, REMOVE_PAT)
            self.assertEqual(len(plans), 1)
            self.assertEqual(len(skips), 0)

    def test_nonexistent_root(self) -> None:
        plans, skips = plan_cleanup_root_scan(
            Path("/nonexistent/path"), KEEP_EXT, KEEP_FN, REMOVE_PAT,
        )
        self.assertEqual(len(plans), 0)
        self.assertEqual(len(skips), 0)


if __name__ == "__main__":
    unittest.main()
