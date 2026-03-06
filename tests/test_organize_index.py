from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from core.organize_index import (
    load_index,
    rebuild_index,
    records_path,
    append_record,
    index_dir,
)


def _write_records(organized_root: Path, records: list) -> None:
    idir = index_dir(organized_root)
    idir.mkdir(parents=True, exist_ok=True)
    rp = records_path(organized_root)
    lines = [json.dumps(r, ensure_ascii=True) for r in records]
    rp.write_text("\n".join(lines) + "\n", encoding="utf-8")


class TestLoadIndex(unittest.TestCase):

    def test_empty_index(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            idx = load_index(Path(td))
            self.assertEqual(idx, {})

    def test_load_records(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            org = Path(td)
            records = [
                {"run_id": "run_a", "job_type": "ts"},
                {"run_id": "run_b", "job_type": "opt"},
                {"run_id": "run_c", "job_type": "ts"},
            ]
            _write_records(org, records)
            idx = load_index(org)
            self.assertEqual(len(idx), 3)
            self.assertIn("run_a", idx)
            self.assertIn("run_b", idx)

    def test_skips_malformed_lines(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            org = Path(td)
            idir = index_dir(org)
            idir.mkdir(parents=True)
            rp = records_path(org)
            rp.write_text(
                '{"run_id": "run_a"}\nnot valid json\n{"run_id": "run_b"}\n',
                encoding="utf-8",
            )
            idx = load_index(org)
            self.assertEqual(len(idx), 2)


class TestAppendRecord(unittest.TestCase):

    def test_appends_to_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            org = Path(td)
            append_record(org, {"run_id": "run_new", "job_type": "opt"})
            idx = load_index(org)
            self.assertIn("run_new", idx)

    def test_appends_to_existing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            org = Path(td)
            _write_records(org, [{"run_id": "run_old"}])
            append_record(org, {"run_id": "run_new"})
            idx = load_index(org)
            self.assertEqual(len(idx), 2)
            self.assertIn("run_old", idx)
            self.assertIn("run_new", idx)


class TestRebuildIndex(unittest.TestCase):

    def test_rebuild_from_organized_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            org = Path(td) / "outputs"
            org.mkdir()

            d = org / "opt" / "CH" / "run_test_001"
            d.mkdir(parents=True)
            inp = d / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nC 0 0 0\nH 1 0 0\n*\n", encoding="utf-8")
            state = {
                "run_id": "run_test_001",
                "status": "completed",
                "selected_inp": str(inp),
                "attempts": [{"index": 1}],
                "final_result": {
                    "status": "completed",
                    "analyzer_status": "completed",
                    "reason": "normal_termination",
                    "completed_at": "2026-01-01T00:00:00+00:00",
                    "last_out_path": str(d / "rxn.out"),
                },
            }
            (d / "run_state.json").write_text(
                json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8",
            )

            count = rebuild_index(org)
            self.assertEqual(count, 1)

            idx = load_index(org)
            self.assertIn("run_test_001", idx)
            self.assertEqual(idx["run_test_001"]["job_type"], "opt")
            self.assertEqual(idx["run_test_001"]["reaction_dir"], str(d))
            self.assertEqual(idx["run_test_001"]["selected_inp"], "rxn.inp")
            self.assertEqual(idx["run_test_001"]["last_out_path"], "rxn.out")

    def test_rebuild_resolves_legacy_absolute_selected_inp(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            org = Path(td) / "outputs"
            org.mkdir()

            d = org / "opt" / "CH" / "run_test_legacy"
            d.mkdir(parents=True)
            inp = d / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nC 0 0 0\nH 1 0 0\n*\n", encoding="utf-8")

            legacy_root = Path(td) / "legacy_runs" / "rxn1"
            legacy_root.mkdir(parents=True)
            legacy_abs_inp = legacy_root / "rxn.inp"

            state = {
                "run_id": "run_test_legacy",
                "status": "completed",
                "selected_inp": str(legacy_abs_inp),
                "attempts": [{"index": 1}],
                "final_result": {
                    "status": "completed",
                    "analyzer_status": "completed",
                    "reason": "normal_termination",
                    "completed_at": "2026-01-01T00:00:00+00:00",
                    "last_out_path": str(legacy_root / "rxn.out"),
                },
            }
            (d / "run_state.json").write_text(
                json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8",
            )

            count = rebuild_index(org)
            self.assertEqual(count, 1)

            idx = load_index(org)
            self.assertEqual(idx["run_test_legacy"]["job_type"], "opt")
            self.assertEqual(idx["run_test_legacy"]["selected_inp"], "rxn.inp")
            self.assertEqual(idx["run_test_legacy"]["last_out_path"], "rxn.out")

    def test_rebuild_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            org = Path(td) / "outputs"
            org.mkdir()
            count = rebuild_index(org)
            self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
