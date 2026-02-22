import tempfile
import time
import unittest
from pathlib import Path

from core.inp_rewriter import rewrite_for_retry


BASE_INP = """! OptTS Freq IRC

%pal
  nprocs 8
end

* xyz 0 1
H 0 0 0
H 0 0 0.74
*
"""


class TestInpRewriter(unittest.TestCase):
    def test_step1_adds_scf_stability_and_uses_previous_xyz(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            src = root / "rxn.inp"
            dst = root / "rxn.retry01.inp"
            src.write_text(BASE_INP, encoding="utf-8")
            (root / "rxn.xyz").write_text("2\n\nH 0 0 0\nH 0 0 0.75\n", encoding="utf-8")
            actions = rewrite_for_retry(src, dst, root, step=1)
            out = dst.read_text(encoding="utf-8")
        self.assertIn("route_add_tightscf_slowconv", actions)
        self.assertIn("TightSCF", out)
        self.assertIn("SlowConv", out)
        self.assertIn("%scf", out)
        self.assertIn("MaxIter 300", out)
        self.assertIn("geometry_restart_from_rxn.xyz", actions)
        self.assertIn("* xyzfile 0 1 rxn.xyz", out)

    def test_step3_replaces_geometry_with_previous_attempt_xyz(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            src = root / "rxn.retry02.inp"
            dst = root / "rxn.retry03.inp"
            src.write_text(BASE_INP, encoding="utf-8")
            (root / "rxn.retry02.xyz").write_text("2\n\nH 0 0 0\nH 0 0 0.8\n", encoding="utf-8")
            # Even when trj exists, retry should use previous .xyz of source input.
            (root / "rxn.retry02_trj.xyz").write_text("2\n\nH 0 0 0\nH 0 0 1.1\n", encoding="utf-8")
            actions = rewrite_for_retry(src, dst, root, step=3)
            out = dst.read_text(encoding="utf-8")
        self.assertIn("geometry_restart_from_rxn.retry02.xyz", actions)
        self.assertIn("* xyzfile 0 1 rxn.retry02.xyz", out)

    def test_step4_and_step5_change_nprocs_with_previous_xyz(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            src = root / "rxn.retry03.inp"
            mid = root / "rxn.retry04.inp"
            end = root / "rxn.retry05.inp"
            src.write_text(BASE_INP, encoding="utf-8")
            (root / "rxn.retry03.xyz").write_text("2\n\nH 0 0 0\nH 0 0 0.85\n", encoding="utf-8")

            actions4 = rewrite_for_retry(src, mid, root, step=4)
            text4 = mid.read_text(encoding="utf-8")
            (root / "rxn.retry04.xyz").write_text("2\n\nH 0 0 0\nH 0 0 0.9\n", encoding="utf-8")
            actions5 = rewrite_for_retry(mid, end, root, step=5)
            text5 = end.read_text(encoding="utf-8")

        self.assertIn("nprocs_reduced_to_4", actions4)
        self.assertIn("nprocs 4", text4)
        self.assertIn("geometry_restart_from_rxn.retry03.xyz", actions4)
        self.assertIn("* xyzfile 0 1 rxn.retry03.xyz", text4)
        self.assertIn("nprocs_set_to_1", actions5)
        self.assertIn("nprocs 1", text5)
        self.assertIn("geometry_restart_from_rxn.retry04.xyz", actions5)
        self.assertIn("* xyzfile 0 1 rxn.retry04.xyz", text5)

    def test_fallbacks_to_latest_geometry_when_previous_xyz_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            src = root / "rxn.inp"
            dst = root / "rxn.retry01.inp"
            src.write_text(BASE_INP, encoding="utf-8")
            (root / "older.xyz").write_text("2\n\nH 0 0 0\nH 0 0 0.7\n", encoding="utf-8")
            time.sleep(0.01)
            (root / "latest_trj.xyz").write_text("2\n\nH 0 0 0\nH 0 0 1.0\n", encoding="utf-8")
            actions = rewrite_for_retry(src, dst, root, step=1)
            out = dst.read_text(encoding="utf-8")

        self.assertIn("no_previous_xyz_file_found", actions)
        self.assertIn("geometry_restart_from_latest_trj.xyz", actions)
        self.assertIn("* xyzfile 0 1 latest_trj.xyz", out)

    def test_marks_missing_all_geometry_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            src = root / "rxn.inp"
            dst = root / "rxn.retry01.inp"
            src.write_text(BASE_INP, encoding="utf-8")
            actions = rewrite_for_retry(src, dst, root, step=1)
        self.assertIn("no_previous_xyz_file_found", actions)
        self.assertIn("no_geometry_file_found", actions)


    def test_find_block_range_does_not_mutate_lines(self) -> None:
        """_find_block_range must not append 'end' to the shared lines list.

        Before the fix, calling _find_block_range on an unclosed block would
        append 'end' to lines, corrupting subsequent block lookups. This test
        verifies that reading nprocs from an unclosed %pal block does NOT
        change the line count, so a subsequent %scf lookup still works correctly.
        """
        from core.inp_rewriter import _find_block_range, _read_nprocs

        lines = [
            "! OptTS Freq IRC",
            "",
            "%pal",
            "  nprocs 8",
            "",
            "%scf",
            "  MaxIter 125",
            "",
            "* xyz 0 1",
            "H 0 0 0",
            "H 0 0 0.74",
            "*",
        ]
        original_len = len(lines)
        # Reading nprocs calls _find_block_range internally; must NOT mutate lines
        nprocs = _read_nprocs(lines)
        self.assertEqual(nprocs, 8)
        self.assertEqual(len(lines), original_len)

        # _find_block_range for %scf should still return correct unclosed range
        rng = _find_block_range(lines, "scf")
        self.assertIsNotNone(rng)
        start, end, needs_close = rng
        self.assertEqual(start, 5)
        self.assertTrue(needs_close)
        self.assertEqual(len(lines), original_len)


if __name__ == "__main__":
    unittest.main()
