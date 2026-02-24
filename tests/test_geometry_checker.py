from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from core.geometry_checker import (
    CheckItem,
    CheckResult,
    CheckSkipReason,
    _check_fragmentation_hint,
    _check_imaginary_frequencies_opt,
    _check_scf_convergence,
    _check_short_contacts,
    _check_spin_contamination,
    _check_ts_frequency_count,
    _compute_pair_distances,
    _nearest_neighbor_distances,
    _parse_multiplicity_from_inp,
    _parse_multiplicity_from_out,
    _parse_xyz_atoms,
    check_root_scan,
    check_single,
)
from core.statuses import AnalyzerStatus


class TestParseXyzAtoms(unittest.TestCase):
    def test_standard_xyz(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".xyz", delete=False) as f:
            f.write("3\ncomment\n")
            f.write("H  0.0  0.0  0.0\n")
            f.write("O  0.0  0.0  0.96\n")
            f.write("H  0.0  0.76  -0.48\n")
            f.flush()
            atoms = _parse_xyz_atoms(Path(f.name))
        self.assertEqual(len(atoms), 3)
        self.assertEqual(atoms[0][0], "H")
        self.assertAlmostEqual(atoms[1][3], 0.96)

    def test_empty_file(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".xyz", delete=False) as f:
            f.write("")
            f.flush()
            atoms = _parse_xyz_atoms(Path(f.name))
        self.assertEqual(atoms, [])


class TestPairDistances(unittest.TestCase):
    def test_two_atoms(self) -> None:
        atoms = [("H", 0.0, 0.0, 0.0), ("H", 1.0, 0.0, 0.0)]
        pairs = _compute_pair_distances(atoms)
        self.assertEqual(len(pairs), 1)
        self.assertAlmostEqual(pairs[0][2], 1.0)


class TestNearestNeighborDistances(unittest.TestCase):
    def test_three_atoms(self) -> None:
        atoms = [("H", 0.0, 0.0, 0.0), ("O", 1.0, 0.0, 0.0), ("H", 5.0, 0.0, 0.0)]
        nn = _nearest_neighbor_distances(atoms)
        self.assertEqual(len(nn), 3)
        self.assertAlmostEqual(nn[0], 1.0)
        self.assertAlmostEqual(nn[1], 1.0)
        self.assertAlmostEqual(nn[2], 4.0)

    def test_single_atom(self) -> None:
        nn = _nearest_neighbor_distances([("H", 0.0, 0.0, 0.0)])
        self.assertEqual(nn, [0.0])


class TestCheckImaginaryFrequenciesOpt(unittest.TestCase):
    def test_no_imaginary(self) -> None:
        result = _check_imaginary_frequencies_opt("No frequencies here")
        self.assertEqual(result.severity, "ok")

    def test_has_imaginary(self) -> None:
        text = "   -123.45 cm**-1\n   456.78 cm**-1\n"
        result = _check_imaginary_frequencies_opt(text)
        self.assertEqual(result.severity, "warning")
        self.assertIn("1", result.message)


class TestCheckTsFrequencyCount(unittest.TestCase):
    def test_exactly_one(self) -> None:
        result = _check_ts_frequency_count(1)
        self.assertEqual(result.severity, "ok")

    def test_zero(self) -> None:
        result = _check_ts_frequency_count(0)
        self.assertEqual(result.severity, "error")

    def test_two(self) -> None:
        result = _check_ts_frequency_count(2)
        self.assertEqual(result.severity, "error")


class TestCheckScfConvergence(unittest.TestCase):
    def test_error_scf_status(self) -> None:
        result = _check_scf_convergence("", "error_scf")
        self.assertEqual(result.severity, "error")

    def test_error_scf_status_enum(self) -> None:
        result = _check_scf_convergence("", AnalyzerStatus.ERROR_SCF)
        self.assertEqual(result.severity, "error")

    def test_parse_failure(self) -> None:
        result = _check_scf_convergence("no energy change here", "completed")
        self.assertEqual(result.severity, "warning")

    def test_large_delta(self) -> None:
        text = "Last Energy change   ...   1.23456789e-04"
        result = _check_scf_convergence(text, "completed")
        self.assertEqual(result.severity, "warning")

    def test_small_delta(self) -> None:
        text = "Last Energy change   ...   -1.23456789e-09"
        result = _check_scf_convergence(text, "completed")
        self.assertEqual(result.severity, "ok")

    def test_uses_last_energy_change_entry(self) -> None:
        text = (
            "Last Energy change   ...   1.00e-03\n"
            "something else\n"
            "Last Energy change   ...   -1.00e-09\n"
        )
        result = _check_scf_convergence(text, "completed")
        self.assertEqual(result.severity, "ok")


class TestCheckShortContacts(unittest.TestCase):
    def test_normal_distances(self) -> None:
        atoms = [("H", 0.0, 0.0, 0.0), ("O", 0.0, 0.0, 0.96)]
        result = _check_short_contacts(atoms)
        self.assertEqual(result.severity, "ok")

    def test_short_contact(self) -> None:
        atoms = [("H", 0.0, 0.0, 0.0), ("H", 0.0, 0.0, 0.3)]
        result = _check_short_contacts(atoms)
        self.assertEqual(result.severity, "error")


class TestCheckFragmentationHint(unittest.TestCase):
    def test_no_fragmentation(self) -> None:
        atoms = [("H", 0.0, 0.0, 0.0), ("O", 0.0, 0.0, 0.96), ("H", 0.0, 0.76, -0.48)]
        result = _check_fragmentation_hint(atoms)
        self.assertEqual(result.severity, "ok")

    def test_fragmentation(self) -> None:
        atoms = [("H", 0.0, 0.0, 0.0), ("O", 0.0, 0.0, 0.96), ("H", 10.0, 0.0, 0.0)]
        result = _check_fragmentation_hint(atoms)
        self.assertEqual(result.severity, "warning")

    def test_single_atom(self) -> None:
        result = _check_fragmentation_hint([("H", 0.0, 0.0, 0.0)])
        self.assertEqual(result.severity, "ok")


class TestCheckSpinContamination(unittest.TestCase):
    def test_no_s2(self) -> None:
        result = _check_spin_contamination("no spin data", 1)
        self.assertEqual(result.severity, "ok")

    def test_singlet_clean(self) -> None:
        text = "Expectation value of <S**2>:    0.0000"
        result = _check_spin_contamination(text, 1)
        self.assertEqual(result.severity, "ok")

    def test_doublet_contaminated(self) -> None:
        # Doublet: S=0.5, S(S+1)=0.75
        text = "Expectation value of <S**2>:    1.5000"
        result = _check_spin_contamination(text, 2)
        self.assertEqual(result.severity, "warning")

    def test_doublet_clean(self) -> None:
        text = "Expectation value of <S**2>:    0.7510"
        result = _check_spin_contamination(text, 2)
        self.assertEqual(result.severity, "ok")

    def test_uses_last_s2_value(self) -> None:
        text = (
            "Expectation value of <S**2>:    1.8000\n"
            "...\n"
            "Expectation value of <S**2>:    0.7500\n"
        )
        result = _check_spin_contamination(text, 2)
        self.assertEqual(result.severity, "ok")


class TestParseMultiplicity(unittest.TestCase):
    def test_found(self) -> None:
        text = "Multiplicity   Mult ....    3\n"
        self.assertEqual(_parse_multiplicity_from_out(text), 3)

    def test_not_found(self) -> None:
        self.assertEqual(_parse_multiplicity_from_out("no mult here"), 1)

    def test_last_match_wins(self) -> None:
        text = (
            "Multiplicity   Mult ....    1\n"
            "...\n"
            "Multiplicity   Mult ....    3\n"
        )
        self.assertEqual(_parse_multiplicity_from_out(text), 3)


class TestParseMultiplicityFromInp(unittest.TestCase):
    def test_xyz_block(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".inp", delete=False) as f:
            f.write("! B3LYP def2-SVP Opt\n")
            f.write("* xyz 0 2\n")
            f.write("H 0 0 0\nH 0 0 0.7\n*\n")
            f.flush()
            mult = _parse_multiplicity_from_inp(Path(f.name))
        self.assertEqual(mult, 2)

    def test_coords_block(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".inp", delete=False) as f:
            f.write("! B3LYP def2-SVP Opt\n")
            f.write("%coords\n")
            f.write("  Mult 3\n")
            f.write("end\n")
            f.flush()
            mult = _parse_multiplicity_from_inp(Path(f.name))
        self.assertEqual(mult, 3)

    def test_missing_returns_none(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".inp", delete=False) as f:
            f.write("! B3LYP def2-SVP Opt\n")
            f.flush()
            mult = _parse_multiplicity_from_inp(Path(f.name))
        self.assertIsNone(mult)


def _make_completed_opt_dir(td: str) -> Path:
    """Create a minimal completed opt directory for testing."""
    rd = Path(td) / "rxn1"
    rd.mkdir()

    # State file
    state = {
        "run_id": "run_test_001",
        "status": "completed",
        "selected_inp": str(rd / "mol.inp"),
        "final_result": {
            "analyzer_status": "completed",
            "last_out_path": str(rd / "mol.out"),
        },
        "attempts": [],
    }
    (rd / "run_state.json").write_text(json.dumps(state), encoding="utf-8")

    # Input file
    (rd / "mol.inp").write_text("! B3LYP def2-SVP Opt\n", encoding="utf-8")

    # Output file
    out_lines = [
        "Multiplicity   Mult ....    1\n",
        "Last Energy change   ...   -1.23e-09\n",
        "****ORCA TERMINATED NORMALLY****\n",
        "TOTAL RUN TIME: 0 days 0 hours 1 minutes\n",
    ]
    (rd / "mol.out").write_text("".join(out_lines), encoding="utf-8")

    # XYZ file
    xyz = "3\ncomment\nH  0.0  0.0  0.0\nO  0.0  0.0  0.96\nH  0.0  0.76  -0.48\n"
    (rd / "mol.xyz").write_text(xyz, encoding="utf-8")

    return rd


class TestCheckSingle(unittest.TestCase):
    def test_completed_opt_pass(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            rd = _make_completed_opt_dir(td)
            result, skip = check_single(rd)
            self.assertIsNotNone(result)
            self.assertIsNone(skip)
            self.assertEqual(result.overall, "pass")
            self.assertEqual(result.job_type, "opt")
            self.assertEqual(result.run_id, "run_test_001")

    def test_missing_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            rd = Path(td) / "empty"
            rd.mkdir()
            result, skip = check_single(rd)
            self.assertIsNone(result)
            self.assertIsNotNone(skip)
            self.assertEqual(skip.reason, "state_missing_or_invalid")

    def test_not_completed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            rd = Path(td) / "running"
            rd.mkdir()
            state = {"status": "running", "run_id": "run_x"}
            (rd / "run_state.json").write_text(json.dumps(state), encoding="utf-8")
            result, skip = check_single(rd)
            self.assertIsNone(result)
            self.assertEqual(skip.reason, "not_completed")

    def test_output_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            rd = Path(td) / "noout"
            rd.mkdir()
            state = {
                "status": "completed",
                "run_id": "run_y",
                "final_result": {},
                "selected_inp": str(rd / "mol.inp"),
            }
            (rd / "run_state.json").write_text(json.dumps(state), encoding="utf-8")
            result, skip = check_single(rd)
            self.assertIsNone(result)
            self.assertEqual(skip.reason, "output_missing")

    def test_xyz_missing_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            rd = Path(td) / "noxyz"
            rd.mkdir()
            state = {
                "status": "completed",
                "run_id": "run_z",
                "selected_inp": str(rd / "mol.inp"),
                "final_result": {
                    "analyzer_status": "completed",
                    "last_out_path": str(rd / "mol.out"),
                },
            }
            (rd / "run_state.json").write_text(json.dumps(state), encoding="utf-8")
            (rd / "mol.inp").write_text("! B3LYP def2-SVP Opt\n", encoding="utf-8")
            (rd / "mol.out").write_text(
                "Last Energy change   ...   -1.23e-09\n"
                "****ORCA TERMINATED NORMALLY****\n",
                encoding="utf-8",
            )
            result, skip = check_single(rd)
            self.assertIsNone(result)
            self.assertIsNotNone(skip)
            self.assertEqual(skip.reason, "xyz_missing")

    def test_spin_uses_inp_multiplicity_when_out_missing_mult(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            rd = Path(td) / "mult_from_inp"
            rd.mkdir()
            state = {
                "status": "completed",
                "run_id": "run_mult",
                "selected_inp": str(rd / "mol.inp"),
                "final_result": {
                    "analyzer_status": "completed",
                    "last_out_path": str(rd / "mol.out"),
                },
            }
            (rd / "run_state.json").write_text(json.dumps(state), encoding="utf-8")
            (rd / "mol.inp").write_text(
                "! B3LYP def2-SVP Opt\n"
                "* xyz 0 2\n"
                "H 0 0 0\n"
                "H 0 0 0.74\n"
                "*\n",
                encoding="utf-8",
            )
            filler = ("FILLER LINE 1234567890\n" * 15000)
            (rd / "mol.out").write_text(
                filler
                + "Last Energy change   ...   -1.23e-09\n"
                + "Expectation value of <S**2>:    0.7500\n"
                + "****ORCA TERMINATED NORMALLY****\n",
                encoding="utf-8",
            )
            (rd / "mol.xyz").write_text("2\nc\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")

            result, skip = check_single(rd)
            self.assertIsNone(skip)
            self.assertIsNotNone(result)
            spin_check = next(c for c in result.checks if c.check_name == "spin_contamination")
            self.assertEqual(spin_check.severity, "ok")

    def test_ts_frequency_uses_final_vibrational_block_count(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            rd = Path(td) / "ts_final_block"
            rd.mkdir()
            state = {
                "status": "completed",
                "run_id": "run_ts_final_block",
                "selected_inp": str(rd / "ts.inp"),
                "final_result": {
                    "analyzer_status": "completed",
                    "last_out_path": str(rd / "ts.out"),
                },
            }
            (rd / "run_state.json").write_text(json.dumps(state), encoding="utf-8")
            (rd / "ts.inp").write_text("! OptTS Freq\n* xyz 0 1\nH 0 0 0\n*\n", encoding="utf-8")
            (rd / "ts.out").write_text(
                "VIBRATIONAL FREQUENCIES\n"
                "  -300.00 cm**-1\n"
                "  123.00 cm**-1\n"
                "...\n"
                "VIBRATIONAL FREQUENCIES\n"
                "  -120.00 cm**-1\n"
                "  222.00 cm**-1\n"
                "****ORCA TERMINATED NORMALLY****\n",
                encoding="utf-8",
            )
            (rd / "ts.xyz").write_text("2\nc\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")

            result, skip = check_single(rd)
            self.assertIsNone(skip)
            self.assertIsNotNone(result)
            ts_check = next(c for c in result.checks if c.check_name == "ts_frequency_count")
            self.assertEqual(ts_check.severity, "ok")
            self.assertEqual(ts_check.details.get("count"), 1)


class TestCheckRootScan(unittest.TestCase):
    def test_scans_subdirs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _make_completed_opt_dir(td)
            results, skips = check_root_scan(Path(td))
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].overall, "pass")

    def test_empty_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            results, skips = check_root_scan(Path(td))
            self.assertEqual(len(results), 0)
            self.assertEqual(len(skips), 0)


if __name__ == "__main__":
    unittest.main()
