from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from core.molecule_key import (
    _atoms_to_hill_formula,
    _directory_name_fallback,
    _find_user_tag,
    _parse_formula_from_inp,
    _parse_xyz_file,
    _sanitize_key,
    extract_molecule_key,
)


class TestUserTag(unittest.TestCase):

    def test_finds_tag(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "rxn.inp"
            inp.write_text("# TAG: my_molecule\n! Opt\n* xyz 0 1\nH 0 0 0\n*\n")
            self.assertEqual(_find_user_tag(inp), "my_molecule")

    def test_sanitizes_special_chars(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "rxn.inp"
            inp.write_text("# TAG: my molecule/v2\n! Opt\n")
            self.assertEqual(_find_user_tag(inp), "my_molecule_v2")

    def test_returns_none_when_no_tag(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\n*\n")
            self.assertIsNone(_find_user_tag(inp))

    def test_case_insensitive(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "rxn.inp"
            inp.write_text("# tag: MyTag\n! Opt\n")
            self.assertEqual(_find_user_tag(inp), "MyTag")


class TestParseFormulaFromInp(unittest.TestCase):

    def test_inline_xyz(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nC 0 0 0\nC 1 0 0\nH 2 0 0\nH 3 0 0\nO 4 0 0\n*\n")
            self.assertEqual(_parse_formula_from_inp(inp), "C2H2O")

    def test_xyzfile_reference(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            xyz = Path(td) / "mol.xyz"
            xyz.write_text("4\ncomment\nC 0 0 0\nH 1 0 0\nH 2 0 0\nH 3 0 0\n")
            inp = Path(td) / "rxn.inp"
            inp.write_text(f"! Opt\n* xyzfile 0 1 mol.xyz\n")
            self.assertEqual(_parse_formula_from_inp(inp), "CH3")

    def test_xyzfile_missing_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "rxn.inp"
            inp.write_text("! Opt\n* xyzfile 0 1 nonexistent.xyz\n")
            self.assertIsNone(_parse_formula_from_inp(inp))

    def test_no_geometry_block(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "rxn.inp"
            inp.write_text("! Opt\n")
            self.assertIsNone(_parse_formula_from_inp(inp))


class TestParseXyzFile(unittest.TestCase):

    def test_standard_xyz(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            xyz = Path(td) / "mol.xyz"
            xyz.write_text("3\ncomment line\nO 0.0 0.0 0.0\nH 0.0 0.0 1.0\nH 0.0 1.0 0.0\n")
            atoms = _parse_xyz_file(xyz)
            self.assertEqual(atoms, ["O", "H", "H"])

    def test_missing_file(self) -> None:
        atoms = _parse_xyz_file(Path("/nonexistent/mol.xyz"))
        self.assertEqual(atoms, [])


class TestHillFormula(unittest.TestCase):

    def test_carbon_first(self) -> None:
        self.assertEqual(_atoms_to_hill_formula(["O", "H", "H", "C"]), "CH2O")

    def test_no_carbon_alphabetical(self) -> None:
        self.assertEqual(_atoms_to_hill_formula(["Na", "Cl"]), "ClNa")

    def test_empty_returns_none(self) -> None:
        self.assertIsNone(_atoms_to_hill_formula([]))

    def test_single_element_no_count(self) -> None:
        self.assertEqual(_atoms_to_hill_formula(["C"]), "C")

    def test_multiple_same(self) -> None:
        self.assertEqual(_atoms_to_hill_formula(["C", "C", "C"]), "C3")

    def test_complex_molecule(self) -> None:
        atoms = ["C"] * 8 + ["H"] * 10 + ["O"] * 2
        self.assertEqual(_atoms_to_hill_formula(atoms), "C8H10O2")


class TestSanitizeKey(unittest.TestCase):

    def test_safe_string(self) -> None:
        self.assertEqual(_sanitize_key("C8H10O2"), "C8H10O2")

    def test_spaces_replaced(self) -> None:
        self.assertEqual(_sanitize_key("my molecule"), "my_molecule")

    def test_slashes_replaced(self) -> None:
        self.assertEqual(_sanitize_key("path/to/mol"), "path_to_mol")

    def test_empty_returns_unknown(self) -> None:
        self.assertEqual(_sanitize_key(""), "unknown")

    def test_all_special_returns_unknown(self) -> None:
        self.assertEqual(_sanitize_key("///"), "unknown")


class TestDirectoryNameFallback(unittest.TestCase):

    def test_uses_parent_name(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = Path(td) / "Int1_DMSO"
            d.mkdir()
            inp = d / "rxn.inp"
            inp.write_text("! Opt\n")
            self.assertEqual(_directory_name_fallback(inp), "Int1_DMSO")


class TestExtractMoleculeKey(unittest.TestCase):

    def test_tag_takes_priority(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "rxn.inp"
            inp.write_text("# TAG: custom_name\n! Opt\n* xyz 0 1\nC 0 0 0\n*\n")
            self.assertEqual(extract_molecule_key(inp), "custom_name")

    def test_formula_when_no_tag(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nC 0 0 0\nH 1 0 0\n*\n")
            self.assertEqual(extract_molecule_key(inp), "CH")

    def test_dirname_when_no_geometry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = Path(td) / "TS1_acetone"
            d.mkdir()
            inp = d / "rxn.inp"
            inp.write_text("! Opt\n")
            self.assertEqual(extract_molecule_key(inp), "TS1_acetone")


if __name__ == "__main__":
    unittest.main()
