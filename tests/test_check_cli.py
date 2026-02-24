from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.cli import main


def _make_config(td: str, allowed: Path, organized: Path) -> str:
    cfg_path = Path(td) / "orca_auto.yaml"
    cfg_path.write_text(
        json.dumps({
            "runtime": {
                "allowed_root": str(allowed),
                "organized_root": str(organized),
            },
            "paths": {"orca_executable": "/usr/bin/orca"},
        }),
        encoding="utf-8",
    )
    return str(cfg_path)


def _make_completed_dir(parent: Path, name: str = "rxn1") -> Path:
    rd = parent / name
    rd.mkdir(parents=True, exist_ok=True)

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
    (rd / "mol.inp").write_text("! B3LYP def2-SVP Opt\n", encoding="utf-8")

    out_lines = [
        "Multiplicity   Mult ....    1\n",
        "Last Energy change   ...   -1.23e-09\n",
        "****ORCA TERMINATED NORMALLY****\n",
        "TOTAL RUN TIME: 0 days\n",
    ]
    (rd / "mol.out").write_text("".join(out_lines), encoding="utf-8")

    xyz = "3\ncomment\nH  0.0  0.0  0.0\nO  0.0  0.0  0.96\nH  0.0  0.76  -0.48\n"
    (rd / "mol.xyz").write_text(xyz, encoding="utf-8")
    return rd


class TestCheckCli(unittest.TestCase):
    def test_mutually_exclusive_options(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            allowed.mkdir()
            organized.mkdir()
            cfg_path = _make_config(td, allowed, organized)
            rd = _make_completed_dir(organized)

            rc = main(["--config", cfg_path, "check",
                        "--reaction-dir", str(rd), "--root", str(organized)])
            self.assertEqual(rc, 1)

    def test_single_reaction_dir_pass(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            allowed.mkdir()
            organized.mkdir()
            cfg_path = _make_config(td, allowed, organized)
            rd = _make_completed_dir(organized)

            rc = main(["--config", cfg_path, "check",
                        "--reaction-dir", str(rd), "--json"])
            self.assertEqual(rc, 0)

    def test_root_scan(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            allowed.mkdir()
            organized.mkdir()
            cfg_path = _make_config(td, allowed, organized)
            _make_completed_dir(organized)

            rc = main(["--config", cfg_path, "check",
                        "--root", str(organized), "--json"])
            self.assertEqual(rc, 0)

    def test_default_root_scan(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            allowed.mkdir()
            organized.mkdir()
            cfg_path = _make_config(td, allowed, organized)
            _make_completed_dir(organized)

            rc = main(["--config", cfg_path, "check", "--json"])
            self.assertEqual(rc, 0)

    def test_invalid_reaction_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            outside = root / "outside"
            allowed.mkdir()
            organized.mkdir()
            outside.mkdir()
            cfg_path = _make_config(td, allowed, organized)

            rc = main(["--config", cfg_path, "check",
                        "--reaction-dir", str(outside)])
            self.assertEqual(rc, 1)

    def test_default_root_missing_returns_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized_missing = root / "organized_missing"
            allowed.mkdir()
            cfg_path = _make_config(td, allowed, organized_missing)

            rc = main(["--config", cfg_path, "check", "--json"])
            self.assertEqual(rc, 1)


if __name__ == "__main__":
    unittest.main()
