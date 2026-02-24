from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.commands._helpers import (
    _validate_check_reaction_dir,
    _validate_check_root_dir,
    _validate_cleanup_reaction_dir,
    _validate_organized_root_dir,
    _validate_reaction_dir,
    _validate_root_scan_dir,
    finalize_batch_apply,
)
from core.config import AppConfig, PathsConfig, RuntimeConfig


def _cfg(allowed_root: Path, organized_root: Path) -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(
            allowed_root=str(allowed_root),
            organized_root=str(organized_root),
            default_max_retries=3,
        ),
        paths=PathsConfig(orca_executable="/usr/bin/orca"),
    )


class TestCommandPathValidators(unittest.TestCase):
    def test_validate_reaction_dir_under_allowed_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            reaction = allowed / "r1"
            allowed.mkdir()
            organized.mkdir()
            reaction.mkdir()
            cfg = _cfg(allowed, organized)

            resolved = _validate_reaction_dir(cfg, str(reaction))
            self.assertEqual(resolved, reaction.resolve())

    def test_validate_reaction_dir_rejects_outside_allowed_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            outside = root / "outside"
            allowed.mkdir()
            organized.mkdir()
            outside.mkdir()
            cfg = _cfg(allowed, organized)

            with self.assertRaises(ValueError):
                _validate_reaction_dir(cfg, str(outside))

    def test_root_validators_require_exact_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            allowed.mkdir()
            organized.mkdir()
            cfg = _cfg(allowed, organized)

            with self.subTest("allowed_root_exact"):
                self.assertEqual(_validate_root_scan_dir(cfg, str(allowed)), allowed.resolve())
            with self.subTest("allowed_root_mismatch"):
                with self.assertRaises(ValueError):
                    _validate_root_scan_dir(cfg, str(allowed / "nested"))
            with self.subTest("organized_root_exact"):
                self.assertEqual(_validate_organized_root_dir(cfg, str(organized)), organized.resolve())
            with self.subTest("organized_root_mismatch"):
                with self.assertRaises(ValueError):
                    _validate_organized_root_dir(cfg, str(root / "other"))

    def test_cleanup_reaction_dir_must_be_under_organized_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            cleanup_dir = organized / "opt" / "H2" / "run_001"
            outside = allowed / "run_001"
            allowed.mkdir()
            organized.mkdir()
            cleanup_dir.mkdir(parents=True)
            outside.mkdir()
            cfg = _cfg(allowed, organized)

            self.assertEqual(
                _validate_cleanup_reaction_dir(cfg, str(cleanup_dir)),
                cleanup_dir.resolve(),
            )
            with self.assertRaises(ValueError):
                _validate_cleanup_reaction_dir(cfg, str(outside))


class TestCheckValidators(unittest.TestCase):
    def test_check_reaction_dir_under_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            reaction = allowed / "r1"
            allowed.mkdir()
            organized.mkdir()
            reaction.mkdir()
            cfg = _cfg(allowed, organized)
            self.assertEqual(
                _validate_check_reaction_dir(cfg, str(reaction)),
                reaction.resolve(),
            )

    def test_check_reaction_dir_under_organized(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            reaction = organized / "opt" / "H2"
            allowed.mkdir()
            organized.mkdir()
            reaction.mkdir(parents=True)
            cfg = _cfg(allowed, organized)
            self.assertEqual(
                _validate_check_reaction_dir(cfg, str(reaction)),
                reaction.resolve(),
            )

    def test_check_reaction_dir_rejects_outside(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            outside = root / "outside"
            allowed.mkdir()
            organized.mkdir()
            outside.mkdir()
            cfg = _cfg(allowed, organized)
            with self.assertRaises(ValueError):
                _validate_check_reaction_dir(cfg, str(outside))

    def test_check_root_matches_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            allowed.mkdir()
            organized.mkdir()
            cfg = _cfg(allowed, organized)
            self.assertEqual(
                _validate_check_root_dir(cfg, str(allowed)),
                allowed.resolve(),
            )

    def test_check_root_matches_organized(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            allowed.mkdir()
            organized.mkdir()
            cfg = _cfg(allowed, organized)
            self.assertEqual(
                _validate_check_root_dir(cfg, str(organized)),
                organized.resolve(),
            )

    def test_check_root_rejects_other(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            other = root / "other"
            allowed.mkdir()
            organized.mkdir()
            other.mkdir()
            cfg = _cfg(allowed, organized)
            with self.assertRaises(ValueError):
                _validate_check_root_dir(cfg, str(other))


class TestFinalizeBatchApply(unittest.TestCase):
    @patch("core.commands._helpers._send_summary_telegram")
    def test_returns_zero_when_no_failures(self, mock_send) -> None:
        emitted = []

        def _emit(payload, as_json):
            emitted.append((payload, as_json))

        rc = finalize_batch_apply(
            {"action": "apply", "failed": 0},
            AppConfig(),
            "summary-text",
            _emit,
            False,
            [],
        )
        self.assertEqual(rc, 0)
        self.assertEqual(emitted, [({"action": "apply", "failed": 0}, False)])
        mock_send.assert_called_once()

    @patch("core.commands._helpers._send_summary_telegram")
    def test_returns_one_when_failures_exist(self, mock_send) -> None:
        rc = finalize_batch_apply(
            {"action": "apply", "failed": 1},
            AppConfig(),
            "summary-text",
            lambda *_args: None,
            True,
            [{"run_id": "run_001", "reason": "apply_failed"}],
        )
        self.assertEqual(rc, 1)
        mock_send.assert_called_once()


if __name__ == "__main__":
    unittest.main()
