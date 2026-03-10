from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from core.commands._helpers import (
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


class TestFinalizeBatchApply(unittest.TestCase):
    def test_returns_zero_when_no_failures(self) -> None:
        emitted = []

        def _emit(payload):
            emitted.append(payload)

        rc = finalize_batch_apply(
            {"action": "apply", "failed": 0},
            _emit,
            [],
        )
        self.assertEqual(rc, 0)
        self.assertEqual(emitted, [{"action": "apply", "failed": 0}])

    def test_returns_one_when_failures_exist(self) -> None:
        rc = finalize_batch_apply(
            {"action": "apply", "failed": 1},
            lambda _payload: None,
            [{"run_id": "run_001", "reason": "apply_failed"}],
        )
        self.assertEqual(rc, 1)


if __name__ == "__main__":
    unittest.main()
