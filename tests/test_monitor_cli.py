from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.cli import main


def _make_config(td: str, allowed: Path, organized: Path, **extra) -> str:
    cfg = {
        "runtime": {
            "allowed_root": str(allowed),
            "organized_root": str(organized),
        },
        "paths": {"orca_executable": "/usr/bin/orca"},
    }
    if extra:
        cfg.update(extra)
    cfg_path = Path(td) / "orca_auto.yaml"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
    return str(cfg_path)


class TestMonitorCli(unittest.TestCase):
    def test_oneshot_below_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            allowed.mkdir()
            organized.mkdir()
            cfg_path = _make_config(td, allowed, organized)

            rc = main(["--config", cfg_path, "monitor", "--json", "--threshold-gb", "999"])
            self.assertEqual(rc, 0)

    def test_oneshot_above_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            allowed.mkdir()
            organized.mkdir()
            # Create some data
            (allowed / "data.txt").write_bytes(b"x" * 1000)
            cfg_path = _make_config(td, allowed, organized)

            rc = main(["--config", cfg_path, "monitor", "--json", "--threshold-gb", "0.0000001"])
            self.assertEqual(rc, 1)

    def test_invalid_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            allowed.mkdir()
            organized.mkdir()
            cfg_path = _make_config(td, allowed, organized)

            rc = main(["--config", cfg_path, "monitor", "--threshold-gb", "0"])
            self.assertEqual(rc, 1)

    def test_invalid_interval(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            allowed.mkdir()
            organized.mkdir()
            cfg_path = _make_config(td, allowed, organized)

            rc = main(["--config", cfg_path, "monitor", "--interval-sec", "5"])
            self.assertEqual(rc, 1)

    def test_custom_top_n(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            allowed.mkdir()
            organized.mkdir()
            cfg_path = _make_config(td, allowed, organized)

            rc = main(["--config", cfg_path, "monitor", "--json", "--top-n", "3"])
            self.assertEqual(rc, 0)

    def test_config_disk_monitor_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            allowed.mkdir()
            organized.mkdir()
            cfg_path = _make_config(
                td, allowed, organized,
                disk_monitor={"threshold_gb": 100, "interval_sec": 60, "top_n": 5},
            )

            rc = main(["--config", cfg_path, "monitor", "--json"])
            self.assertEqual(rc, 0)


class TestMonitorWatchMode(unittest.TestCase):
    @patch("core.commands.monitor.time.sleep", side_effect=KeyboardInterrupt)
    @patch("core.commands.monitor.send_batch_summary")
    def test_watch_exits_on_keyboard_interrupt(self, mock_send, mock_sleep) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            allowed.mkdir()
            organized.mkdir()
            cfg_path = _make_config(td, allowed, organized)

            rc = main(["--config", cfg_path, "monitor", "--watch", "--json"])
            self.assertEqual(rc, 0)

    @patch("core.commands.monitor.time.sleep", side_effect=[None, KeyboardInterrupt])
    @patch("core.commands.monitor.send_batch_summary")
    def test_watch_threshold_transition_sends_once(self, mock_send, mock_sleep) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            organized = root / "organized"
            allowed.mkdir()
            organized.mkdir()
            (allowed / "data.txt").write_bytes(b"x" * 1000)
            cfg_path = _make_config(td, allowed, organized)

            # Very low threshold => exceeded on first scan
            rc = main(["--config", cfg_path, "monitor", "--watch",
                        "--threshold-gb", "0.0000001", "--json"])
            self.assertEqual(rc, 0)
            # Should have sent exactly 1 threshold notification
            self.assertEqual(mock_send.call_count, 1)


if __name__ == "__main__":
    unittest.main()
