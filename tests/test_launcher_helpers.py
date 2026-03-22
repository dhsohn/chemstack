from __future__ import annotations

import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from core import launcher


class _FailedProcess:
    pid = 4321

    def poll(self) -> int:
        return 1

    def wait(self) -> int:
        return 1


class TestLauncherHelpers(unittest.TestCase):
    def test_detect_command_skips_global_flags(self) -> None:
        command, index = launcher._detect_command(
            ["--config", "/tmp/orca_auto.yaml", "--verbose", "run-inp", "--reaction-dir", "/tmp/rxn"]
        )

        self.assertEqual(command, "run-inp")
        self.assertEqual(index, 3)

    def test_detect_queue_subcommand_skips_queue_flags(self) -> None:
        subcommand = launcher._detect_queue_subcommand(["queue", "--foreground", "worker"])

        self.assertEqual(subcommand, "worker")

    def test_background_requested_by_default_honors_falsey_values(self) -> None:
        for raw in ("0", "false", "no", "off"):
            with self.subTest(raw=raw):
                with patch.dict(os.environ, {launcher.RUN_INP_BACKGROUND_ENV_VAR: raw}, clear=False):
                    self.assertFalse(
                        launcher._background_requested_by_default(launcher.RUN_INP_BACKGROUND_ENV_VAR)
                    )

    def test_default_log_dir_uses_repo_adjacent_logs_for_config_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config_path = root / "config" / "orca_auto.yaml"
            config_path.parent.mkdir()
            config_path.write_text("{}", encoding="utf-8")

            log_dir = launcher._default_log_dir(["--config", str(config_path), "run-inp"])

        self.assertEqual(log_dir, root / "logs")

    def test_build_log_file_sanitizes_reaction_dir_name(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with patch.dict(os.environ, {launcher.LOG_DIR_ENV_VAR: td}, clear=False), patch(
                "core.launcher.time.strftime",
                return_value="20260322_120000",
            ):
                log_file = launcher._build_log_file(
                    ["run-inp", "--reaction-dir", "/tmp/My Rxn!!"]
                )

        self.assertEqual(log_file.name, "run_inp_20260322_120000_MyRxn.log")

    def test_run_in_background_prints_tail_for_failed_early_process(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            stdout = io.StringIO()
            with (
                patch.dict(os.environ, {launcher.LOG_DIR_ENV_VAR: td}, clear=False),
                patch("core.launcher.time.sleep", return_value=None),
                patch("core.launcher.subprocess.Popen", return_value=_FailedProcess()),
                patch("core.launcher._tail_log_lines", return_value=["line 1", "line 2"]),
                redirect_stdout(stdout),
            ):
                rc = launcher._run_in_background(["run-inp", "--reaction-dir", "/tmp/rxn1"])

        self.assertEqual(rc, 1)
        output = stdout.getvalue()
        self.assertIn("status: failed_early", output)
        self.assertIn("last_log_lines:", output)
        self.assertIn("line 1", output)
        self.assertIn("line 2", output)

    def test_run_in_background_reports_spawn_failure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            stdout = io.StringIO()
            with (
                patch.dict(os.environ, {launcher.LOG_DIR_ENV_VAR: td}, clear=False),
                patch("core.launcher.subprocess.Popen", side_effect=OSError("boom")),
                redirect_stdout(stdout),
            ):
                rc = launcher._run_in_background(["queue", "worker"])

        self.assertEqual(rc, 1)
        output = stdout.getvalue()
        self.assertIn("status: failed_early", output)
        self.assertIn("pid: unavailable", output)


if __name__ == "__main__":
    unittest.main()
