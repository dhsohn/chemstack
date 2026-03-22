from __future__ import annotations

import io
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from core.cli import build_parser
from core import launcher


class _RunningProcess:
    pid = 4321

    def poll(self) -> None:
        return None

    def wait(self) -> int:
        raise AssertionError("wait() should not be called for a running process")


class TestLauncher(unittest.TestCase):
    def test_config_path_from_args_supports_equals_syntax(self) -> None:
        self.assertEqual(
            launcher._config_path_from_args(["--config=/tmp/cfg.yaml", "run-inp"]),
            "/tmp/cfg.yaml",
        )

    def test_detect_queue_subcommand_stops_at_double_dash(self) -> None:
        self.assertIsNone(launcher._detect_queue_subcommand(["queue", "--", "worker"]))

    def test_wants_background_respects_falsey_run_inp_env(self) -> None:
        with patch.dict(
            os.environ,
            {launcher.RUN_INP_BACKGROUND_ENV_VAR: "false"},
            clear=False,
        ):
            self.assertFalse(launcher._wants_background(["run-inp", "--reaction-dir", "/tmp/rxn1"]))

    def test_wants_background_ignores_queue_worker_when_subcommand_missing(self) -> None:
        self.assertFalse(launcher._wants_background(["queue"]))

    def test_run_inp_defaults_to_background_via_subprocess(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            stdout = io.StringIO()
            with (
                patch.dict(os.environ, {launcher.LOG_DIR_ENV_VAR: td}, clear=False),
                patch("core.launcher.time.sleep", return_value=None),
                patch("core.launcher.subprocess.Popen", return_value=_RunningProcess()) as popen_mock,
                redirect_stdout(stdout),
            ):
                rc = launcher.main(["run-inp", "--reaction-dir", "/tmp/rxn1"])
                popen_args = popen_mock.call_args
                self.assertIsNotNone(popen_args)
                log_handle = popen_args.kwargs["stdout"]
                log_path = Path(log_handle.name)
                self.assertTrue(log_path.exists())

        self.assertEqual(rc, 0)
        command = popen_args.args[0]
        self.assertEqual(command[:3], [sys.executable, "-m", "core.cli"])
        self.assertEqual(command[3:], ["run-inp", "--reaction-dir", "/tmp/rxn1"])
        self.assertTrue(popen_args.kwargs["start_new_session"])
        output = stdout.getvalue()
        self.assertIn("status: started", output)
        self.assertIn("pid: 4321", output)
        self.assertIn(f"log: {log_path}", output)

    def test_foreground_flag_delegates_to_cli_main(self) -> None:
        with (
            patch("core.launcher.cli.main", return_value=7) as cli_main_mock,
            patch("core.launcher.subprocess.Popen") as popen_mock,
        ):
            rc = launcher.main(["run-inp", "--reaction-dir", "/tmp/rxn1", "--foreground"])

        self.assertEqual(rc, 7)
        cli_main_mock.assert_called_once_with(["run-inp", "--reaction-dir", "/tmp/rxn1", "--foreground"])
        popen_mock.assert_not_called()

    def test_help_does_not_background_run_inp(self) -> None:
        with (
            patch("core.launcher.cli.main", return_value=0) as cli_main_mock,
            patch("core.launcher.subprocess.Popen") as popen_mock,
        ):
            rc = launcher.main(["run-inp", "--help"])

        self.assertEqual(rc, 0)
        cli_main_mock.assert_called_once_with(["run-inp", "--help"])
        popen_mock.assert_not_called()

    def test_queue_worker_defaults_to_background_via_subprocess(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            stdout = io.StringIO()
            with (
                patch.dict(os.environ, {launcher.LOG_DIR_ENV_VAR: td}, clear=False),
                patch("core.launcher.time.sleep", return_value=None),
                patch("core.launcher.subprocess.Popen", return_value=_RunningProcess()) as popen_mock,
                redirect_stdout(stdout),
            ):
                rc = launcher.main(["queue", "worker"])
                popen_args = popen_mock.call_args
                self.assertIsNotNone(popen_args)
                log_handle = popen_args.kwargs["stdout"]
                log_path = Path(log_handle.name)
                self.assertTrue(log_path.exists())

        self.assertEqual(rc, 0)
        command = popen_args.args[0]
        self.assertEqual(command[:3], [sys.executable, "-m", "core.cli"])
        self.assertEqual(command[3:], ["queue", "worker"])
        self.assertTrue(popen_args.kwargs["start_new_session"])
        self.assertTrue(log_path.name.startswith("queue_worker_"))
        output = stdout.getvalue()
        self.assertIn("status: started", output)
        self.assertIn("pid: 4321", output)
        self.assertIn(f"log: {log_path}", output)

    def test_queue_worker_foreground_flag_delegates_to_cli_main(self) -> None:
        with (
            patch("core.launcher.cli.main", return_value=5) as cli_main_mock,
            patch("core.launcher.subprocess.Popen") as popen_mock,
        ):
            rc = launcher.main(["queue", "worker", "--foreground"])

        self.assertEqual(rc, 5)
        cli_main_mock.assert_called_once_with(["queue", "worker", "--foreground"])
        popen_mock.assert_not_called()

    def test_queue_worker_daemon_flag_delegates_to_cli_main(self) -> None:
        with (
            patch("core.launcher.cli.main", return_value=3) as cli_main_mock,
            patch("core.launcher.subprocess.Popen") as popen_mock,
        ):
            rc = launcher.main(["queue", "worker", "--daemon"])

        self.assertEqual(rc, 3)
        cli_main_mock.assert_called_once_with(["queue", "worker", "--daemon"])
        popen_mock.assert_not_called()

    def test_build_log_file_uses_sanitized_reaction_dir_name(self) -> None:
        with tempfile.TemporaryDirectory() as td, patch.dict(
            os.environ,
            {launcher.LOG_DIR_ENV_VAR: td},
            clear=False,
        ), patch("core.launcher.time.strftime", return_value="20260322_120000"):
            log_file = launcher._build_log_file(
                ["run-inp", "--reaction-dir", "/tmp/rxn name(1)"]
            )

        self.assertEqual(log_file.parent, Path(td))
        self.assertEqual(log_file.name, "run_inp_20260322_120000_rxnname1.log")

    def test_background_runner_reports_finished_for_zero_exit(self) -> None:
        class _FinishedProcess:
            pid = 9876

            def poll(self) -> int:
                return 0

            def wait(self) -> int:
                return 0

        with tempfile.TemporaryDirectory() as td:
            stdout = io.StringIO()
            with (
                patch.dict(os.environ, {launcher.LOG_DIR_ENV_VAR: td}, clear=False),
                patch("core.launcher.time.sleep", return_value=None),
                patch("core.launcher.subprocess.Popen", return_value=_FinishedProcess()),
                redirect_stdout(stdout),
            ):
                rc = launcher._run_in_background(["run-inp", "--reaction-dir", "/tmp/rxn1"])

        self.assertEqual(rc, 0)
        output = stdout.getvalue()
        self.assertIn("status: finished", output)
        self.assertIn("pid: 9876", output)

    def test_background_runner_prints_tail_for_failed_early_exit(self) -> None:
        class _FailedProcess:
            pid = 4321

            def poll(self) -> int:
                return 1

            def wait(self) -> int:
                return 1

        with tempfile.TemporaryDirectory() as td:
            stdout = io.StringIO()
            with (
                patch.dict(os.environ, {launcher.LOG_DIR_ENV_VAR: td}, clear=False),
                patch("core.launcher.time.sleep", return_value=None),
                patch("core.launcher.subprocess.Popen", return_value=_FailedProcess()),
                patch("core.launcher._tail_log_lines", return_value=["line1", "line2"]),
                redirect_stdout(stdout),
            ):
                rc = launcher.main(["run-inp", "--reaction-dir", "/tmp/rxn1"])

        self.assertEqual(rc, 1)
        output = stdout.getvalue()
        self.assertIn("status: failed_early", output)
        self.assertIn("last_log_lines:", output)
        self.assertIn("line1", output)
        self.assertIn("line2", output)


class TestCliParserCompatibility(unittest.TestCase):
    def test_run_inp_parser_accepts_foreground_flag(self) -> None:
        args = build_parser().parse_args(["run-inp", "--reaction-dir", "/tmp/rxn1", "--foreground"])
        self.assertTrue(args.foreground)

    def test_queue_worker_parser_accepts_foreground_flag(self) -> None:
        args = build_parser().parse_args(["queue", "worker", "--foreground"])
        self.assertTrue(args.foreground)

    def test_detect_command_skips_global_flags(self) -> None:
        command, index = launcher._detect_command(["--config", "/tmp/cfg.yaml", "--verbose", "run-inp"])
        self.assertEqual(command, "run-inp")
        self.assertEqual(index, 3)


if __name__ == "__main__":
    unittest.main()
