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


class TestCliParserCompatibility(unittest.TestCase):
    def test_run_inp_parser_accepts_foreground_flag(self) -> None:
        args = build_parser().parse_args(["run-inp", "--reaction-dir", "/tmp/rxn1", "--foreground"])
        self.assertTrue(args.foreground)

    def test_queue_worker_parser_accepts_foreground_flag(self) -> None:
        args = build_parser().parse_args(["queue", "worker", "--foreground"])
        self.assertTrue(args.foreground)


if __name__ == "__main__":
    unittest.main()
