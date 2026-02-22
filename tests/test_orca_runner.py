import signal
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from core.orca_runner import OrcaRunner


class TestOrcaRunnerCommandConstruction(unittest.TestCase):
    @patch("core.orca_runner.subprocess.Popen")
    def test_command_uses_linux_binary(self, mock_popen: MagicMock) -> None:
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        runner = OrcaRunner("/opt/orca/orca")
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "test.inp"
            inp.write_text("! Opt\n", encoding="utf-8")
            runner.run(inp)

        args, kwargs = mock_popen.call_args
        command = args[0]
        self.assertEqual(command[0], "/opt/orca/orca")
        self.assertEqual(command[1], "test.inp")
        self.assertEqual(len(command), 2)
        self.assertTrue(kwargs["start_new_session"])


class TestOrcaRunnerTermination(unittest.TestCase):
    def test_terminate_noop_when_process_already_exited(self) -> None:
        runner = OrcaRunner("/opt/orca/orca")
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 0
        with patch("core.orca_runner.os.killpg") as killpg:
            runner._terminate_subprocess_tree(mock_proc)
        killpg.assert_not_called()

    @patch("core.orca_runner.os.killpg")
    def test_terminate_sends_sigterm_and_sigkill_on_timeout(self, mock_killpg: MagicMock) -> None:
        runner = OrcaRunner("/opt/orca/orca")
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.pid = 99999
        mock_proc.wait.side_effect = Exception("timeout")

        runner._terminate_subprocess_tree(mock_proc)
        self.assertEqual(
            mock_killpg.mock_calls,
            [
                call(99999, signal.SIGTERM),
                call(99999, signal.SIGKILL),
            ],
        )


if __name__ == "__main__":
    unittest.main()
