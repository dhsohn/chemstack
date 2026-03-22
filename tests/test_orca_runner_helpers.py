from __future__ import annotations

import signal
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from core.orca_runner import OrcaRunner


def test_ensure_trailing_newline_only_appends_when_needed(tmp_path: Path) -> None:
    runner = OrcaRunner("/opt/orca/orca")

    empty_inp = tmp_path / "empty.inp"
    empty_inp.write_bytes(b"")
    runner._ensure_trailing_newline(empty_inp)
    assert empty_inp.read_bytes() == b""

    newline_inp = tmp_path / "newline.inp"
    newline_inp.write_bytes(b"! Opt\n")
    runner._ensure_trailing_newline(newline_inp)
    assert newline_inp.read_bytes() == b"! Opt\n"

    missing_newline_inp = tmp_path / "missing_newline.inp"
    missing_newline_inp.write_bytes(b"! Opt")
    runner._ensure_trailing_newline(missing_newline_inp)
    assert missing_newline_inp.read_bytes() == b"! Opt\n"


def test_terminate_subprocess_tree_falls_back_to_terminate_when_sigterm_group_kill_fails() -> None:
    runner = OrcaRunner("/opt/orca/orca")
    proc = MagicMock()
    proc.poll.return_value = None
    proc.pid = 4242
    proc.wait.return_value = 0

    with patch("core.orca_runner.os.killpg", side_effect=Exception("no pg")):
        runner._terminate_subprocess_tree(proc)

    proc.terminate.assert_called_once()


def test_terminate_subprocess_tree_falls_back_to_proc_kill_when_sigkill_group_kill_fails() -> None:
    runner = OrcaRunner("/opt/orca/orca")
    proc = MagicMock()
    proc.poll.return_value = None
    proc.pid = 4343
    proc.wait.side_effect = Exception("timeout")

    with patch("core.orca_runner.os.killpg", side_effect=[None, Exception("no pg kill")]):
        runner._terminate_subprocess_tree(proc)

    proc.kill.assert_called_once()


def test_terminate_subprocess_tree_ignores_terminate_failure_when_sigterm_group_kill_fails() -> None:
    runner = OrcaRunner("/opt/orca/orca")
    proc = MagicMock()
    proc.poll.return_value = None
    proc.pid = 4444
    proc.terminate.side_effect = Exception("terminate failed")
    proc.wait.return_value = 0

    with patch("core.orca_runner.os.killpg", side_effect=Exception("no pg")):
        runner._terminate_subprocess_tree(proc)

    proc.terminate.assert_called_once()


def test_terminate_subprocess_tree_ignores_proc_kill_failure_when_sigkill_group_kill_fails() -> None:
    runner = OrcaRunner("/opt/orca/orca")
    proc = MagicMock()
    proc.poll.return_value = None
    proc.pid = 4545
    proc.wait.side_effect = Exception("timeout")
    proc.kill.side_effect = Exception("kill failed")

    with patch("core.orca_runner.os.killpg", side_effect=[None, Exception("no pg kill")]):
        runner._terminate_subprocess_tree(proc)

    proc.kill.assert_called_once()


@patch("core.orca_runner.subprocess.Popen")
@patch("core.orca_runner.signal.getsignal", return_value=signal.SIG_DFL)
@patch("core.orca_runner.signal.signal", side_effect=ValueError("not main thread"))
def test_run_handles_signal_install_value_error(
    _mock_signal: MagicMock,
    _mock_getsignal: MagicMock,
    mock_popen: MagicMock,
) -> None:
    proc = MagicMock()
    proc.wait.return_value = 0
    mock_popen.return_value = proc

    runner = OrcaRunner("/opt/orca/orca")
    with tempfile.TemporaryDirectory() as td:
        inp = Path(td) / "test.inp"
        inp.write_text("! Opt\n", encoding="utf-8")
        result = runner.run(inp)

    assert result.return_code == 0
    assert result.out_path.endswith("test.out")


@patch("core.orca_runner.subprocess.Popen")
@patch("core.orca_runner.signal.getsignal", return_value=signal.SIG_DFL)
def test_run_ignores_restore_signal_value_error(
    _mock_getsignal: MagicMock,
    mock_popen: MagicMock,
) -> None:
    proc = MagicMock()
    proc.wait.return_value = 0
    mock_popen.return_value = proc

    restore_calls = []

    def _signal(_sig: int, handler):
        restore_calls.append(handler)
        if len(restore_calls) == 2:
            raise ValueError("restore failed")
        return None

    runner = OrcaRunner("/opt/orca/orca")
    with patch("core.orca_runner.signal.signal", side_effect=_signal):
        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "test.inp"
            inp.write_text("! Opt\n", encoding="utf-8")
            result = runner.run(inp)

    assert result.return_code == 0
    assert len(restore_calls) == 2
