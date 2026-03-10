"""Telegram bot handler tests."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.config import AppConfig, PathsConfig, RuntimeConfig, TelegramConfig
from core.state_store import STATE_FILE_NAME
from core.telegram_bot import _handle_cancel, _handle_help, _handle_list


def _write_running_state(reaction_dir: Path, *, run_id: str, pid: int) -> None:
    reaction_dir.mkdir(parents=True, exist_ok=True)
    state = {
        "run_id": run_id,
        "reaction_dir": str(reaction_dir),
        "selected_inp": str(reaction_dir / "rxn.inp"),
        "max_retries": 2,
        "status": "running",
        "started_at": "2026-03-01T00:00:00+00:00",
        "updated_at": "2026-03-01T01:00:00+00:00",
        "attempts": [{"index": 1}],
        "final_result": None,
    }
    (reaction_dir / STATE_FILE_NAME).write_text(json.dumps(state), encoding="utf-8")
    (reaction_dir / "run.lock").write_text(json.dumps({"pid": pid}), encoding="utf-8")


class TestTelegramBotHandlers(unittest.TestCase):
    def _make_cfg(self, allowed_root: str) -> AppConfig:
        return AppConfig(
            runtime=RuntimeConfig(allowed_root=allowed_root),
            paths=PathsConfig(),
            telegram=TelegramConfig(bot_token="fake", chat_id="123"),
        )

    def _make_run(self, reaction_dir: Path, *, status: str = "completed") -> None:
        reaction_dir.mkdir(parents=True, exist_ok=True)
        state = {
            "run_id": f"run_{reaction_dir.name}",
            "reaction_dir": str(reaction_dir),
            "selected_inp": str(reaction_dir / "rxn.inp"),
            "max_retries": 2,
            "status": status,
            "started_at": "2026-03-01T00:00:00+00:00",
            "updated_at": "2026-03-01T01:00:00+00:00",
            "attempts": [{"index": 1}],
            "final_result": {"status": status},
        }
        (reaction_dir / "run_state.json").write_text(json.dumps(state), encoding="utf-8")

    def test_handle_list_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            allowed = Path(td) / "orca_runs"
            allowed.mkdir()
            cfg = self._make_cfg(str(allowed))
            result = _handle_list(cfg, "")
        self.assertIn("No simulations found", result)

    def test_handle_list_with_runs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            allowed = Path(td) / "orca_runs"
            self._make_run(allowed / "rxn1", status="completed")
            self._make_run(allowed / "rxn2", status="running")
            cfg = self._make_cfg(str(allowed))
            result = _handle_list(cfg, "")
        self.assertIn("rxn1", result)
        self.assertIn("rxn2", result)
        self.assertIn("(2)", result)

    def test_handle_list_filter(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            allowed = Path(td) / "orca_runs"
            self._make_run(allowed / "rxn1", status="completed")
            self._make_run(allowed / "rxn2", status="running")
            cfg = self._make_cfg(str(allowed))
            result = _handle_list(cfg, "running")
        self.assertIn("rxn2", result)
        self.assertNotIn("rxn1", result)
        self.assertIn("(1)", result)

    def test_handle_help(self) -> None:
        cfg = self._make_cfg("/tmp/nonexistent")
        result = _handle_help(cfg, "")
        self.assertIn("/list", result)
        self.assertIn("/cancel &lt;target&gt;", result)
        self.assertIn("/help", result)

    def test_handle_list_missing_root(self) -> None:
        cfg = self._make_cfg("/tmp/nonexistent_root_xyz")
        result = _handle_list(cfg, "")
        self.assertIn("not found", result)

    @patch("core.cancellation.os.kill")
    @patch("core.cancellation.is_process_alive", return_value=True)
    def test_handle_cancel_direct_run(self, mock_alive, mock_kill) -> None:
        with tempfile.TemporaryDirectory() as td:
            allowed = Path(td) / "orca_runs"
            reaction_dir = allowed / "group_a" / "rxn1"
            _write_running_state(reaction_dir, run_id="run_direct_1", pid=4321)
            cfg = self._make_cfg(str(allowed))

            result = _handle_cancel(cfg, "group_a/rxn1")

        self.assertIn("Cancel requested for running simulation", result)
        self.assertIn("rxn1", result)
        mock_alive.assert_called_once_with(4321)
        mock_kill.assert_called_once()

    def test_handle_cancel_usage(self) -> None:
        cfg = self._make_cfg("/tmp/nonexistent")
        result = _handle_cancel(cfg, "")
        self.assertIn("/cancel &lt;target&gt;", result)


if __name__ == "__main__":
    unittest.main()
