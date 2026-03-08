"""Telegram bot handler tests."""

import json
import tempfile
import unittest
from pathlib import Path

from core.config import AppConfig, PathsConfig, RuntimeConfig, TelegramConfig
from core.telegram_bot import _handle_help, _handle_list


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
        self.assertIn("No registered runs found", result)

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
        self.assertIn("/help", result)

    def test_handle_list_missing_root(self) -> None:
        cfg = self._make_cfg("/tmp/nonexistent_root_xyz")
        result = _handle_list(cfg, "")
        self.assertIn("not found", result)


if __name__ == "__main__":
    unittest.main()
