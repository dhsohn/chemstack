"""Unified list command tests."""

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.cli import main
from core.commands.list_runs import _collect_unified, _format_elapsed, _status_icon
from core.queue_store import dequeue_next, enqueue, mark_completed


class _ListTestBase(unittest.TestCase):
    """Shared helpers for list tests."""

    def _write_config(self, root: Path, allowed_root: Path) -> Path:
        fake_orca = root / "fake_orca"
        fake_orca.touch()
        fake_orca.chmod(0o755)
        config = root / "orca_auto.yaml"
        config.write_text(
            json.dumps({
                "runtime": {
                    "allowed_root": str(allowed_root),
                    "default_max_retries": 2,
                },
                "paths": {"orca_executable": str(fake_orca)},
            }),
            encoding="utf-8",
        )
        return config

    def _make_run(self, reaction_dir: Path, *, status: str = "completed",
                  started_at: str = "2026-03-01T00:00:00+00:00",
                  updated_at: str = "2026-03-01T01:00:00+00:00",
                  inp_name: str = "rxn.inp",
                  run_id: str | None = None) -> None:
        reaction_dir.mkdir(parents=True, exist_ok=True)
        state = {
            "run_id": run_id or f"run_{reaction_dir.name}",
            "reaction_dir": str(reaction_dir),
            "selected_inp": str(reaction_dir / inp_name),
            "max_retries": 2,
            "status": status,
            "started_at": started_at,
            "updated_at": updated_at,
            "attempts": [{"index": 1}],
            "final_result": {"status": status},
        }
        (reaction_dir / "run_state.json").write_text(json.dumps(state), encoding="utf-8")


class TestStatusIcon(unittest.TestCase):
    def test_known_statuses(self) -> None:
        self.assertEqual(_status_icon("pending"), "\u23f3")
        self.assertEqual(_status_icon("running"), "\u25b6")
        self.assertEqual(_status_icon("completed"), "\u2705")
        self.assertEqual(_status_icon("failed"), "\u274c")
        self.assertEqual(_status_icon("cancelled"), "\u26d4")
        self.assertEqual(_status_icon("created"), "\U0001f195")
        self.assertEqual(_status_icon("retrying"), "\U0001f504")

    def test_unknown_status(self) -> None:
        self.assertEqual(_status_icon("mystery"), "?")


class TestFormatElapsed(unittest.TestCase):
    def test_seconds(self) -> None:
        self.assertEqual(_format_elapsed("2026-03-10T00:00:00+00:00", "2026-03-10T00:00:30+00:00"), "30s")

    def test_minutes(self) -> None:
        self.assertEqual(_format_elapsed("2026-03-10T00:00:00+00:00", "2026-03-10T00:05:00+00:00"), "5m 00s")

    def test_hours(self) -> None:
        self.assertEqual(_format_elapsed("2026-03-10T00:00:00+00:00", "2026-03-10T02:30:00+00:00"), "2h 30m")

    def test_invalid_start(self) -> None:
        self.assertEqual(_format_elapsed("invalid", None), "-")

    def test_invalid_end_uses_now(self) -> None:
        result = _format_elapsed("2026-03-10T00:00:00+00:00", "invalid")
        self.assertNotEqual(result, "-")


class TestListEmpty(_ListTestBase):
    def test_list_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            config = self._write_config(root, allowed)

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "list"])

        self.assertEqual(rc, 0)
        self.assertIn("No simulations found", captured.getvalue())


class TestListStandaloneRuns(_ListTestBase):
    """Test listing standalone runs (not queued)."""

    def test_shows_runs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            self._make_run(allowed / "rxn1", status="completed")
            self._make_run(allowed / "rxn2", status="running",
                           started_at="2026-03-02T00:00:00+00:00")
            config = self._write_config(root, allowed)

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "list"])

        self.assertEqual(rc, 0)
        output = captured.getvalue()
        self.assertIn("rxn1", output)
        self.assertIn("rxn2", output)
        self.assertIn("completed", output)
        self.assertIn("running", output)
        self.assertIn("Simulations: 2 total", output)

    def test_filter(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            self._make_run(allowed / "rxn1", status="completed")
            self._make_run(allowed / "rxn2", status="running",
                           started_at="2026-03-02T00:00:00+00:00")
            config = self._write_config(root, allowed)

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "list", "--filter", "running"])

        self.assertEqual(rc, 0)
        output = captured.getvalue()
        self.assertIn("rxn2", output)
        self.assertNotIn("rxn1", output)
        self.assertIn("Simulations: 1 total", output)

    def test_nested_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            self._make_run(allowed / "project" / "rxn1", status="completed")
            self._make_run(allowed / "project" / "rxn2", status="failed")
            config = self._write_config(root, allowed)

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "list"])

        self.assertEqual(rc, 0)
        output = captured.getvalue()
        # Nested dirs show just the leaf name in the DIRECTORY column
        self.assertIn("rxn1", output)
        self.assertIn("rxn2", output)
        self.assertIn("Simulations: 2 total", output)


class TestListQueueEntries(_ListTestBase):
    """Test listing queue entries in unified view."""

    def test_queue_entries_shown(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            config = self._write_config(root, allowed)

            rxn_dir = allowed / "mol_A"
            rxn_dir.mkdir()
            enqueue(allowed, str(rxn_dir))

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "list"])

        self.assertEqual(rc, 0)
        output = captured.getvalue()
        self.assertIn("Simulations:", output)
        self.assertIn("1 pending", output)
        self.assertIn("mol_A", output)
        self.assertIn("ID", output)
        self.assertIn("STATUS", output)

    def test_filter_pending(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            config = self._write_config(root, allowed)

            rxn_a = allowed / "mol_A"
            rxn_a.mkdir()
            enqueue(allowed, str(rxn_a))
            # Also add a standalone completed run
            self._make_run(allowed / "rxn_done", status="completed")

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "list", "--filter", "pending"])

        self.assertEqual(rc, 0)
        output = captured.getvalue()
        self.assertIn("mol_A", output)
        self.assertNotIn("rxn_done", output)

    def test_queue_with_run_state(self) -> None:
        """Queue entry enriched with run_state data."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            config = self._write_config(root, allowed)

            rxn_dir = allowed / "mol_A"
            rxn_dir.mkdir()
            enqueue(allowed, str(rxn_dir))
            # Create a run_state for the same directory
            self._make_run(rxn_dir, status="running",
                           started_at="2026-03-02T00:00:00+00:00",
                           inp_name="opt.inp")

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "list"])

        self.assertEqual(rc, 0)
        output = captured.getvalue()
        self.assertIn("mol_A", output)
        self.assertIn("opt.inp", output)

    def test_active_queue_entry_with_null_run_id_is_not_duplicated(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()

            rxn_dir = allowed / "mol_A"
            rxn_dir.mkdir()
            entry = enqueue(allowed, str(rxn_dir))
            self.assertIsNone(entry["run_id"])
            dequeue_next(allowed)
            self._make_run(
                rxn_dir,
                status="retrying",
                started_at="2026-03-02T00:00:00+00:00",
                updated_at="2026-03-02T01:00:00+00:00",
                inp_name="opt.retry01.inp",
                run_id="run_retry_1",
            )

            rows = _collect_unified(allowed)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], entry["queue_id"])
        self.assertEqual(rows[0]["status"], "pending")
        self.assertEqual(rows[0]["inp"], "opt.retry01.inp")

    def test_stale_terminal_queue_entry_does_not_hide_newer_standalone_run(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()

            rxn_dir = allowed / "mol_A"
            rxn_dir.mkdir()
            entry = enqueue(allowed, str(rxn_dir))
            self.assertTrue(mark_completed(allowed, entry["queue_id"], run_id="run_old"))

            self._make_run(
                rxn_dir,
                status="completed",
                inp_name="rerun.inp",
                run_id="run_new",
                started_at="2026-03-03T00:00:00+00:00",
                updated_at="2026-03-03T01:00:00+00:00",
            )

            rows = _collect_unified(allowed)

        self.assertEqual(len(rows), 2)

        queue_row = next(r for r in rows if r["id"] == entry["queue_id"])
        self.assertEqual(queue_row["status"], "completed")
        self.assertEqual(queue_row["inp"], "")
        self.assertEqual(queue_row["attempts"], "-")

        standalone_row = next(r for r in rows if r["id"] == "run_new")
        self.assertEqual(standalone_row["status"], "completed")
        self.assertEqual(standalone_row["inp"], "rerun.inp")
        self.assertEqual(standalone_row["attempts"], "1")

    def test_list_reconciles_orphaned_running_queue_entry_from_run_report(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            config = self._write_config(root, allowed)

            rxn_dir = allowed / "mol_done"
            rxn_dir.mkdir()
            entry = enqueue(allowed, str(rxn_dir))
            dequeue_next(allowed)
            (rxn_dir / "run_report.json").write_text(
                json.dumps(
                    {
                        "run_id": "run_done_1",
                        "status": "completed",
                        "updated_at": "2026-03-10T05:00:00+00:00",
                        "final_result": {
                            "status": "completed",
                            "completed_at": "2026-03-10T04:59:59+00:00",
                        },
                    }
                ),
                encoding="utf-8",
            )

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "list"])

        self.assertEqual(rc, 0)
        output = captured.getvalue()
        self.assertIn(entry["queue_id"], output)
        self.assertIn("completed", output)
        self.assertNotIn("running", output)


class TestListClear(_ListTestBase):
    """Test list clear subaction."""

    def test_clear_queue_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            config = self._write_config(root, allowed)

            rxn_dir = allowed / "mol_A"
            rxn_dir.mkdir()
            entry = enqueue(allowed, str(rxn_dir))
            mark_completed(allowed, entry["queue_id"])

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "list", "clear"])

        self.assertEqual(rc, 0)
        self.assertIn("Cleared", captured.getvalue())

    def test_clear_standalone_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            self._make_run(allowed / "rxn1", status="completed")
            self._make_run(allowed / "rxn2", status="running",
                           started_at="2026-03-02T00:00:00+00:00")
            config = self._write_config(root, allowed)

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "list", "clear"])

            self.assertEqual(rc, 0)
            # rxn1 (completed) should be cleared
            self.assertFalse((allowed / "rxn1" / "run_state.json").exists())
            # rxn2 (running) should remain
            self.assertTrue((allowed / "rxn2" / "run_state.json").exists())

    def test_clear_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            config = self._write_config(root, allowed)

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "list", "clear"])

        self.assertEqual(rc, 0)
        self.assertIn("Cleared 0", captured.getvalue())


if __name__ == "__main__":
    unittest.main()
