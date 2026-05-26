"""Unified list command tests."""

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from chemstack.cli import main
from chemstack.orca.queue_store import dequeue_next, enqueue, mark_completed


class _ListTestBase(unittest.TestCase):
    """Shared helpers for list tests."""

    def _write_config(self, root: Path, allowed_root: Path) -> Path:
        fake_orca = root / "fake_orca"
        fake_orca.touch()
        fake_orca.chmod(0o755)
        config = root / "chemstack.yaml"
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


class TestListEmpty(_ListTestBase):
    def test_list_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            config = self._write_config(root, allowed)

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "queue", "list", "--engine", "orca", "--kind", "job"])

        self.assertEqual(rc, 0)
        output = captured.getvalue()
        self.assertIn("active_simulations: 0", output)
        self.assertNotIn("- ", output)


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
                rc = main(["--config", str(config), "queue", "list", "--engine", "orca", "--kind", "job"])

        self.assertEqual(rc, 0)
        output = captured.getvalue()
        self.assertIn("rxn1", output)
        self.assertIn("rxn2", output)
        self.assertIn("✅", output)
        self.assertIn("▶", output)
        self.assertIn("active_simulations: 1", output)

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
                rc = main(
                    [
                        "--config",
                        str(config),
                        "queue",
                        "list",
                        "--engine",
                        "orca",
                        "--kind",
                        "job",
                        "--status",
                        "running",
                    ]
                )

        self.assertEqual(rc, 0)
        output = captured.getvalue()
        self.assertIn("rxn2", output)
        self.assertNotIn("rxn1", output)
        self.assertIn("active_simulations: 1", output)

    def test_nested_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            self._make_run(allowed / "project" / "rxn1", status="completed")
            self._make_run(allowed / "project" / "rxn2", status="failed")
            config = self._write_config(root, allowed)

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "queue", "list", "--engine", "orca", "--kind", "job"])

        self.assertEqual(rc, 0)
        output = captured.getvalue()
        self.assertIn("rxn1", output)
        self.assertIn("rxn2", output)
        self.assertIn("active_simulations: 0", output)

    def test_tracked_organized_run_is_listed_via_job_locations_index(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            organized = root / "organized" / "project" / "rxn_tracked"
            allowed.mkdir()
            organized.mkdir(parents=True)
            config = self._write_config(root, allowed)

            state = {
                "run_id": "run_tracked",
                "reaction_dir": str(organized),
                "selected_inp": str(organized / "tracked.inp"),
                "max_retries": 2,
                "status": "completed",
                "started_at": "2026-03-01T00:00:00+00:00",
                "updated_at": "2026-03-01T01:00:00+00:00",
                "attempts": [{"index": 1}],
                "final_result": {"status": "completed"},
            }
            (organized / "run_state.json").write_text(
                json.dumps(state, ensure_ascii=True, indent=2),
                encoding="utf-8",
            )
            (allowed / "job_locations.json").write_text(
                json.dumps(
                    [
                        {
                            "job_id": "job_tracked",
                            "app_name": "chemstack_orca",
                            "job_type": "orca_opt",
                            "status": "completed",
                            "original_run_dir": str(allowed / "project" / "rxn_tracked"),
                            "molecule_key": "rxn_tracked",
                            "selected_input_xyz": str(organized / "tracked.inp"),
                            "organized_output_dir": str(organized),
                            "latest_known_path": str(organized),
                            "resource_request": {},
                            "resource_actual": {},
                        }
                    ],
                    ensure_ascii=True,
                    indent=2,
                ),
                encoding="utf-8",
            )

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "queue", "list", "--engine", "orca", "--kind", "job"])

        self.assertEqual(rc, 0)
        output = captured.getvalue()
        self.assertIn("run_tracked", output)
        self.assertIn("✅", output)
        self.assertIn("active_simulations: 0", output)


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
            entry = enqueue(allowed, str(rxn_dir))

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "queue", "list", "--engine", "orca", "--kind", "job"])

        self.assertEqual(rc, 0)
        output = captured.getvalue()
        self.assertIn("active_simulations: 0", output)
        self.assertIn(entry.queue_id, output)
        self.assertIn("ORCA", output)
        self.assertIn("⏳", output)

    def test_filter_pending(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            config = self._write_config(root, allowed)

            rxn_a = allowed / "mol_A"
            rxn_a.mkdir()
            entry = enqueue(allowed, str(rxn_a))
            # Also add a standalone completed run
            self._make_run(allowed / "rxn_done", status="completed")

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(
                    [
                        "--config",
                        str(config),
                        "queue",
                        "list",
                        "--engine",
                        "orca",
                        "--kind",
                        "job",
                        "--status",
                        "pending",
                    ]
                )

        self.assertEqual(rc, 0)
        output = captured.getvalue()
        self.assertIn(entry.queue_id, output)
        self.assertIn("ORCA", output)
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
            entry = enqueue(allowed, str(rxn_dir))
            # Create a run_state for the same directory
            self._make_run(rxn_dir, status="running",
                           started_at="2026-03-02T00:00:00+00:00",
                           inp_name="opt.inp")

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "queue", "list", "--engine", "orca", "--kind", "job"])

        self.assertEqual(rc, 0)
        output = captured.getvalue()
        self.assertIn(entry.queue_id, output)
        self.assertIn("active_simulations: 0", output)
        self.assertIn("ORCA", output)
        self.assertIn("⏳", output)

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
                rc = main(["--config", str(config), "queue", "list", "--engine", "orca", "--kind", "job"])

        self.assertEqual(rc, 0)
        output = captured.getvalue()
        self.assertIn(entry.queue_id, output)
        self.assertIn("✅", output)
        self.assertNotIn("▶", output)


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
            mark_completed(allowed, entry.queue_id)

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "queue", "list", "clear"])

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
                rc = main(["--config", str(config), "queue", "list", "clear"])

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
                rc = main(["--config", str(config), "queue", "list", "clear"])

        self.assertEqual(rc, 0)
        self.assertIn("Nothing to clear.", captured.getvalue())
