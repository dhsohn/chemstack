"""list 커맨드 테스트."""

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.cli import main


class TestListRuns(unittest.TestCase):
    def _write_config(self, root: Path, allowed_root: Path) -> Path:
        config = root / "orca_auto.yaml"
        config.write_text(
            json.dumps({
                "runtime": {
                    "allowed_root": str(allowed_root),
                    "default_max_retries": 2,
                },
                "paths": {"orca_executable": "/home/daehyupsohn/opt/orca/orca"},
            }),
            encoding="utf-8",
        )
        return config

    def _make_run(self, reaction_dir: Path, *, status: str = "completed",
                  started_at: str = "2026-03-01T00:00:00+00:00",
                  updated_at: str = "2026-03-01T01:00:00+00:00",
                  inp_name: str = "rxn.inp") -> None:
        reaction_dir.mkdir(parents=True, exist_ok=True)
        state = {
            "run_id": f"run_{reaction_dir.name}",
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
        self.assertIn("등록된 작업이 없습니다", captured.getvalue())

    def test_list_shows_runs(self) -> None:
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
        self.assertIn("Total: 2", output)

    def test_list_filter(self) -> None:
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
        self.assertIn("Total: 1", output)

    def test_list_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            self._make_run(allowed / "rxn1", status="completed")
            config = self._write_config(root, allowed)

            captured = io.StringIO()
            with patch("sys.stdout", captured):
                rc = main(["--config", str(config), "list", "--json"])

        self.assertEqual(rc, 0)
        data = json.loads(captured.getvalue())
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["dir"], "rxn1")
        self.assertEqual(data[0]["status"], "completed")

    def test_list_nested_dirs(self) -> None:
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
        self.assertIn("project/rxn1", output)
        self.assertIn("project/rxn2", output)
        self.assertIn("Total: 2", output)

    def test_list_elapsed_formatting(self) -> None:
        from core.commands.list_runs import _elapsed_text
        self.assertEqual(_elapsed_text(30), "30s")
        self.assertEqual(_elapsed_text(90), "1m 30s")
        self.assertEqual(_elapsed_text(3700), "1h 01m")
        self.assertEqual(_elapsed_text(-1), "-")


if __name__ == "__main__":
    unittest.main()
