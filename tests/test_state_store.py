import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.state_store import acquire_run_lock, load_state, new_state, write_report_files, save_state


class TestStateStore(unittest.TestCase):
    def test_recover_stale_lock_with_dead_pid(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction = Path(td)
            lock_path = reaction / "run.lock"
            lock_path.write_text(
                json.dumps({"pid": 2147483647, "started_at": "2026-01-01T00:00:00+00:00"}) + "\n",
                encoding="utf-8",
            )

            with acquire_run_lock(reaction):
                payload = json.loads(lock_path.read_text(encoding="utf-8"))
                self.assertEqual(payload.get("pid"), os.getpid())
                self.assertIsInstance(payload.get("started_at"), str)
            self.assertFalse(lock_path.exists())

    def test_active_lock_blocks_second_runner(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction = Path(td)
            lock_path = reaction / "run.lock"
            lock_path.write_text(
                json.dumps({"pid": os.getpid(), "started_at": "2026-01-01T00:00:00+00:00"}) + "\n",
                encoding="utf-8",
            )

            with self.assertRaises(RuntimeError):
                with acquire_run_lock(reaction):
                    pass

    def test_active_lock_with_matching_process_ticks_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction = Path(td)
            lock_path = reaction / "run.lock"
            lock_path.write_text(
                json.dumps(
                    {
                        "pid": 12345,
                        "started_at": "2026-01-01T00:00:00+00:00",
                        "process_start_ticks": 111,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with patch("core.state_store.lock_utils.is_process_alive", return_value=True), patch(
                "core.state_store.lock_utils.process_start_ticks", return_value=111
            ):
                with self.assertRaises(RuntimeError):
                    with acquire_run_lock(reaction):
                        pass

    def test_pid_reused_lock_is_recovered_by_start_ticks(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction = Path(td)
            lock_path = reaction / "run.lock"
            lock_path.write_text(
                json.dumps(
                    {
                        "pid": 12345,
                        "started_at": "2026-01-01T00:00:00+00:00",
                        "process_start_ticks": 111,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with patch("core.state_store.lock_utils.is_process_alive", return_value=True), patch(
                "core.state_store.lock_utils.process_start_ticks", return_value=222
            ), patch("core.state_store.lock_utils.current_process_start_ticks", return_value=333):
                with acquire_run_lock(reaction):
                    payload = json.loads(lock_path.read_text(encoding="utf-8"))
                    self.assertEqual(payload.get("pid"), os.getpid())
                    self.assertEqual(payload.get("process_start_ticks"), 333)

    def test_state_and_reports_are_written_without_tmp_leaks(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction = Path(td)
            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n", encoding="utf-8")
            state = new_state(reaction, inp, max_retries=1)

            save_state(reaction, state)
            loaded = load_state(reaction)
            self.assertIsInstance(loaded, dict)

            write_report_files(reaction, state)
            report_json = reaction / "run_report.json"
            report_md = reaction / "run_report.md"
            self.assertTrue(report_json.exists())
            self.assertTrue(report_md.exists())

            tmp_files = list(reaction.glob("*.tmp.*"))
            self.assertEqual(tmp_files, [])


    def test_write_report_files_json_fields(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction = Path(td)
            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n", encoding="utf-8")
            state = new_state(reaction, inp, max_retries=3)
            state["status"] = "completed"
            state["final_result"] = {
                "status": "completed",
                "analyzer_status": "completed",
                "reason": "normal_termination",
                "completed_at": "2026-01-01T00:00:00+00:00",
                "last_out_path": str(reaction / "rxn.out"),
            }
            result = write_report_files(reaction, state)
            report_json_path = Path(result["report_json"])
            report_md_path = Path(result["report_md"])

            report = json.loads(report_json_path.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "completed")
            self.assertEqual(report["max_retries"], 3)
            self.assertEqual(report["attempt_count"], 0)
            self.assertIsNotNone(report["final_result"])

            md = report_md_path.read_text(encoding="utf-8")
            self.assertIn("# ORCA Run Report", md)
            self.assertIn("## Attempts", md)
            self.assertIn("## Final Result", md)

    def test_load_state_returns_none_for_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction = Path(td)
            self.assertIsNone(load_state(reaction))

    def test_load_state_returns_none_for_invalid_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction = Path(td)
            (reaction / "run_state.json").write_text("not valid json!!!", encoding="utf-8")
            self.assertIsNone(load_state(reaction))

    def test_lock_released_after_context_exit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction = Path(td)
            with acquire_run_lock(reaction):
                lock_path = reaction / "run.lock"
                self.assertTrue(lock_path.exists())
            self.assertFalse(lock_path.exists())

    def test_unreadable_lock_pid_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction = Path(td)
            lock_path = reaction / "run.lock"
            lock_path.write_text(json.dumps({"pid": "invalid", "started_at": "x"}) + "\n", encoding="utf-8")
            with self.assertRaises(RuntimeError) as ctx:
                with acquire_run_lock(reaction):
                    pass
            self.assertIn("unreadable", str(ctx.exception).lower())


if __name__ == "__main__":
    unittest.main()
