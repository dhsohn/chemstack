import io
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from core.admission_store import acquire_direct_slot
from core.cli import main
from core.config import load_config
from core.queue_store import list_queue
from core.queue_worker import QueueWorker, WorkerLaunchResult


def _advance_mtime(
    path: Path,
    *,
    delta_seconds: float = 5.0,
    min_exclusive: float | None = None,
) -> None:
    before = path.stat().st_mtime
    baseline = before if min_exclusive is None else max(before, min_exclusive)
    for _ in range(5):
        target = baseline + delta_seconds
        os.utime(path, (target, target))
        after = path.stat().st_mtime
        if after > baseline:
            return
        baseline = max(baseline, after)
        delta_seconds *= 2
    raise AssertionError(f"Failed to advance mtime for {path}")


def _wait_for(predicate, *, timeout_seconds: float, interval_seconds: float = 0.05) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval_seconds)
    raise AssertionError("Timed out while waiting for integration-test condition")


def _write_config(
    root: Path,
    allowed_root: Path,
    organized_root: Path,
    orca_executable: Path,
    *,
    max_concurrent: int = 4,
) -> Path:
    config = root / "orca_auto.yaml"
    config.write_text(
        json.dumps(
            {
                "runtime": {
                    "allowed_root": str(allowed_root),
                    "organized_root": str(organized_root),
                    "default_max_retries": 2,
                    "max_concurrent": max_concurrent,
                },
                "paths": {"orca_executable": str(orca_executable)},
            }
        ),
        encoding="utf-8",
    )
    return config


def _write_fake_orca(binary_path: Path, counter_path: Path, *, mode: str) -> None:
    script = f"""#!/usr/bin/env python3
import sys
from pathlib import Path

MODE = {mode!r}
COUNTER = Path({str(counter_path)!r})


def _increment_counter() -> None:
    count = 0
    if COUNTER.exists():
        try:
            count = int(COUNTER.read_text(encoding="utf-8").strip() or "0")
        except ValueError:
            count = 0
    COUNTER.write_text(str(count + 1), encoding="utf-8")


def main() -> int:
    inp = Path(sys.argv[1]).resolve()
    _increment_counter()
    inp.with_suffix(".xyz").write_text(
        "2\\nfake geometry\\nH 0 0 0\\nH 0 0 0.75\\n",
        encoding="utf-8",
    )

    if MODE == "retry_then_success" and ".retry01" not in inp.name:
        print("SCF NOT CONVERGED AFTER 300 CYCLES")
        return 1

    print("****ORCA TERMINATED NORMALLY****")
    print("TOTAL RUN TIME: 0 days 0 hours 0 minutes 1 seconds 0 msec")
    return 0


raise SystemExit(main())
"""
    binary_path.write_text(script, encoding="utf-8")
    binary_path.chmod(0o755)


class TestIntegrationCliFlow(unittest.TestCase):
    def test_run_inp_retry_flow_generates_reports_and_list_entry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            organized = root / "orca_outputs"
            reaction = allowed / "project_a" / "rxn_retry_demo"
            reaction.mkdir(parents=True)
            organized.mkdir()

            counter = root / "fake_orca_counter.txt"
            fake_orca = root / "fake_orca.py"
            _write_fake_orca(fake_orca, counter, mode="retry_then_success")
            config = _write_config(root, allowed, organized, fake_orca)

            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")

            run_stdout = io.StringIO()
            with patch("sys.stdout", run_stdout):
                rc = main(
                    [
                        "--config",
                        str(config),
                        "run-inp",
                        "--reaction-dir",
                        str(reaction),
                    ]
                )

            list_stdout = io.StringIO()
            with patch("sys.stdout", list_stdout):
                list_rc = main(["--config", str(config), "list"])

            self.assertEqual(rc, 0)
            self.assertEqual(list_rc, 0)
            self.assertEqual(counter.read_text(encoding="utf-8").strip(), "2")

            retry_inp = reaction / "rxn.retry01.inp"
            self.assertTrue(retry_inp.exists())
            retry_text = retry_inp.read_text(encoding="utf-8")
            self.assertIn("TightSCF", retry_text)
            self.assertIn("SlowConv", retry_text)
            self.assertIn("* xyzfile 0 1 rxn.xyz", retry_text)

            state = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))
            self.assertEqual(state["status"], "completed")
            self.assertEqual(len(state["attempts"]), 2)
            self.assertEqual(state["attempts"][0]["analyzer_status"], "error_scf")
            self.assertIn("route_add_tightscf_slowconv", state["attempts"][0]["patch_actions"])
            self.assertIn("geometry_restart_from_rxn.xyz", state["attempts"][0]["patch_actions"])
            self.assertEqual(state["final_result"]["reason"], "normal_termination")
            self.assertEqual(state["final_result"]["last_out_path"], str(reaction / "rxn.retry01.out"))

            report_json = json.loads((reaction / "run_report.json").read_text(encoding="utf-8"))
            self.assertEqual(report_json["attempt_count"], 2)
            self.assertEqual(report_json["final_result"]["reason"], "normal_termination")

            report_md = (reaction / "run_report.md").read_text(encoding="utf-8")
            self.assertIn("attempt_count: `2`", report_md)
            self.assertIn("normal_termination", report_md)

            list_output = list_stdout.getvalue()
            self.assertIn("Simulations: 1 total", list_output)
            self.assertIn("rxn_retry_demo", list_output)
            self.assertIn("completed", list_output)
            self.assertIn("rxn.inp", list_output)

    def test_force_reruns_even_when_completed_output_already_exists(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            organized = root / "orca_outputs"
            reaction = allowed / "rxn_force_demo"
            reaction.mkdir(parents=True)
            organized.mkdir()

            counter = root / "fake_orca_counter.txt"
            fake_orca = root / "fake_orca.py"
            _write_fake_orca(fake_orca, counter, mode="always_success")
            config = _write_config(root, allowed, organized, fake_orca)

            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            (reaction / "rxn.out").write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")

            run_stdout = io.StringIO()
            with patch("sys.stdout", run_stdout):
                rc = main(
                    [
                        "--config",
                        str(config),
                        "run-inp",
                        "--reaction-dir",
                        str(reaction),
                        "--force",
                    ]
                )

            self.assertEqual(rc, 0)
            self.assertEqual(counter.read_text(encoding="utf-8").strip(), "1")

            state = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))
            self.assertEqual(state["status"], "completed")
            self.assertEqual(len(state["attempts"]), 1)
            self.assertEqual(state["final_result"]["reason"], "normal_termination")
            self.assertFalse(state["final_result"].get("skipped_execution", False))

            report_md = (reaction / "run_report.md").read_text(encoding="utf-8")
            self.assertIn("attempt_count: `1`", report_md)

    def test_run_inp_auto_enqueues_and_worker_uses_latest_inp_at_execution_time(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            organized = root / "orca_outputs"
            reaction = allowed / "rxn_queue_latest"
            reaction.mkdir(parents=True)
            organized.mkdir()

            counter = root / "fake_orca_counter.txt"
            fake_orca = root / "fake_orca.py"
            _write_fake_orca(fake_orca, counter, mode="always_success")
            config = _write_config(root, allowed, organized, fake_orca, max_concurrent=1)

            old_inp = reaction / "old.inp"
            old_inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")

            with acquire_direct_slot(allowed, max_concurrent=1, reaction_dir=str(allowed / "slot_holder")):
                run_stdout = io.StringIO()
                with patch(
                    "core.queue_worker.ensure_worker_running",
                    return_value=WorkerLaunchResult(status="already_running", pid=4321),
                ):
                    with patch("sys.stdout", run_stdout):
                        rc = main(
                            [
                                "--config",
                                str(config),
                                "run-inp",
                                "--reaction-dir",
                                str(reaction),
                                "--foreground",
                            ]
                        )

                self.assertEqual(rc, 0)
                self.assertIn("status: queued", run_stdout.getvalue())
                queued_entries = [entry for entry in list_queue(allowed) if entry["reaction_dir"] == str(reaction.resolve())]
                self.assertEqual(len(queued_entries), 1)
                self.assertEqual(queued_entries[0]["status"], "pending")

                new_inp = reaction / "new.inp"
                new_inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
                _advance_mtime(new_inp, min_exclusive=old_inp.stat().st_mtime)

            cfg = load_config(str(config))
            worker = QueueWorker(cfg, str(config), max_concurrent=1)
            worker._fill_slots()
            self.assertEqual(len(worker._running), 1)

            _wait_for(
                lambda: all(job.process.poll() is not None for job in worker._running.values()),
                timeout_seconds=20,
            )
            worker._check_completed_jobs()
            _wait_for(lambda: (reaction / "run_state.json").exists(), timeout_seconds=5)

            state = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))
            self.assertEqual(state["status"], "completed")
            self.assertEqual(Path(state["selected_inp"]).name, "new.inp")
            self.assertEqual(counter.read_text(encoding="utf-8").strip(), "1")

            queued_entries = [entry for entry in list_queue(allowed) if entry["reaction_dir"] == str(reaction.resolve())]
            self.assertEqual(len(queued_entries), 1)
            self.assertEqual(queued_entries[0]["status"], "completed")


if __name__ == "__main__":
    unittest.main()
