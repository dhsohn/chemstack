import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.cli import main


def _write_config(root: Path, allowed_root: Path, organized_root: Path, orca_executable: Path) -> Path:
    config = root / "orca_auto.yaml"
    config.write_text(
        json.dumps(
            {
                "runtime": {
                    "allowed_root": str(allowed_root),
                    "organized_root": str(organized_root),
                    "default_max_retries": 2,
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
                list_rc = main(["--config", str(config), "list", "--json"])

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

            listed = json.loads(list_stdout.getvalue())
            self.assertEqual(len(listed), 1)
            self.assertEqual(listed[0]["dir"], "project_a/rxn_retry_demo")
            self.assertEqual(listed[0]["status"], "completed")
            self.assertEqual(listed[0]["attempts"], 2)
            self.assertEqual(listed[0]["inp"], "rxn.inp")

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


if __name__ == "__main__":
    unittest.main()
