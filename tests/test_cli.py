import io
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from core.cli import CONFIG_ENV_VAR, _retry_inp_path, _select_latest_inp, default_config_path, main
from core.orchestrator import _emit
from core.orca_runner import RunResult


class TestCli(unittest.TestCase):
    def _write_config(self, root: Path, allowed_root: Path) -> Path:
        config = root / "orca_auto.yaml"
        config.write_text(
            json.dumps(
                {
                    "runtime": {
                        "allowed_root": str(allowed_root),
                        "default_max_retries": 2,
                    },
                    "paths": {"orca_executable": "/home/daehyupsohn/opt/orca/orca"},
                }
            ),
            encoding="utf-8",
        )
        return config

    def test_rejects_outside_allowed_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            outside = root / "outside"
            allowed.mkdir()
            outside.mkdir()
            (outside / "a.inp").write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            config = self._write_config(root, allowed)

            rc = main(["--config", str(config), "run-inp", "--reaction-dir", str(outside)])
        self.assertEqual(rc, 1)

    def test_select_latest_inp_prefers_base_input(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            reaction = Path(td)
            base = reaction / "rxn.inp"
            retry = reaction / "rxn.retry01.inp"
            base.write_text("! Opt\n", encoding="utf-8")
            time.sleep(0.01)
            retry.write_text("! Opt\n", encoding="utf-8")
            selected = _select_latest_inp(reaction)
        self.assertEqual(selected.name, "rxn.inp")

    def test_retry_inp_path_uses_canonical_base_stem(self) -> None:
        retry_base = Path("/tmp/rxn.retry03.inp")
        retry_next = _retry_inp_path(retry_base, 1)
        self.assertEqual(retry_next.name, "rxn.retry01.inp")

    def test_default_config_path_prefers_env_var(self) -> None:
        with patch.dict(os.environ, {CONFIG_ENV_VAR: "/tmp/custom_orca_auto.yaml"}, clear=False):
            self.assertEqual(default_config_path(), "/tmp/custom_orca_auto.yaml")

    def test_skips_when_existing_out_is_completed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn1"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            (reaction / "rxn.out").write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")
            config = self._write_config(root, root / "orca_runs")

            rc = main(["--config", str(config), "run-inp", "--reaction-dir", str(reaction)])

            state = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))
        self.assertEqual(rc, 0)
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["final_result"]["reason"], "existing_out_completed")

    def test_skips_when_existing_retry_out_is_completed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn1_retry_done"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            retry_inp = reaction / "rxn.retry01.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            retry_inp.write_text("! Opt\n* xyzfile 0 1 rxn.xyz\n", encoding="utf-8")
            (reaction / "rxn.out").write_text("SCF NOT CONVERGED\n", encoding="utf-8")
            (reaction / "rxn.retry01.out").write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")
            config = self._write_config(root, root / "orca_runs")

            with patch("core.cli.OrcaRunner.run") as run_mock:
                rc = main(["--config", str(config), "run-inp", "--reaction-dir", str(reaction)])
            self.assertFalse(run_mock.called)
            state = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))
        self.assertEqual(rc, 0)
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["final_result"]["reason"], "existing_out_completed")
        self.assertEqual(state["final_result"]["last_out_path"], str(reaction / "rxn.retry01.out"))

    def test_skip_existing_completed_out_still_respects_run_lock(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn1_locked"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            (reaction / "rxn.out").write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")
            (reaction / "run.lock").write_text(
                json.dumps({"pid": os.getpid(), "started_at": "2026-02-24T00:00:00+00:00"}) + "\n",
                encoding="utf-8",
            )
            config = self._write_config(root, root / "orca_runs")

            rc = main(["--config", str(config), "run-inp", "--reaction-dir", str(reaction)])

        self.assertEqual(rc, 1)
        self.assertFalse((reaction / "run_state.json").exists())

    def test_retries_and_completes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn2"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            inp.write_text("! OptTS Freq IRC\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            config = self._write_config(root, root / "orca_runs")

            calls = {"n": 0}

            def _fake_run(_self, inp_path: Path) -> RunResult:
                calls["n"] += 1
                out = inp_path.with_suffix(".out")
                if calls["n"] == 1:
                    out.write_text("ORCA finished by error termination in SCF gradient\n", encoding="utf-8")
                    return RunResult(out_path=str(out), return_code=55)
                out.write_text(
                    "\n".join(
                        [
                            "some mode -100.00 cm**-1",
                            "IRC PATH SUMMARY",
                            "****ORCA TERMINATED NORMALLY****",
                        ]
                    ),
                    encoding="utf-8",
                )
                return RunResult(out_path=str(out), return_code=0)

            with patch("core.cli.OrcaRunner.run", new=_fake_run):
                rc = main(
                    [
                        "--config",
                        str(config),
                        "run-inp",
                        "--reaction-dir",
                        str(reaction),
                        "--max-retries",
                        "5",
                    ]
                )

            state = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))
            retry_exists = (reaction / "rxn.retry01.inp").exists()
        self.assertEqual(rc, 0)
        self.assertEqual(calls["n"], 2)
        self.assertTrue(retry_exists)
        self.assertEqual(state["status"], "completed")
        self.assertEqual(len(state["attempts"]), 2)

    def test_disk_io_error_retries_until_limit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn_disk"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            config = self._write_config(root, root / "orca_runs")
            calls = {"n": 0}

            def _fake_run(_self, inp_path: Path) -> RunResult:
                calls["n"] += 1
                out = inp_path.with_suffix(".out")
                out.write_text("COULD NOT WRITE TO DISK\n", encoding="utf-8")
                return RunResult(out_path=str(out), return_code=99)

            with patch("core.cli.OrcaRunner.run", new=_fake_run):
                rc = main(
                    [
                        "--config",
                        str(config),
                        "run-inp",
                        "--reaction-dir",
                        str(reaction),
                        "--max-retries",
                        "2",
                    ]
                )
            state = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))
            retry01_exists = (reaction / "rxn.retry01.inp").exists()
            retry02_exists = (reaction / "rxn.retry02.inp").exists()

        self.assertEqual(rc, 1)
        self.assertEqual(calls["n"], 3)
        self.assertTrue(retry01_exists)
        self.assertTrue(retry02_exists)
        self.assertEqual(state["status"], "failed")
        self.assertEqual(len(state["attempts"]), 3)
        self.assertEqual(state["final_result"]["reason"], "retry_limit_reached")
        self.assertEqual(state["final_result"]["analyzer_status"], "error_disk_io")

    def test_config_default_max_retries_can_exceed_five(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn_disk_long"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            config = root / "orca_auto.yaml"
            config.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": str(root / "orca_runs"),
                            "default_max_retries": 6,
                        },
                        "paths": {"orca_executable": "/home/daehyupsohn/opt/orca/orca"},
                    }
                ),
                encoding="utf-8",
            )
            calls = {"n": 0}

            def _fake_run(_self, inp_path: Path) -> RunResult:
                calls["n"] += 1
                out = inp_path.with_suffix(".out")
                out.write_text("COULD NOT WRITE TO DISK\n", encoding="utf-8")
                return RunResult(out_path=str(out), return_code=99)

            with patch("core.cli.OrcaRunner.run", new=_fake_run):
                rc = main(
                    [
                        "--config",
                        str(config),
                        "run-inp",
                        "--reaction-dir",
                        str(reaction),
                    ]
                )
            state = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))

        self.assertEqual(rc, 1)
        self.assertEqual(calls["n"], 7)
        self.assertEqual(len(state["attempts"]), 7)
        self.assertEqual(state["final_result"]["reason"], "retry_limit_reached")
        self.assertEqual(state["final_result"]["analyzer_status"], "error_disk_io")

    def test_status_command(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn3"
            reaction.mkdir(parents=True)
            config = self._write_config(root, root / "orca_runs")
            state = {
                "run_id": "run_test",
                "reaction_dir": str(reaction),
                "selected_inp": str(reaction / "x.inp"),
                "status": "completed",
                "attempts": [],
                "final_result": {"status": "completed"},
            }
            (reaction / "run_state.json").write_text(json.dumps(state), encoding="utf-8")

            rc = main(["--config", str(config), "status", "--reaction-dir", str(reaction)])
        self.assertEqual(rc, 0)

    def test_retry_limit_already_reached_finalizes_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn4"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            config = self._write_config(root, root / "orca_runs")
            state = {
                "run_id": "run_test_resume",
                "reaction_dir": str(reaction),
                "selected_inp": str(inp),
                "max_retries": 5,
                "status": "running",
                "started_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "attempts": [
                    {
                        "index": 1,
                        "inp_path": str(inp),
                        "out_path": str(reaction / "rxn.out"),
                        "return_code": 1,
                        "analyzer_status": "incomplete",
                        "markers": {},
                        "patch_actions": [],
                        "started_at": "2026-01-01T00:00:00+00:00",
                        "ended_at": "2026-01-01T00:00:01+00:00",
                    },
                    {
                        "index": 2,
                        "inp_path": str(reaction / "rxn.retry01.inp"),
                        "out_path": str(reaction / "rxn.retry01.out"),
                        "return_code": 1,
                        "analyzer_status": "incomplete",
                        "markers": {},
                        "patch_actions": [],
                        "started_at": "2026-01-01T00:00:02+00:00",
                        "ended_at": "2026-01-01T00:00:03+00:00",
                    },
                ],
                "final_result": None,
            }
            (reaction / "run_state.json").write_text(json.dumps(state), encoding="utf-8")

            with patch("core.cli.OrcaRunner.run") as run_mock:
                rc = main(
                    [
                        "--config",
                        str(config),
                        "run-inp",
                        "--reaction-dir",
                        str(reaction),
                        "--max-retries",
                        "0",
                    ]
                )
            self.assertFalse(run_mock.called)
            saved = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))

        self.assertEqual(rc, 1)
        self.assertEqual(saved["status"], "failed")
        self.assertEqual(saved["final_result"]["reason"], "retry_limit_reached")
        self.assertEqual(saved["final_result"]["last_out_path"], str(reaction / "rxn.retry01.out"))

    def test_resume_recreates_missing_retry_input_and_continues(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn_resume"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            config = self._write_config(root, root / "orca_runs")
            state = {
                "run_id": "run_resume_recover",
                "reaction_dir": str(reaction),
                "selected_inp": str(inp),
                "max_retries": 5,
                "status": "running",
                "started_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "attempts": [
                    {
                        "index": 1,
                        "inp_path": str(inp),
                        "out_path": str(reaction / "rxn.out"),
                        "return_code": 1,
                        "analyzer_status": "incomplete",
                        "analyzer_reason": "run_incomplete",
                        "markers": {},
                        "patch_actions": [],
                        "started_at": "2026-01-01T00:00:00+00:00",
                        "ended_at": "2026-01-01T00:00:01+00:00",
                    }
                ],
                "final_result": None,
            }
            (reaction / "run_state.json").write_text(json.dumps(state), encoding="utf-8")
            seen = {"inp_name": ""}

            def _fake_run(_self, inp_path: Path) -> RunResult:
                seen["inp_name"] = inp_path.name
                out = inp_path.with_suffix(".out")
                out.write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")
                return RunResult(out_path=str(out), return_code=0)

            with patch("core.cli.OrcaRunner.run", new=_fake_run):
                rc = main(
                    [
                        "--config",
                        str(config),
                        "run-inp",
                        "--reaction-dir",
                        str(reaction),
                    ]
                )
            saved = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))
            retry_exists = (reaction / "rxn.retry01.inp").exists()

        self.assertEqual(rc, 0)
        self.assertEqual(seen["inp_name"], "rxn.retry01.inp")
        self.assertTrue(retry_exists)
        self.assertEqual(saved["status"], "completed")
        self.assertEqual(len(saved["attempts"]), 2)
        actions = saved["attempts"][0].get("patch_actions", [])
        self.assertTrue(any("resume_recreated_missing_input:rxn.retry01.inp" in action for action in actions))

    def test_resume_completed_attempt_finalizes_without_extra_run(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn_resume_done"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            out = reaction / "rxn.out"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            out.write_text("SCF NOT CONVERGED\n", encoding="utf-8")
            config = self._write_config(root, root / "orca_runs")
            state = {
                "run_id": "run_resume_completed",
                "reaction_dir": str(reaction),
                "selected_inp": str(inp),
                "max_retries": 5,
                "status": "running",
                "started_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "attempts": [
                    {
                        "index": 1,
                        "inp_path": str(inp),
                        "out_path": str(out),
                        "return_code": 0,
                        "analyzer_status": "completed",
                        "analyzer_reason": "normal_termination",
                        "markers": {},
                        "patch_actions": [],
                        "started_at": "2026-01-01T00:00:00+00:00",
                        "ended_at": "2026-01-01T00:00:01+00:00",
                    }
                ],
                "final_result": None,
            }
            (reaction / "run_state.json").write_text(json.dumps(state), encoding="utf-8")

            with patch("core.cli.OrcaRunner.run") as run_mock:
                rc = main(
                    [
                        "--config",
                        str(config),
                        "run-inp",
                        "--reaction-dir",
                        str(reaction),
                    ]
                )
            self.assertFalse(run_mock.called)
            saved = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))

        self.assertEqual(rc, 0)
        self.assertEqual(saved["status"], "completed")
        self.assertEqual(len(saved["attempts"]), 1)
        self.assertEqual(saved["final_result"]["reason"], "normal_termination")
        self.assertTrue(saved["final_result"]["resumed"])

    def test_keyboard_interrupt_stops_run_and_finalizes_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn5"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            config = self._write_config(root, root / "orca_runs")

            def _fake_run(_self, inp_path: Path) -> RunResult:
                raise KeyboardInterrupt

            with patch("core.cli.OrcaRunner.run", new=_fake_run):
                rc = main(
                    [
                        "--config",
                        str(config),
                        "run-inp",
                        "--reaction-dir",
                        str(reaction),
                    ]
                )
            saved = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))

        self.assertEqual(rc, 130)
        self.assertEqual(saved["status"], "failed")
        self.assertEqual(saved["final_result"]["reason"], "interrupted_by_user")
        self.assertEqual(saved["final_result"]["analyzer_status"], "incomplete")
        self.assertEqual(len(saved["attempts"]), 0)

    def test_runner_exception_finalizes_state_with_failure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn6"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            config = self._write_config(root, root / "orca_runs")

            def _fake_run(_self, inp_path: Path) -> RunResult:
                raise RuntimeError("runner exploded")

            with patch("core.cli.OrcaRunner.run", new=_fake_run):
                rc = main(
                    [
                        "--config",
                        str(config),
                        "run-inp",
                        "--reaction-dir",
                        str(reaction),
                    ]
                )
            saved = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))

        self.assertEqual(rc, 1)
        self.assertEqual(saved["status"], "failed")
        self.assertEqual(saved["final_result"]["reason"], "runner_exception")
        self.assertEqual(saved["final_result"]["analyzer_status"], "incomplete")
        self.assertEqual(saved["final_result"]["runner_error"], "runner exploded")
        self.assertEqual(len(saved["attempts"]), 0)

    def test_emit_plain_text_filters_known_keys(self) -> None:
        payload = {
            "status": "completed",
            "reaction_dir": "/tmp/rxn",
            "selected_inp": "/tmp/rxn/rxn.inp",
            "attempt_count": 1,
            "reason": "normal_termination",
            "run_state": "/tmp/rxn/run_state.json",
            "extra_unknown_key": "ignored",
        }
        captured = io.StringIO()
        with patch("sys.stdout", captured):
            _emit(payload, as_json=False)
        output = captured.getvalue()
        self.assertIn("status: completed", output)
        self.assertIn("attempt_count: 1", output)
        self.assertNotIn("extra_unknown_key", output)

    def test_emit_json_includes_all_keys(self) -> None:
        payload = {
            "status": "completed",
            "reaction_dir": "/tmp/rxn",
            "extra_key": "included",
        }
        captured = io.StringIO()
        with patch("sys.stdout", captured):
            _emit(payload, as_json=True)
        parsed = json.loads(captured.getvalue())
        self.assertEqual(parsed["extra_key"], "included")

    def test_error_goes_to_stderr(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            outside = root / "outside"
            allowed.mkdir()
            outside.mkdir()
            (outside / "a.inp").write_text("! Opt\n* xyz 0 1\nH 0 0 0\n*\n", encoding="utf-8")
            config = self._write_config(root, allowed)

            captured_stderr = io.StringIO()
            captured_stdout = io.StringIO()
            with patch("sys.stderr", captured_stderr), patch("sys.stdout", captured_stdout):
                rc = main(["--config", str(config), "run-inp", "--reaction-dir", str(outside)])
        self.assertEqual(rc, 1)
        # Error should go to stderr (via logger.error), not stdout
        self.assertNotIn("allowed root", captured_stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
