import io
import json
import logging
import os
import tempfile
import time
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, patch

from core.commands._helpers import CONFIG_ENV_VAR, _emit, default_config_path
from core.commands.run_inp import _cmd_run_inp_execute, _retry_inp_path, _select_latest_inp
from core.orca_runner import RunResult, WorkerShutdownInterrupt

try:
    from core.cli import _configure_logging, _remove_managed_handlers, build_parser, cmd_bot, cmd_queue, main
except ImportError as exc:
    _CLI_IMPORT_ERROR = exc

    def _raise_cli_import_error(*args, **kwargs):
        raise _CLI_IMPORT_ERROR

    _configure_logging = _raise_cli_import_error
    _remove_managed_handlers = _raise_cli_import_error
    build_parser = _raise_cli_import_error
    cmd_bot = _raise_cli_import_error
    cmd_queue = _raise_cli_import_error
    main = _raise_cli_import_error

try:
    from core.launcher import main as launcher_main
except ImportError as exc:
    _LAUNCHER_IMPORT_ERROR = exc

    def launcher_main(*args, **kwargs):
        raise _LAUNCHER_IMPORT_ERROR


class TestCli(unittest.TestCase):
    def _write_config(self, root: Path, allowed_root: Path, *, telegram_enabled: bool = False) -> Path:
        fake_orca = root / "fake_orca"
        fake_orca.touch()
        fake_orca.chmod(0o755)
        payload = {
            "runtime": {
                "allowed_root": str(allowed_root),
                "default_max_retries": 2,
            },
            "paths": {"orca_executable": str(fake_orca)},
        }
        if telegram_enabled:
            payload["telegram"] = {
                "bot_token": "123:ABC",
                "chat_id": "999",
            }
        config = root / "orca_auto.yaml"
        config.write_text(
            json.dumps(payload),
            encoding="utf-8",
        )
        return config

    def _run_internal_execute(self, config: Path, reaction_dir: Path, *, force: bool = False) -> int:
        return _cmd_run_inp_execute(
            Namespace(
                config=str(config),
                reaction_dir=str(reaction_dir),
                force=force,
            )
        )

    def test_rejects_outside_allowed_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "allowed"
            outside = root / "outside"
            allowed.mkdir()
            outside.mkdir()
            (outside / "a.inp").write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            config = self._write_config(root, allowed)

            rc = main(["--config", str(config), "run-dir", str(outside)])
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

    def test_run_dir_accepts_queue_submission_flags(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "run-dir",
                "/tmp/rxn",
                "--priority",
                "3",
                "--max-cores",
                "16",
                "--max-memory-gb",
                "64",
                "--queue-only",
            ]
        )

        self.assertEqual(args.command, "run-dir")
        self.assertEqual(args.path, "/tmp/rxn")
        self.assertEqual(args.priority, 3)
        self.assertEqual(args.max_cores, 16)
        self.assertEqual(args.max_memory_gb, 64)
        self.assertTrue(args.queue_only)

    def test_run_dir_rejects_foreground_flag(self) -> None:
        parser = build_parser()
        with self.assertRaises(SystemExit) as exc:
            parser.parse_args(["run-dir", "/tmp/rxn", "--foreground"])
        self.assertEqual(exc.exception.code, 2)

    def test_hidden_run_job_command_is_parsed(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["run-job", "--reaction-dir", "/tmp/rxn"])

        self.assertEqual(args.command, "run-job")
        self.assertEqual(args.reaction_dir, "/tmp/rxn")

    def test_queue_worker_accepts_auto_organize_flag(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["queue", "worker", "--auto-organize"])

        self.assertEqual(args.command, "queue")
        self.assertEqual(args.queue_command, "worker")
        self.assertTrue(args.auto_organize)
        self.assertFalse(args.no_auto_organize)

    def test_queue_worker_accepts_no_auto_organize_flag(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["queue", "worker", "--no-auto-organize"])

        self.assertEqual(args.command, "queue")
        self.assertEqual(args.queue_command, "worker")
        self.assertFalse(args.auto_organize)
        self.assertTrue(args.no_auto_organize)

    def test_queue_worker_rejects_conflicting_auto_organize_flags(self) -> None:
        parser = build_parser()
        with self.assertRaises(SystemExit) as exc:
            parser.parse_args(["queue", "worker", "--auto-organize", "--no-auto-organize"])

        self.assertEqual(exc.exception.code, 2)

    @patch("core.cli.cmd_run_job", return_value=9)
    def test_main_dispatches_hidden_run_job_command(self, mock_cmd_run_job: MagicMock) -> None:
        rc = main(["run-job", "--reaction-dir", "/tmp/rxn"])

        self.assertEqual(rc, 9)
        mock_cmd_run_job.assert_called_once()

    @patch("core.cli.cmd_run_inp", return_value=8)
    def test_main_dispatches_run_dir_command(self, mock_cmd_run_inp: MagicMock) -> None:
        rc = main(["run-dir", "/tmp/rxn"])

        self.assertEqual(rc, 8)
        mock_cmd_run_inp.assert_called_once()

    def test_launcher_main_delegates_to_cli_main(self) -> None:
        self.assertIs(launcher_main, main)

    def test_configure_logging_replaces_previous_orca_auto_handler(self) -> None:
        root_logger = logging.getLogger()
        original_level = root_logger.level
        original_handlers = list(root_logger.handlers)
        for handler in list(root_logger.handlers):
            if getattr(handler, "_orca_auto_managed_handler", False):
                root_logger.removeHandler(handler)
                handler.close()

        try:
            _configure_logging(Namespace(verbose=False, log_file=None))
            _configure_logging(Namespace(verbose=True, log_file=None))

            managed_handlers = [
                handler for handler in root_logger.handlers
                if getattr(handler, "_orca_auto_managed_handler", False)
            ]
            self.assertEqual(len(managed_handlers), 1)
            self.assertEqual(root_logger.level, logging.DEBUG)
        finally:
            for handler in list(root_logger.handlers):
                if getattr(handler, "_orca_auto_managed_handler", False):
                    root_logger.removeHandler(handler)
                    handler.close()
            root_logger.setLevel(original_level)
            current_handlers = list(root_logger.handlers)
            for handler in current_handlers:
                if handler not in original_handlers:
                    root_logger.removeHandler(handler)
            for handler in original_handlers:
                if handler not in root_logger.handlers:
                    root_logger.addHandler(handler)

    def test_queue_add_is_not_a_valid_subcommand(self) -> None:
        parser = build_parser()
        with self.assertRaises(SystemExit) as exc:
            parser.parse_args(["queue", "add"])
        self.assertEqual(exc.exception.code, 2)

    @patch("core.cli._run_bot", return_value=7)
    @patch("core.config.load_config")
    def test_cmd_bot_loads_config_and_returns_int(self, mock_load: MagicMock, mock_run_bot: MagicMock) -> None:
        args = Namespace(config="orca_auto.yaml")

        rc = cmd_bot(args)

        self.assertEqual(rc, 7)
        mock_load.assert_called_once_with("orca_auto.yaml")
        mock_run_bot.assert_called_once_with(mock_load.return_value)

    def test_cmd_queue_invalid_subcommand_prints_usage_and_returns_1(self) -> None:
        args = Namespace(queue_command="unknown")
        buf = io.StringIO()

        with unittest.mock.patch("sys.stdout", buf):
            rc = cmd_queue(args)

        self.assertEqual(rc, 1)
        self.assertIn("Usage: orca_auto queue", buf.getvalue())

    @patch("core.cli._cmd_queue_worker", return_value=4)
    def test_cmd_queue_dispatches_to_selected_subcommand(self, mock_worker: MagicMock) -> None:
        args = Namespace(queue_command="worker")

        rc = cmd_queue(args)

        self.assertEqual(rc, 4)
        mock_worker.assert_called_once_with(args)

    @patch("core.cli._remove_managed_handlers")
    @patch("core.cli.logging.handlers.RotatingFileHandler")
    @patch("core.cli.logging.getLogger")
    def test_configure_logging_uses_rotating_file_handler_when_log_file_is_set(
        self,
        mock_get_logger: MagicMock,
        mock_rotating_handler: MagicMock,
        mock_remove_handlers: MagicMock,
    ) -> None:
        root_logger = MagicMock()
        mock_get_logger.return_value = root_logger
        handler = MagicMock(spec=logging.Handler)
        mock_rotating_handler.return_value = handler

        _configure_logging(Namespace(verbose=False, log_file="/tmp/orca_auto.log"))

        mock_remove_handlers.assert_called_once_with(root_logger)
        mock_rotating_handler.assert_called_once_with(
            "/tmp/orca_auto.log",
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        root_logger.setLevel.assert_called_once_with(logging.INFO)
        handler.setFormatter.assert_called_once()
        root_logger.addHandler.assert_called_once_with(handler)

    def test_remove_managed_handlers_ignores_close_errors(self) -> None:
        root_logger = logging.Logger("test_cli_remove_managed")
        root_logger.handlers = []

        unmanaged = logging.StreamHandler()
        managed = MagicMock(spec=logging.Handler)
        setattr(managed, "_orca_auto_managed_handler", True)
        managed.close.side_effect = RuntimeError("boom")

        root_logger.addHandler(unmanaged)
        root_logger.addHandler(managed)

        _remove_managed_handlers(root_logger)

        self.assertIn(unmanaged, root_logger.handlers)
        self.assertNotIn(managed, root_logger.handlers)

    def test_skips_when_existing_out_is_completed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn1"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            (reaction / "rxn.out").write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")
            config = self._write_config(root, root / "orca_runs")

            rc = main(["--config", str(config), "run-dir", str(reaction)])

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

            with patch("core.commands.run_inp.OrcaRunner.run") as run_mock:
                rc = main(["--config", str(config), "run-dir", str(reaction)])
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

            rc = main(["--config", str(config), "run-dir", str(reaction)])

        self.assertEqual(rc, 1)
        self.assertFalse((reaction / "run_state.json").exists())

    def test_skip_existing_completed_out_reuses_worker_shutdown_run_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn1_resume_skip"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            out = reaction / "rxn.out"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            out.write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")
            config = self._write_config(root, root / "orca_runs")
            state = {
                "run_id": "run_resume_skip_existing_out",
                "reaction_dir": str(reaction),
                "selected_inp": str(inp),
                "max_retries": 5,
                "status": "failed",
                "started_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "attempts": [
                    {
                        "index": 1,
                        "inp_path": str(inp),
                        "out_path": str(out),
                        "return_code": 1,
                        "analyzer_status": "incomplete",
                        "analyzer_reason": "run_incomplete",
                        "markers": {},
                        "patch_actions": [],
                        "started_at": "2026-01-01T00:00:00+00:00",
                        "ended_at": "2026-01-01T00:00:01+00:00",
                    }
                ],
                "final_result": {
                    "status": "failed",
                    "analyzer_status": "incomplete",
                    "reason": "worker_shutdown",
                    "completed_at": "2026-01-01T00:00:02+00:00",
                    "last_out_path": str(out),
                },
            }
            (reaction / "run_state.json").write_text(json.dumps(state), encoding="utf-8")

            with patch("core.commands.run_inp.OrcaRunner.run") as run_mock:
                rc = main(["--config", str(config), "run-dir", str(reaction)])
            self.assertFalse(run_mock.called)
            saved = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))

        self.assertEqual(rc, 0)
        self.assertEqual(saved["run_id"], "run_resume_skip_existing_out")
        self.assertEqual(saved["status"], "completed")
        self.assertEqual(saved["final_result"]["reason"], "existing_out_completed")
        self.assertTrue(saved["final_result"]["resumed"])

    def test_worker_shutdown_stops_run_and_finalizes_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn5_worker_shutdown"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            config = self._write_config(root, root / "orca_runs")

            def _fake_run(_self, inp_path: Path) -> RunResult:
                raise WorkerShutdownInterrupt

            with patch("core.commands.run_inp.OrcaRunner.run", new=_fake_run):
                rc = self._run_internal_execute(config, reaction)
            saved = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))

        self.assertEqual(rc, 143)
        self.assertEqual(saved["status"], "failed")
        self.assertEqual(saved["final_result"]["reason"], "worker_shutdown")
        self.assertEqual(saved["final_result"]["analyzer_status"], "incomplete")
        self.assertEqual(len(saved["attempts"]), 0)

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

            with patch("core.commands.run_inp.OrcaRunner.run", new=_fake_run):
                rc = self._run_internal_execute(config, reaction)

            state = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))
            retry_exists = (reaction / "rxn.retry01.inp").exists()
        self.assertEqual(rc, 0)
        self.assertEqual(calls["n"], 2)
        self.assertTrue(retry_exists)
        self.assertEqual(state["status"], "completed")
        self.assertEqual(len(state["attempts"]), 2)

    @patch("core.commands.run_inp.notify_retry_event", return_value=True)
    def test_retry_flow_sends_telegram_notification_when_configured(self, mock_notify: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn_notify"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            config = self._write_config(root, root / "orca_runs", telegram_enabled=True)

            calls = {"n": 0}

            def _fake_run(_self, inp_path: Path) -> RunResult:
                calls["n"] += 1
                inp_path.with_suffix(".xyz").write_text(
                    "2\nretry geometry\nH 0 0 0\nH 0 0 0.75\n",
                    encoding="utf-8",
                )
                out = inp_path.with_suffix(".out")
                if calls["n"] == 1:
                    out.write_text("SCF NOT CONVERGED AFTER 300 CYCLES\n", encoding="utf-8")
                    return RunResult(out_path=str(out), return_code=1)
                out.write_text(
                    "****ORCA TERMINATED NORMALLY****\nTOTAL RUN TIME: 0 days 0 hours 0 minutes 1 seconds 0 msec\n",
                    encoding="utf-8",
                )
                return RunResult(out_path=str(out), return_code=0)

            with patch("core.commands.run_inp.OrcaRunner.run", new=_fake_run):
                rc = self._run_internal_execute(config, reaction)

        self.assertEqual(rc, 0)
        self.assertEqual(calls["n"], 2)
        mock_notify.assert_called_once()
        notify_cfg = mock_notify.call_args.args[0]
        event = mock_notify.call_args.args[1]
        self.assertEqual(notify_cfg.chat_id, "999")
        self.assertEqual(event["analyzer_status"], "error_scf")
        self.assertEqual(event["analyzer_reason"], "scf_not_converged")
        self.assertTrue(event["failed_inp"].endswith("rxn.inp"))
        self.assertTrue(event["next_inp"].endswith("rxn.retry01.inp"))

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

            with patch("core.commands.run_inp.OrcaRunner.run", new=_fake_run):
                rc = self._run_internal_execute(config, reaction)
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
            fake_orca = root / "fake_orca"
            fake_orca.touch()
            fake_orca.chmod(0o755)
            config = root / "orca_auto.yaml"
            config.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": str(root / "orca_runs"),
                            "default_max_retries": 6,
                        },
                        "paths": {"orca_executable": str(fake_orca)},
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

            with patch("core.commands.run_inp.OrcaRunner.run", new=_fake_run):
                rc = self._run_internal_execute(config, reaction)
            state = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))

        self.assertEqual(rc, 1)
        self.assertEqual(calls["n"], 7)
        self.assertEqual(len(state["attempts"]), 7)
        self.assertEqual(state["final_result"]["reason"], "retry_limit_reached")
        self.assertEqual(state["final_result"]["analyzer_status"], "error_disk_io")

    def test_retry_limit_already_reached_finalizes_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn4"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            fake_orca = root / "fake_orca"
            fake_orca.touch()
            fake_orca.chmod(0o755)
            config = root / "orca_auto.yaml"
            config.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": str(root / "orca_runs"),
                            "default_max_retries": 0,
                        },
                        "paths": {"orca_executable": str(fake_orca)},
                    }
                ),
                encoding="utf-8",
            )
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

            with patch("core.commands.run_inp.OrcaRunner.run") as run_mock:
                rc = self._run_internal_execute(config, reaction)
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

            with patch("core.commands.run_inp.OrcaRunner.run", new=_fake_run):
                rc = self._run_internal_execute(config, reaction)
            saved = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))
            retry_exists = (reaction / "rxn.retry01.inp").exists()

        self.assertEqual(rc, 0)
        self.assertEqual(seen["inp_name"], "rxn.retry01.inp")
        self.assertTrue(retry_exists)
        self.assertEqual(saved["status"], "completed")
        self.assertEqual(len(saved["attempts"]), 2)
        actions = saved["attempts"][0].get("patch_actions", [])
        self.assertTrue(any("resume_recreated_missing_input:rxn.retry01.inp" in action for action in actions))

    def test_resume_interrupted_failure_keeps_run_id_and_continues(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reaction = root / "orca_runs" / "rxn_resume_interrupt"
            reaction.mkdir(parents=True)
            inp = reaction / "rxn.inp"
            inp.write_text("! Opt\n* xyz 0 1\nH 0 0 0\nH 0 0 0.74\n*\n", encoding="utf-8")
            config = self._write_config(root, root / "orca_runs")
            state = {
                "run_id": "run_resume_interrupted",
                "reaction_dir": str(reaction),
                "selected_inp": str(inp),
                "max_retries": 5,
                "status": "failed",
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
                "final_result": {
                    "status": "failed",
                    "analyzer_status": "incomplete",
                    "reason": "interrupted_by_user",
                    "completed_at": "2026-01-01T00:00:02+00:00",
                    "last_out_path": str(reaction / "rxn.out"),
                },
            }
            (reaction / "run_state.json").write_text(json.dumps(state), encoding="utf-8")
            seen = {"inp_name": ""}

            def _fake_run(_self, inp_path: Path) -> RunResult:
                seen["inp_name"] = inp_path.name
                out = inp_path.with_suffix(".out")
                out.write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")
                return RunResult(out_path=str(out), return_code=0)

            with patch("core.commands.run_inp.OrcaRunner.run", new=_fake_run):
                rc = self._run_internal_execute(config, reaction)
            saved = json.loads((reaction / "run_state.json").read_text(encoding="utf-8"))
            retry_exists = (reaction / "rxn.retry01.inp").exists()

        self.assertEqual(rc, 0)
        self.assertEqual(saved["run_id"], "run_resume_interrupted")
        self.assertEqual(seen["inp_name"], "rxn.retry01.inp")
        self.assertTrue(retry_exists)
        self.assertEqual(saved["status"], "completed")
        self.assertEqual(len(saved["attempts"]), 2)
        self.assertTrue(saved["final_result"]["resumed"])

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

            with patch("core.commands.run_inp.OrcaRunner.run") as run_mock:
                rc = self._run_internal_execute(config, reaction)
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

            with patch("core.commands.run_inp.OrcaRunner.run", new=_fake_run):
                rc = self._run_internal_execute(config, reaction)
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

            with patch("core.commands.run_inp.OrcaRunner.run", new=_fake_run):
                rc = self._run_internal_execute(config, reaction)
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
            _emit(payload)
        output = captured.getvalue()
        self.assertIn("status: completed", output)
        self.assertIn("attempt_count: 1", output)
        self.assertNotIn("extra_unknown_key", output)

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
                rc = main(["--config", str(config), "run-dir", str(outside)])
        self.assertEqual(rc, 1)
        # Error should go to stderr (via logger.error), not stdout
        self.assertNotIn("allowed root", captured_stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
