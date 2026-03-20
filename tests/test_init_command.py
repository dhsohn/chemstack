from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import yaml

from core.cli import main
from core.config import load_config


class TestInitCommand(unittest.TestCase):
    def test_init_creates_config_from_interactive_answers(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config_path = root / "config" / "orca_auto.yaml"
            fake_orca = root / "bin" / "orca"
            fake_orca.parent.mkdir(parents=True)
            fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")
            fake_orca.chmod(0o755)

            allowed_root = root / "orca_runs"
            organized_root = root / "orca_outputs"
            stdout = io.StringIO()

            answers = [
                str(fake_orca),
                str(allowed_root),
                "y",
                "",
                "y",
                "",
                "",
                "n",
            ]

            with (
                patch("core.commands.init.default_config_path", return_value=str(config_path)),
                patch("builtins.input", side_effect=answers),
                redirect_stdout(stdout),
            ):
                rc = main(["init"])

            rendered = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            cfg = load_config(str(config_path))
            self.assertEqual(rc, 0)
            self.assertEqual(rendered["runtime"]["allowed_root"], str(allowed_root))
            self.assertEqual(rendered["runtime"]["organized_root"], str(organized_root))
            self.assertEqual(rendered["runtime"]["default_max_retries"], 2)
            self.assertEqual(rendered["runtime"]["max_concurrent"], 4)
            self.assertEqual(rendered["paths"]["orca_executable"], str(fake_orca))
            self.assertEqual(rendered["telegram"]["bot_token"], "")
            self.assertEqual(rendered["telegram"]["chat_id"], "")
            self.assertTrue(allowed_root.exists())
            self.assertTrue(organized_root.exists())
            self.assertEqual(cfg.runtime.allowed_root, str(allowed_root))
            self.assertEqual(cfg.runtime.organized_root, str(organized_root))
            self.assertEqual(cfg.runtime.max_concurrent, 4)
            self.assertIn("Config created successfully.", stdout.getvalue())

    def test_init_does_not_overwrite_existing_config_when_declined(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config_path = root / "orca_auto.yaml"
            config_path.write_text("runtime:\n  allowed_root: /tmp/existing\n", encoding="utf-8")
            stdout = io.StringIO()

            with (
                patch("core.commands.init.default_config_path", return_value=str(config_path)),
                patch("builtins.input", side_effect=["n"]),
                redirect_stdout(stdout),
            ):
                rc = main(["init"])

            contents = config_path.read_text(encoding="utf-8")

        self.assertEqual(rc, 0)
        self.assertEqual(contents, "runtime:\n  allowed_root: /tmp/existing\n")
        self.assertIn("Cancelled.", stdout.getvalue())
