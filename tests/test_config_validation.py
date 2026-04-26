import json
import tempfile
import unittest
from pathlib import Path

import yaml

from chemstack.orca.config import load_config


class TestConfigValidation(unittest.TestCase):
    def test_platform_mode_key_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "platform_mode": "linux_native",
                            "allowed_root": "/tmp/runs",
                        },
                        "paths": {"orca_executable": "/opt/orca/orca"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("runtime.platform_mode is removed", str(ctx.exception))

    def test_windows_allowed_root_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": "C:\\orca_runs",
                        },
                        "paths": {"orca_executable": "/opt/orca/orca"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("Linux path", str(ctx.exception))

    def test_windows_mount_allowed_root_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": "/mnt/c/orca_runs",
                        },
                        "paths": {"orca_executable": "/home/user/opt/orca/orca"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("Linux path", str(ctx.exception))

    def test_relative_paths_raise(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": "./orca_runs",
                        },
                        "paths": {"orca_executable": "./opt/orca/orca"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("absolute Linux path", str(ctx.exception))

    def test_windows_orca_executable_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": "/home/user/orca_runs",
                        },
                        "paths": {"orca_executable": "C:\\Orca\\orca.exe"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("Linux path", str(ctx.exception))

    def test_exe_suffix_orca_executable_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": "/home/user/orca_runs",
                        },
                        "paths": {"orca_executable": "/home/user/opt/orca/orca.exe"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("Linux ORCA binary", str(ctx.exception))

    def test_linux_paths_succeed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            fake_orca = root / "orca"
            fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")

            cfg_path = root / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": str(allowed),
                        },
                        "paths": {"orca_executable": str(fake_orca)},
                    }
                ),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))
            self.assertEqual(cfg.runtime.allowed_root, str(allowed))
            self.assertEqual(cfg.paths.orca_executable, str(fake_orca))

    def test_telegram_delivery_settings_are_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            fake_orca = root / "orca"
            fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")

            cfg_path = root / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": str(allowed),
                        },
                        "paths": {"orca_executable": str(fake_orca)},
                        "telegram": {
                            "bot_token": "token",
                            "chat_id": "chat",
                            "timeout_seconds": 3.5,
                            "max_attempts": 4,
                            "retry_backoff_seconds": 0.25,
                        },
                    }
                ),
                encoding="utf-8",
            )

            cfg = load_config(str(cfg_path))

            self.assertEqual(cfg.telegram.bot_token, "token")
            self.assertEqual(cfg.telegram.chat_id, "chat")
            self.assertEqual(cfg.telegram.timeout_seconds, 3.5)
            self.assertEqual(cfg.telegram.max_attempts, 4)
            self.assertEqual(cfg.telegram.retry_backoff_seconds, 0.25)

    def test_workflow_root_is_preserved_with_engine_scoped_orca_config(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            workflow_root = root / "workflow_runs"
            workflow_root.mkdir()
            allowed = root / "orca_runs"
            allowed.mkdir()
            fake_orca = root / "orca"
            fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")

            cfg_path = root / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "workflow": {
                            "root": str(workflow_root),
                        },
                        "orca": {
                            "runtime": {
                                "allowed_root": str(allowed),
                            },
                            "paths": {"orca_executable": str(fake_orca)},
                        },
                    }
                ),
                encoding="utf-8",
            )

            cfg = load_config(str(cfg_path))

            self.assertEqual(cfg.workflow_root, str(workflow_root.resolve()))
            self.assertEqual(cfg.runtime.allowed_root, str(allowed.resolve()))
            self.assertEqual(cfg.paths.orca_executable, str(fake_orca.resolve()))

    def test_deprecated_max_attempts_key_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            fake_orca = root / "orca"
            fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")

            cfg_path = root / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": str(allowed),
                            "default_max_attempts": 3,
                        },
                        "paths": {"orca_executable": str(fake_orca)},
                    }
                ),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))
            self.assertEqual(cfg.runtime.default_max_retries, 2)

    def test_default_max_retries_can_exceed_five(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            fake_orca = root / "orca"
            fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")

            cfg_path = root / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": str(allowed),
                            "default_max_retries": 9,
                        },
                        "paths": {"orca_executable": str(fake_orca)},
                    }
                ),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))
            self.assertEqual(cfg.runtime.default_max_retries, 9)
            self.assertEqual(cfg.runtime.max_concurrent, 4)

    def test_removed_runtime_scheduler_keys_are_rejected(self) -> None:
        for key, value in (
            ("max_concurrent", 7),
            ("admission_limit", 3),
            ("admission_max_concurrent", 5),
            ("admission_root", "/tmp/admission"),
        ):
            with self.subTest(key=key):
                with tempfile.TemporaryDirectory() as td:
                    root = Path(td)
                    allowed = root / "orca_runs"
                    allowed.mkdir()
                    fake_orca = root / "orca"
                    fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")

                    cfg_path = root / "chemstack.yaml"
                    cfg_path.write_text(
                        json.dumps(
                            {
                                "runtime": {
                                    "allowed_root": str(allowed),
                                    key: value,
                                },
                                "paths": {"orca_executable": str(fake_orca)},
                            }
                        ),
                        encoding="utf-8",
                    )
                    with self.assertRaises(ValueError) as ctx:
                        load_config(str(cfg_path))
                    self.assertIn("unsupported runtime keys", str(ctx.exception))
                    self.assertIn(f"runtime.{key}", str(ctx.exception))

    def test_resources_section_and_common_runtime_conversion_are_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            fake_orca = root / "orca"
            fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")

            cfg_path = root / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "scheduler": {
                            "max_active_simulations": 6,
                        },
                        "runtime": {
                            "allowed_root": str(allowed),
                        },
                        "paths": {"orca_executable": str(fake_orca)},
                        "resources": {
                            "max_cores_per_task": 12,
                            "max_memory_gb_per_task": 48,
                        },
                    }
                ),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))

            self.assertEqual(cfg.resources.max_cores_per_task, 12)
            self.assertEqual(cfg.resources.max_memory_gb_per_task, 48)

            common_runtime = cfg.runtime.to_common_runtime_config()
            self.assertEqual(common_runtime.allowed_root, str(allowed))
            self.assertEqual(common_runtime.max_concurrent, 6)
            self.assertEqual(common_runtime.resolved_admission_limit, 6)
            self.assertEqual(common_runtime.resolved_admission_root, str(root / "admission"))

    def test_behavior_auto_organize_is_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            fake_orca = root / "orca"
            fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")

            cfg_path = root / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": str(allowed),
                        },
                        "paths": {"orca_executable": str(fake_orca)},
                        "behavior": {
                            "auto_organize_on_terminal": True,
                        },
                    }
                ),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))
            self.assertTrue(cfg.behavior.auto_organize_on_terminal)

    def test_config_example_sets_auto_organize_off_by_default(self) -> None:
        example_path = Path(__file__).resolve().parents[1] / "config" / "chemstack.yaml.example"
        payload = yaml.safe_load(example_path.read_text(encoding="utf-8"))

        self.assertFalse(payload["behavior"]["auto_organize_on_terminal"])

    def test_missing_config_file_raises_with_setup_hint(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "chemstack.yaml"
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("Config file not found", str(ctx.exception))
            self.assertIn("chemstack.yaml.example", str(ctx.exception))

    def test_missing_required_paths_raise_with_explicit_path_hint(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "chemstack.yaml"
            cfg_path.write_text("{}", encoding="utf-8")
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("runtime.allowed_root", str(ctx.exception))
            self.assertIn("paths.orca_executable", str(ctx.exception))
            self.assertIn("no longer assumes personal defaults", str(ctx.exception))

    def test_organized_root_defaults_next_to_allowed_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            fake_orca = root / "orca"
            fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")

            cfg_path = root / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": str(allowed),
                        },
                        "paths": {"orca_executable": str(fake_orca)},
                    }
                ),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))
            self.assertEqual(cfg.runtime.organized_root, str(root / "orca_outputs"))
            self.assertEqual(cfg.runtime.default_max_retries, 2)
            self.assertEqual(cfg.runtime.max_concurrent, 4)

    def test_template_placeholder_paths_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": "/path/to/orca_runs",
                            "organized_root": "/path/to/orca_outputs",
                        },
                        "paths": {"orca_executable": "/path/to/orca/orca"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("template placeholder paths", str(ctx.exception))

    def test_windows_organized_root_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": "/home/user/orca_runs",
                            "organized_root": "C:\\orca_outputs",
                        },
                        "paths": {"orca_executable": "/opt/orca/orca"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("Linux path", str(ctx.exception))

    def test_relative_organized_root_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": "/home/user/orca_runs",
                            "organized_root": "./outputs",
                        },
                        "paths": {"orca_executable": "/opt/orca/orca"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("absolute Linux path", str(ctx.exception))

    def test_organized_root_inside_allowed_root_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "runs"
            organized = allowed / "outputs"
            allowed.mkdir()
            organized.mkdir()
            fake_orca = root / "orca"
            fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")
            cfg_path = root / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": str(allowed),
                            "organized_root": str(organized),
                        },
                        "paths": {"orca_executable": str(fake_orca)},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("must not contain each other", str(ctx.exception))

    def test_organized_root_set_correctly(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            organized = root / "orca_outputs"
            organized.mkdir()
            fake_orca = root / "orca"
            fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")

            cfg_path = root / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": str(allowed),
                            "organized_root": str(organized),
                        },
                        "paths": {"orca_executable": str(fake_orca)},
                    }
                ),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))
            self.assertEqual(cfg.runtime.organized_root, str(organized))


    def test_nonexistent_orca_executable_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            allowed.mkdir()
            cfg_path = root / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {"allowed_root": str(allowed)},
                        "paths": {"orca_executable": str(root / "nonexistent_orca")},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("orca_executable not found", str(ctx.exception))

    def test_nonexistent_allowed_root_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            fake_orca = root / "orca"
            fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")
            cfg_path = root / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {"allowed_root": str(root / "nonexistent_dir")},
                        "paths": {"orca_executable": str(fake_orca)},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("allowed_root directory not found", str(ctx.exception))

    def test_allowed_root_is_file_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            not_a_dir = root / "orca_runs"
            not_a_dir.write_text("oops", encoding="utf-8")
            fake_orca = root / "orca"
            fake_orca.write_text("#!/bin/sh\n", encoding="utf-8")
            cfg_path = root / "chemstack.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {"allowed_root": str(not_a_dir)},
                        "paths": {"orca_executable": str(fake_orca)},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("is not a directory", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
