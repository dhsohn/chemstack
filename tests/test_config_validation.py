import json
import tempfile
import unittest
from pathlib import Path

from core.config import load_config


class TestConfigValidation(unittest.TestCase):
    def test_platform_mode_key_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "orca_auto.yaml"
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
            cfg_path = Path(td) / "orca_auto.yaml"
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
            cfg_path = Path(td) / "orca_auto.yaml"
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
            cfg_path = Path(td) / "orca_auto.yaml"
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
            cfg_path = Path(td) / "orca_auto.yaml"
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
            cfg_path = Path(td) / "orca_auto.yaml"
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
            cfg_path = Path(td) / "orca_auto.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": "/home/user/orca_runs",
                        },
                        "paths": {"orca_executable": "/opt/orca/orca"},
                    }
                ),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))
            self.assertEqual(cfg.runtime.allowed_root, "/home/user/orca_runs")
            self.assertEqual(cfg.paths.orca_executable, "/opt/orca/orca")

    def test_deprecated_max_attempts_key_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "orca_auto.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": "/home/user/orca_runs",
                            "default_max_attempts": 3,
                        },
                        "paths": {"orca_executable": "/opt/orca/orca"},
                    }
                ),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))
            self.assertEqual(cfg.runtime.default_max_retries, 2)

    def test_default_max_retries_can_exceed_five(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "orca_auto.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": "/home/user/orca_runs",
                            "default_max_retries": 9,
                        },
                        "paths": {"orca_executable": "/opt/orca/orca"},
                    }
                ),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))
            self.assertEqual(cfg.runtime.default_max_retries, 9)

    def test_defaults_are_applied(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "orca_auto.yaml"
            cfg_path.write_text("{}", encoding="utf-8")
            cfg = load_config(str(cfg_path))
            home = str(Path.home())
            self.assertEqual(cfg.runtime.allowed_root, f"{home}/orca_runs")
            self.assertEqual(cfg.runtime.organized_root, f"{home}/orca_outputs")
            self.assertEqual(cfg.runtime.default_max_retries, 2)
            self.assertEqual(cfg.paths.orca_executable, f"{home}/opt/orca/orca")

    def test_windows_organized_root_raises(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "orca_auto.yaml"
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
            cfg_path = Path(td) / "orca_auto.yaml"
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
            allowed = Path(td) / "runs"
            organized = allowed / "outputs"
            allowed.mkdir()
            organized.mkdir()
            cfg_path = Path(td) / "orca_auto.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": str(allowed),
                            "organized_root": str(organized),
                        },
                        "paths": {"orca_executable": "/opt/orca/orca"},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("must not contain each other", str(ctx.exception))

    def test_organized_root_set_correctly(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "orca_auto.yaml"
            cfg_path.write_text(
                json.dumps(
                    {
                        "runtime": {
                            "allowed_root": "/home/user/orca_runs",
                            "organized_root": "/home/user/orca_outputs",
                        },
                        "paths": {"orca_executable": "/opt/orca/orca"},
                    }
                ),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))
            self.assertEqual(cfg.runtime.organized_root, "/home/user/orca_outputs")


if __name__ == "__main__":
    unittest.main()
