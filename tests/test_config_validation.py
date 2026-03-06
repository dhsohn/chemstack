import json
import tempfile
import unittest
from pathlib import Path

from core.config import (
    CleanupConfig,
    _normalize_extensions,
    _normalize_string_list,
    _validate_cleanup_config,
    load_config,
    _DEFAULT_KEEP_EXTENSIONS,
    _DEFAULT_KEEP_FILENAMES,
    _DEFAULT_REMOVE_PATTERNS,
)


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
            # deprecated key is now ignored; default_max_retries uses its default value
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


class TestNormalizeExtensions(unittest.TestCase):

    def test_default_on_non_list(self) -> None:
        self.assertEqual(_normalize_extensions("not a list"), list(_DEFAULT_KEEP_EXTENSIONS))
        self.assertEqual(_normalize_extensions(42), list(_DEFAULT_KEEP_EXTENSIONS))
        self.assertEqual(_normalize_extensions(None), list(_DEFAULT_KEEP_EXTENSIONS))

    def test_lowercases(self) -> None:
        self.assertEqual(_normalize_extensions([".INP", ".Out"]), [".inp", ".out"])

    def test_adds_dot_prefix(self) -> None:
        self.assertEqual(_normalize_extensions(["inp", "out"]), [".inp", ".out"])

    def test_deduplicates(self) -> None:
        self.assertEqual(_normalize_extensions([".inp", ".INP", "inp"]), [".inp"])

    def test_strips_whitespace(self) -> None:
        self.assertEqual(_normalize_extensions(["  .inp  ", " .out"]), [".inp", ".out"])

    def test_filters_empty_and_non_string(self) -> None:
        self.assertEqual(_normalize_extensions(["", " ", 123, ".inp"]), [".inp"])


class TestNormalizeStringList(unittest.TestCase):

    def test_default_on_non_list(self) -> None:
        defaults = ["a.json", "b.json"]
        self.assertEqual(_normalize_string_list("not a list", defaults), defaults)

    def test_deduplicates(self) -> None:
        self.assertEqual(
            _normalize_string_list(["a.json", "a.json", "b.json"], []),
            ["a.json", "b.json"],
        )

    def test_strips_whitespace(self) -> None:
        self.assertEqual(
            _normalize_string_list(["  a.json  ", " b.json"], []),
            ["a.json", "b.json"],
        )


class TestValidateCleanupConfig(unittest.TestCase):

    def test_empty_keep_extensions_raises(self) -> None:
        cfg = CleanupConfig(keep_extensions=[], keep_filenames=["run_state.json"])
        with self.assertRaises(ValueError) as ctx:
            _validate_cleanup_config(cfg)
        self.assertIn("keep_extensions must not be empty", str(ctx.exception))

    def test_empty_keep_filenames_raises(self) -> None:
        cfg = CleanupConfig(keep_extensions=[".inp"], keep_filenames=[])
        with self.assertRaises(ValueError) as ctx:
            _validate_cleanup_config(cfg)
        self.assertIn("keep_filenames must not be empty", str(ctx.exception))

    def test_empty_remove_patterns_is_ok(self) -> None:
        cfg = CleanupConfig(
            keep_extensions=[".inp"],
            keep_filenames=["run_state.json"],
            remove_patterns=[],
        )
        _validate_cleanup_config(cfg)

    def test_valid_config_passes(self) -> None:
        cfg = CleanupConfig()
        _validate_cleanup_config(cfg)


class TestCleanupConfigLoading(unittest.TestCase):

    def test_defaults_when_no_cleanup_section(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "orca_auto.yaml"
            cfg_path.write_text("{}", encoding="utf-8")
            cfg = load_config(str(cfg_path))
            self.assertEqual(cfg.cleanup.keep_extensions, list(_DEFAULT_KEEP_EXTENSIONS))
            self.assertEqual(cfg.cleanup.keep_filenames, list(_DEFAULT_KEEP_FILENAMES))
            self.assertEqual(cfg.cleanup.remove_patterns, list(_DEFAULT_REMOVE_PATTERNS))
            self.assertFalse(cfg.cleanup.remove_overrides_keep)

    def test_custom_keep_extensions(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "orca_auto.yaml"
            cfg_path.write_text(
                json.dumps({"cleanup": {"keep_extensions": ["inp", "OUT"]}}),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))
            self.assertEqual(cfg.cleanup.keep_extensions, [".inp", ".out"])

    def test_invalid_type_falls_back_to_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "orca_auto.yaml"
            cfg_path.write_text(
                json.dumps({"cleanup": {"keep_extensions": "not_a_list"}}),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))
            self.assertEqual(cfg.cleanup.keep_extensions, list(_DEFAULT_KEEP_EXTENSIONS))

    def test_empty_keep_extensions_raises_via_load(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "orca_auto.yaml"
            cfg_path.write_text(
                json.dumps({"cleanup": {"keep_extensions": []}}),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("keep_extensions must not be empty", str(ctx.exception))

    def test_empty_keep_filenames_raises_via_load(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "orca_auto.yaml"
            cfg_path.write_text(
                json.dumps({"cleanup": {"keep_filenames": []}}),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                load_config(str(cfg_path))
            self.assertIn("keep_filenames must not be empty", str(ctx.exception))

    def test_empty_remove_patterns_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "orca_auto.yaml"
            cfg_path.write_text(
                json.dumps({"cleanup": {"remove_patterns": []}}),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))
            self.assertEqual(cfg.cleanup.remove_patterns, [])

    def test_remove_overrides_keep_true(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "orca_auto.yaml"
            cfg_path.write_text(
                json.dumps({"cleanup": {"remove_overrides_keep": True}}),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))
            self.assertTrue(cfg.cleanup.remove_overrides_keep)

    def test_remove_overrides_keep_invalid_type_falls_back_to_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "orca_auto.yaml"
            cfg_path.write_text(
                json.dumps({"cleanup": {"remove_overrides_keep": "true"}}),
                encoding="utf-8",
            )
            cfg = load_config(str(cfg_path))
            self.assertFalse(cfg.cleanup.remove_overrides_keep)


if __name__ == "__main__":
    unittest.main()
