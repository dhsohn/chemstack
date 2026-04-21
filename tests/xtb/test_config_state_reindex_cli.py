from __future__ import annotations

import argparse
import json
import runpy
import warnings
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from chemstack.xtb import cli
from chemstack.xtb import state as state_mod
from chemstack.xtb.commands import reindex as reindex_cmd
from chemstack.xtb.config import CONFIG_ENV_VAR, _as_bool, _as_int, _as_str, default_config_path, load_config


def test_default_config_path_prefers_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(CONFIG_ENV_VAR, "/tmp/custom-chemstack.yaml")
    assert default_config_path() == "/tmp/custom-chemstack.yaml"

    monkeypatch.delenv(CONFIG_ENV_VAR, raising=False)
    assert default_config_path().endswith("/config/chemstack.yaml")


def test_load_config_parses_defaults_and_normalizes_values(tmp_path: Path) -> None:
    allowed_root = tmp_path / "allowed"
    allowed_root.mkdir()
    config_path = tmp_path / "chemstack.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "scheduler": {
                    "max_active_simulations": "6",
                },
                "xtb": {
                    "runtime": {
                        "allowed_root": str(allowed_root),
                    },
                    "paths": {
                        "xtb_executable": " /opt/xtb ",
                    },
                },
                "behavior": {
                    "auto_organize_on_terminal": "yes",
                },
                "resources": {
                    "max_cores_per_task": "0",
                    "max_memory_gb_per_task": "-5",
                },
                "telegram": {
                    "bot_token": " token ",
                    "chat_id": " chat ",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    cfg = load_config(str(config_path))

    assert cfg.runtime.allowed_root == str(allowed_root)
    assert cfg.runtime.organized_root == str(allowed_root.parent / "xtb_outputs")
    assert cfg.runtime.max_concurrent == 6
    assert cfg.runtime.admission_root == str(tmp_path / "admission")
    assert cfg.runtime.admission_limit == 6
    assert cfg.paths.xtb_executable == "/opt/xtb"
    assert cfg.behavior.auto_organize_on_terminal is True
    assert cfg.resources.max_cores_per_task == 1
    assert cfg.resources.max_memory_gb_per_task == 1
    assert cfg.telegram.bot_token == "token"
    assert cfg.telegram.chat_id == "chat"


def test_load_config_reports_missing_file_invalid_payload_and_missing_allowed_root(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing.yaml"
    with pytest.raises(ValueError, match="Config file not found"):
        load_config(str(missing_path))

    invalid_path = tmp_path / "invalid.yaml"
    invalid_path.write_text("- not-a-mapping\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Config file is invalid"):
        load_config(str(invalid_path))

    missing_allowed_root_path = tmp_path / "missing-allowed.yaml"
    missing_allowed_root_path.write_text(yaml.safe_dump({"xtb": {"runtime": {}}}), encoding="utf-8")
    with pytest.raises(ValueError, match="Config is missing runtime.allowed_root"):
        load_config(str(missing_allowed_root_path))


@pytest.mark.parametrize(
    ("value", "default", "expected"),
    [
        (None, False, False),
        (None, True, True),
        (True, False, True),
        (False, True, False),
        ("YES", False, True),
        ("off", True, False),
        ("maybe", True, True),
        ("maybe", False, False),
    ],
)
def test_helper_normalizers_cover_boolean_and_default_branches(
    value: object,
    default: bool,
    expected: bool,
) -> None:
    assert _as_bool(value, default) == expected


def test_helper_normalizers_cover_string_and_int_defaults() -> None:
    assert _as_str(None, "fallback") == "fallback"
    assert _as_str("  value  ", "fallback") == "value"
    assert _as_int(None, 7) == 7
    assert _as_int("9", 7) == 9
    assert _as_int("not-a-number", 7) == 7


def test_load_config_applies_defaults_for_missing_and_non_mapping_optional_sections(
    tmp_path: Path,
) -> None:
    allowed_root = tmp_path / "allowed"
    allowed_root.mkdir()
    config_path = tmp_path / "chemstack.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "xtb": {
                    "runtime": {
                        "allowed_root": str(allowed_root),
                    },
                    "paths": [],
                },
                "behavior": [],
                "resources": [],
                "telegram": [],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    cfg = load_config(str(config_path))

    assert cfg.runtime.allowed_root == str(allowed_root)
    assert cfg.runtime.organized_root == str(allowed_root.parent / "xtb_outputs")
    assert cfg.runtime.max_concurrent == 4
    assert cfg.runtime.admission_root == str(allowed_root)
    assert cfg.runtime.admission_limit == 4
    assert cfg.paths.xtb_executable == ""
    assert cfg.behavior.auto_organize_on_terminal is False
    assert cfg.resources.max_cores_per_task == 8
    assert cfg.resources.max_memory_gb_per_task == 32
    assert cfg.telegram.bot_token == ""
    assert cfg.telegram.chat_id == ""


def test_state_helpers_write_and_load_round_trip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_dir = tmp_path / "job-001"
    job_dir.mkdir()
    monkeypatch.setattr(state_mod, "now_utc_iso", lambda: "2026-04-20T00:00:00Z")

    state_path = state_mod.write_state(job_dir, {"status": "queued"})
    report_json_path = state_mod.write_report_json(job_dir, {"status": "completed"})
    report_md_path = state_mod.write_report_md(
        job_dir,
        job_id="job-001",
        status="completed",
        reason="xtb_ok",
        selected_input="input.xyz",
    )
    report_lines_path = state_mod.write_report_md_lines(job_dir, ["# heading", "", "- done"])
    organized_ref_path = state_mod.write_organized_ref(job_dir, {"organized_output_dir": "/tmp/out"})

    assert state_path == job_dir / state_mod.STATE_FILE_NAME
    assert report_json_path == job_dir / state_mod.REPORT_JSON_FILE_NAME
    assert report_md_path == job_dir / state_mod.REPORT_MD_FILE_NAME
    assert report_lines_path == job_dir / state_mod.REPORT_MD_FILE_NAME
    assert organized_ref_path == job_dir / state_mod.ORGANIZED_REF_FILE_NAME
    assert state_mod.load_state(job_dir) == {"status": "queued"}
    assert state_mod.load_report_json(job_dir) == {"status": "completed"}
    assert state_mod.load_organized_ref(job_dir) == {"organized_output_dir": "/tmp/out"}
    assert (job_dir / state_mod.REPORT_MD_FILE_NAME).read_text(encoding="utf-8") == "# heading\n\n- done\n"


def test_state_loaders_return_none_for_missing_invalid_and_non_mapping_payloads(tmp_path: Path) -> None:
    job_dir = tmp_path / "job-002"
    job_dir.mkdir()

    assert state_mod.load_state(job_dir) is None
    assert state_mod.load_report_json(job_dir) is None
    assert state_mod.load_organized_ref(job_dir) is None

    for filename, loader in (
        (state_mod.STATE_FILE_NAME, state_mod.load_state),
        (state_mod.REPORT_JSON_FILE_NAME, state_mod.load_report_json),
        (state_mod.ORGANIZED_REF_FILE_NAME, state_mod.load_organized_ref),
    ):
        path = job_dir / filename
        path.write_text("{invalid-json", encoding="utf-8")
        assert loader(job_dir) is None
        path.write_text(json.dumps(["not", "a", "mapping"]), encoding="utf-8")
        assert loader(job_dir) is None


def test_reindex_scan_roots_prefers_explicit_root_and_skips_invalid_default_roots(tmp_path: Path) -> None:
    explicit_root = tmp_path / "explicit"
    organized_root = tmp_path / "organized"
    explicit_root.mkdir()
    organized_root.mkdir()
    cfg = SimpleNamespace(runtime=SimpleNamespace(allowed_root="", organized_root=str(organized_root)))

    assert reindex_cmd._scan_roots(cfg, str(explicit_root)) == [explicit_root.resolve()]
    assert reindex_cmd._scan_roots(cfg, None) == [organized_root.resolve()]


def test_cmd_reindex_reports_error_when_no_roots_are_available(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = SimpleNamespace(runtime=SimpleNamespace(allowed_root="", organized_root=""))
    monkeypatch.setattr(reindex_cmd, "load_config", lambda path=None: cfg)

    exit_code = reindex_cmd.cmd_reindex(SimpleNamespace(config=None, root=None))

    assert exit_code == 1
    assert capsys.readouterr().out == "error: no reindex roots available\n"


def test_cmd_reindex_counts_skipped_candidates_when_record_cannot_be_built(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "allowed"
    root.mkdir()
    job_dir = root / "job-skip"
    job_dir.mkdir()
    cfg = SimpleNamespace(runtime=SimpleNamespace(allowed_root=str(root), organized_root=str(root)))

    monkeypatch.setattr(reindex_cmd, "load_config", lambda path=None: cfg)
    monkeypatch.setattr(reindex_cmd, "_scan_roots", lambda cfg_obj, raw_root: [root.resolve()])
    monkeypatch.setattr(reindex_cmd, "_iter_candidate_dirs", lambda root_path: {job_dir.resolve()})
    monkeypatch.setattr(reindex_cmd, "index_root_for_cfg", lambda cfg_obj: root.resolve())
    monkeypatch.setattr(reindex_cmd, "load_state", lambda path: {})
    monkeypatch.setattr(reindex_cmd, "load_report_json", lambda path: {})
    monkeypatch.setattr(reindex_cmd, "load_organized_ref", lambda path: {})
    monkeypatch.setattr(reindex_cmd, "record_from_artifacts", lambda **kwargs: None)

    exit_code = reindex_cmd.cmd_reindex(SimpleNamespace(config=None, root=None))

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "candidate_dirs: 1" in output
    assert "indexed: 0" in output
    assert "skipped: 1" in output


def test_cli_cmd_queue_dispatches_worker_cancel_and_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_args = argparse.Namespace(queue_command="worker")
    cancel_args = argparse.Namespace(queue_command="cancel")
    other_args = argparse.Namespace(queue_command="unknown")

    monkeypatch.setattr(cli, "cmd_queue_worker", lambda args: 11)
    monkeypatch.setattr(cli, "cmd_queue_cancel", lambda args: 17)

    assert cli._cmd_queue(worker_args) == 11
    assert cli._cmd_queue(cancel_args) == 17

    with pytest.raises(ValueError, match="Unsupported queue subcommand: unknown"):
        cli._cmd_queue(other_args)


def test_cli_module_main_entrypoint_raises_system_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "chemstack.yaml"
    allowed_root = tmp_path / "allowed"
    organized_root = tmp_path / "organized"
    allowed_root.mkdir()
    organized_root.mkdir()
    config_path.write_text(
        yaml.safe_dump(
            {
                "runtime": {
                    "allowed_root": str(allowed_root),
                    "organized_root": str(organized_root),
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "sys.argv",
        ["xtb_auto", "--config", str(config_path), "list"],
    )

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r"'xtb_auto\.cli' found in sys\.modules .* prior to execution of 'xtb_auto\.cli'",
            category=RuntimeWarning,
        )
        with pytest.raises(SystemExit) as exc_info:
            runpy.run_module("chemstack.xtb.cli", run_name="__main__")

    assert exc_info.value.code == 0
    assert capsys.readouterr().out == "No xTB jobs found.\n"
