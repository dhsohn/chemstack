from __future__ import annotations

import json
import textwrap
from pathlib import Path
from typing import Any, Callable

import pytest

from chemstack.crest import config as config_mod
from chemstack.crest import state as state_mod

JsonWriter = Callable[[Path, dict[str, Any]], Path]
JsonLoader = Callable[[Path], dict[str, Any] | None]


def _write_config(path: Path, contents: str) -> Path:
    path.write_text(textwrap.dedent(contents).strip() + "\n", encoding="utf-8")
    return path


def test_default_config_path_prefers_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(config_mod.CONFIG_ENV_VAR, "  ~/custom-config.yaml  ")

    assert config_mod.default_config_path() == "~/custom-config.yaml"


@pytest.mark.parametrize("env_value", [None, "   "], ids=["unset", "blank"])
def test_default_config_path_falls_back_to_repo_config(
    monkeypatch: pytest.MonkeyPatch,
    env_value: str | None,
) -> None:
    if env_value is None:
        monkeypatch.delenv(config_mod.CONFIG_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(config_mod.CONFIG_ENV_VAR, env_value)

    expected = str(Path(config_mod.__file__).resolve().parents[3] / "config" / "chemstack.yaml")

    assert config_mod.default_config_path() == expected


@pytest.mark.parametrize(
    ("value", "default", "expected"),
    [
        (None, "fallback", "fallback"),
        ("  crest  ", "", "crest"),
        (123, "", "123"),
    ],
)
def test_as_str_normalizes_values(value: object, default: str, expected: str) -> None:
    assert config_mod._as_str(value, default) == expected


@pytest.mark.parametrize(
    ("value", "default", "expected"),
    [
        ("7", 3, 7),
        (5.9, 0, 5),
        ("not-an-int", 9, 9),
        (None, 4, 4),
    ],
)
def test_as_int_returns_default_for_invalid_values(value: object, default: int, expected: int) -> None:
    assert config_mod._as_int(value, default) == expected


@pytest.mark.parametrize(
    ("value", "default", "expected"),
    [
        (None, True, True),
        (True, False, True),
        (" yes ", False, True),
        ("OFF", True, False),
        ("maybe", True, True),
    ],
)
def test_as_bool_normalizes_truthy_and_falsy_strings(
    value: object,
    default: bool,
    expected: bool,
) -> None:
    assert config_mod._as_bool(value, default) is expected


def test_load_config_reads_and_normalizes_all_sections(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow_root = tmp_path / "workflow_root"
    workflow_root.mkdir()
    config_path = _write_config(
        tmp_path / "chemstack.yaml",
        f"""
        scheduler:
          max_active_simulations: "6"
          admission_root: /tmp/admission
        workflow:
          root: {workflow_root}
          paths:
            crest_executable: " /opt/crest "
        behavior:
          auto_organize_on_terminal: "yes"
        resources:
          max_cores_per_task: "12"
          max_memory_gb_per_task: "48"
        telegram:
          bot_token: " token-123 "
          chat_id: " 4567 "
        """,
    )
    monkeypatch.setattr(config_mod, "default_config_path", lambda: str(config_path))

    cfg = config_mod.load_config()

    assert cfg.runtime.allowed_root == str((workflow_root / "internal" / "crest" / "runs").resolve())
    assert cfg.runtime.organized_root == str((workflow_root / "internal" / "crest" / "outputs").resolve())
    assert cfg.runtime.max_concurrent == 6
    assert cfg.runtime.admission_root == "/tmp/admission"
    assert cfg.runtime.admission_limit == 6
    assert cfg.paths.crest_executable == "/opt/crest"
    assert cfg.behavior.auto_organize_on_terminal is True
    assert cfg.resources.max_cores_per_task == 12
    assert cfg.resources.max_memory_gb_per_task == 48
    assert cfg.telegram.bot_token == "token-123"
    assert cfg.telegram.chat_id == "4567"


def test_load_config_no_longer_supports_top_level_runtime_and_paths_shape(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "chemstack.yaml",
        """
        scheduler:
          max_active_simulations: "6"
          admission_root: /tmp/admission
        runtime:
          allowed_root: /tmp/runs
          organized_root: /tmp/organized
        paths:
          crest_executable: " /opt/crest "
        behavior:
          auto_organize_on_terminal: "yes"
        resources:
          max_cores_per_task: "12"
          max_memory_gb_per_task: "48"
        telegram:
          bot_token: " token-123 "
          chat_id: " 4567 "
        """,
    )

    with pytest.raises(ValueError, match=r"Config is missing workflow\.root"):
        config_mod.load_config(str(config_path))


def test_load_config_applies_defaults_for_missing_or_invalid_sections(tmp_path: Path) -> None:
    workflow_root = tmp_path / "workflow_root"
    workflow_root.mkdir()
    config_path = _write_config(
        tmp_path / "chemstack.yaml",
        f"""
        scheduler:
          max_active_simulations: 0
        workflow:
          root: {workflow_root}
          paths: []
        behavior: invalid
        resources: nope
        telegram: []
        """,
    )

    cfg = config_mod.load_config(str(config_path))

    assert cfg.runtime.allowed_root == str((workflow_root / "internal" / "crest" / "runs").resolve())
    assert cfg.runtime.organized_root == str((workflow_root / "internal" / "crest" / "outputs").resolve())
    assert cfg.runtime.max_concurrent == 1
    assert cfg.runtime.admission_root == str(tmp_path / "admission")
    assert cfg.runtime.admission_limit == 1
    assert cfg.paths.crest_executable == ""
    assert cfg.behavior.auto_organize_on_terminal is False
    assert cfg.resources.max_cores_per_task == 8
    assert cfg.resources.max_memory_gb_per_task == 32
    assert cfg.telegram.bot_token == ""
    assert cfg.telegram.chat_id == ""


def test_load_config_rejects_removed_runtime_scheduler_keys(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "chemstack.yaml",
        """
        crest:
          runtime:
            allowed_root: /tmp/runs
            max_concurrent: 2
        """,
    )

    with pytest.raises(ValueError, match=r"Config is missing workflow\.root"):
        config_mod.load_config(str(config_path))


def test_load_config_rejects_missing_file(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing.yaml"

    with pytest.raises(ValueError, match="Config file not found"):
        config_mod.load_config(str(missing_path))


def test_load_config_rejects_non_mapping_yaml(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "chemstack.yaml",
        """
        - not
        - a
        - mapping
        """,
    )

    with pytest.raises(ValueError, match="Config file is invalid"):
        config_mod.load_config(str(config_path))


def test_load_config_requires_workflow_root(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path / "chemstack.yaml",
        """
        workflow:
          paths:
            crest_executable: /opt/crest
        """,
    )

    with pytest.raises(ValueError, match=r"Config is missing workflow\.root"):
        config_mod.load_config(str(config_path))


@pytest.mark.parametrize(
    ("writer", "loader", "filename"),
    [
        pytest.param(
            state_mod.write_state,
            state_mod.load_state,
            state_mod.STATE_FILE_NAME,
            id="state",
        ),
        pytest.param(
            state_mod.write_report_json,
            state_mod.load_report_json,
            state_mod.REPORT_JSON_FILE_NAME,
            id="report-json",
        ),
        pytest.param(
            state_mod.write_organized_ref,
            state_mod.load_organized_ref,
            state_mod.ORGANIZED_REF_FILE_NAME,
            id="organized-ref",
        ),
    ],
)
def test_json_state_helpers_round_trip(
    tmp_path: Path,
    writer: JsonWriter,
    loader: JsonLoader,
    filename: str,
) -> None:
    job_dir = tmp_path / "job"
    payload = {"status": "running", "attempt": 1}

    path = writer(job_dir, payload)

    assert path == job_dir / filename
    assert path.exists()
    assert json.loads(path.read_text(encoding="utf-8")) == payload
    assert loader(job_dir) == payload


@pytest.mark.parametrize(
    ("loader", "filename"),
    [
        pytest.param(state_mod.load_state, state_mod.STATE_FILE_NAME, id="state"),
        pytest.param(state_mod.load_report_json, state_mod.REPORT_JSON_FILE_NAME, id="report-json"),
        pytest.param(state_mod.load_organized_ref, state_mod.ORGANIZED_REF_FILE_NAME, id="organized-ref"),
    ],
)
def test_json_state_helpers_return_none_for_missing_invalid_and_non_object_payloads(
    tmp_path: Path,
    loader: JsonLoader,
    filename: str,
) -> None:
    job_dir = tmp_path / "job"

    assert loader(job_dir) is None

    job_dir.mkdir()
    (job_dir / filename).write_text("{invalid json", encoding="utf-8")
    assert loader(job_dir) is None

    (job_dir / filename).write_text('["not", "an", "object"]', encoding="utf-8")
    assert loader(job_dir) is None


def test_write_report_md_writes_expected_markdown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    monkeypatch.setattr(state_mod, "now_utc_iso", lambda: "2026-04-19T00:00:00+00:00")

    path = state_mod.write_report_md(
        job_dir,
        job_id="crest-123",
        status="completed",
        reason="ok",
        selected_xyz="/tmp/input.xyz",
    )

    assert path == job_dir / state_mod.REPORT_MD_FILE_NAME
    assert path.read_text(encoding="utf-8") == (
        "# crest_auto Report\n"
        "\n"
        "- Job ID: `crest-123`\n"
        "- Status: `completed`\n"
        "- Reason: `ok`\n"
        "- Selected XYZ: `/tmp/input.xyz`\n"
        "- Updated At: `2026-04-19T00:00:00+00:00`\n"
    )


def test_write_report_md_lines_writes_lines_with_trailing_newline(tmp_path: Path) -> None:
    job_dir = tmp_path / "job"
    job_dir.mkdir()

    path = state_mod.write_report_md_lines(job_dir, ["# Custom Report", "", "- Item: `value`"])

    assert path == job_dir / state_mod.REPORT_MD_FILE_NAME
    assert path.read_text(encoding="utf-8") == "# Custom Report\n\n- Item: `value`\n"
