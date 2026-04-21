from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from chemstack.core.config.schema import CommonResourceConfig, CommonRuntimeConfig, TelegramConfig

from chemstack.crest.config import AppConfig, BehaviorConfig, PathsConfig
from chemstack.crest.runner import (
    CrestRunningJob,
    _bool_flag,
    _build_command,
    _count_xyz_structures,
    _manifest_int,
    _resolve_crest_executable,
    _retained_outputs,
    finalize_crest_job,
)


def _cfg(tmp_path: Path, *, crest_executable: str = "/opt/crest") -> AppConfig:
    return AppConfig(
        runtime=CommonRuntimeConfig(
            allowed_root=str(tmp_path / "runs"),
            organized_root=str(tmp_path / "organized"),
        ),
        paths=PathsConfig(crest_executable=crest_executable),
        behavior=BehaviorConfig(),
        resources=CommonResourceConfig(max_cores_per_task=4, max_memory_gb_per_task=8),
        telegram=TelegramConfig(),
    )


def _write_xyz(path: Path, labels: tuple[str, ...]) -> None:
    lines: list[str] = []
    for label in labels:
        lines.extend(
            [
                "1",
                label,
                "H 0.0 0.0 0.0",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _running_job(
    tmp_path: Path,
    *,
    poll_result: int | None,
    wait_result: int | None = None,
) -> tuple[CrestRunningJob, MagicMock]:
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    stdout_path = job_dir / "crest.stdout.log"
    stderr_path = job_dir / "crest.stderr.log"
    stdout_handle = stdout_path.open("w", encoding="utf-8")
    stderr_handle = stderr_path.open("w", encoding="utf-8")
    process = MagicMock()
    process.poll.return_value = poll_result
    process.wait.return_value = wait_result
    return (
        CrestRunningJob(
            process=process,
            command=("crest", "input.xyz"),
            started_at="2026-04-19T00:00:00+00:00",
            stdout_log=str(stdout_path.resolve()),
            stderr_log=str(stderr_path.resolve()),
            stdout_handle=stdout_handle,
            stderr_handle=stderr_handle,
            selected_input_xyz=str((job_dir / "input.xyz").resolve()),
            mode="standard",
            manifest_path="",
            resource_request={"max_cores": 4, "max_memory_gb": 8},
            resource_actual={"assigned_cores": 4, "memory_limit_gb": 8},
            job_dir=str(job_dir.resolve()),
        ),
        process,
    )


def test_resolve_crest_executable_raises_for_missing_configured_path(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, crest_executable=str(tmp_path / "missing-crest"))

    with pytest.raises(ValueError, match="Configured CREST executable not found"):
        _resolve_crest_executable(cfg)


def test_resolve_crest_executable_returns_resolved_configured_file(tmp_path: Path) -> None:
    crest_path = tmp_path / "crest"
    crest_path.write_text("#!/bin/sh\n", encoding="utf-8")
    cfg = _cfg(tmp_path, crest_executable=str(crest_path))

    assert _resolve_crest_executable(cfg) == str(crest_path.resolve())


def test_resolve_crest_executable_falls_back_to_path_lookup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = _cfg(tmp_path, crest_executable="  ")
    monkeypatch.setattr("chemstack.crest.runner.shutil.which", lambda _name: "/usr/bin/crest")

    assert _resolve_crest_executable(cfg) == "/usr/bin/crest"


def test_resolve_crest_executable_raises_when_not_configured_and_not_on_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = _cfg(tmp_path, crest_executable="")
    monkeypatch.setattr("chemstack.crest.runner.shutil.which", lambda _name: None)

    with pytest.raises(ValueError, match="not configured and not found on PATH"):
        _resolve_crest_executable(cfg)


@pytest.mark.parametrize(
    ("manifest", "expected"),
    [
        ({}, False),
        ({"dry_run": False}, False),
        ({"dry_run": True}, True),
        ({"dry_run": " On "}, True),
        ({"dry_run": "0"}, False),
    ],
)
def test_bool_flag_handles_bool_and_string_edge_values(
    manifest: dict[str, object],
    expected: bool,
) -> None:
    assert _bool_flag(manifest, "dry_run") is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        (0, None),
        ("0", None),
        ("   ", None),
        (" 7 ", 7),
        (2.9, 2),
    ],
)
def test_manifest_int_handles_empty_and_numeric_like_values(
    value: object,
    expected: int | None,
) -> None:
    assert _manifest_int({"charge": value}, "charge") == expected


def test_manifest_int_rejects_non_integer_compatible_values() -> None:
    with pytest.raises(ValueError, match="must be an integer-compatible value"):
        _manifest_int({"charge": object()}, "charge")


def test_build_command_omits_unsupported_gfn_and_solvent_model(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = _cfg(tmp_path)
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    selected_xyz = job_dir / "input.xyz"
    _write_xyz(selected_xyz, ("conf_a",))
    monkeypatch.setattr("chemstack.crest.runner._resolve_crest_executable", lambda _cfg: "/usr/bin/crest")

    command = _build_command(
        cfg,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        manifest={
            "gfn": "gfn42",
            "solvent_model": "cosmo",
            "solvent": "water",
        },
    )

    assert "--gfn1" not in command
    assert "--gfn2" not in command
    assert "--gfnff" not in command
    assert "--gfn2//gfnff" not in command
    assert "--gbsa" not in command
    assert "--alpb" not in command
    assert "water" not in command


def test_build_command_omits_solvent_flag_when_solvent_is_blank(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = _cfg(tmp_path)
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    selected_xyz = job_dir / "input.xyz"
    _write_xyz(selected_xyz, ("conf_a",))
    monkeypatch.setattr("chemstack.crest.runner._resolve_crest_executable", lambda _cfg: "/usr/bin/crest")

    command = _build_command(
        cfg,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        manifest={
            "solvent_model": "gbsa",
            "solvent": "   ",
        },
    )

    assert "--gbsa" not in command
    assert "--alpb" not in command


def test_count_xyz_structures_returns_zero_when_file_cannot_be_read(tmp_path: Path) -> None:
    assert _count_xyz_structures(tmp_path / "missing.xyz") == 0


def test_count_xyz_structures_stops_after_first_invalid_record(tmp_path: Path) -> None:
    xyz_path = tmp_path / "ensemble.xyz"
    xyz_path.write_text(
        "\n".join(
            [
                "1",
                "conf_a",
                "H 0.0 0.0 0.0",
                "not-an-atom-count",
                "conf_b",
                "H 0.0 0.0 0.0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    assert _count_xyz_structures(xyz_path) == 1


def test_count_xyz_structures_skips_blank_lines_and_truncated_tail(tmp_path: Path) -> None:
    xyz_path = tmp_path / "ensemble.xyz"
    xyz_path.write_text("\n\n1\n", encoding="utf-8")

    assert _count_xyz_structures(xyz_path) == 1


def test_retained_outputs_returns_empty_when_no_candidates_exist(tmp_path: Path) -> None:
    assert _retained_outputs(tmp_path) == (0, ())


def test_finalize_crest_job_waits_for_running_process_and_uses_failed_defaults(
    tmp_path: Path,
) -> None:
    running, process = _running_job(tmp_path, poll_result=None, wait_result=7)

    result = finalize_crest_job(running)

    process.wait.assert_called_once_with()
    assert result.status == "failed"
    assert result.reason == "crest_exit_code_7"
    assert result.exit_code == 7
    assert result.retained_conformer_count == 0
    assert result.retained_conformer_paths == ()


def test_finalize_crest_job_allows_forced_status_with_default_reason(tmp_path: Path) -> None:
    running, process = _running_job(tmp_path, poll_result=0)

    result = finalize_crest_job(running, forced_status="cancelled")

    process.wait.assert_not_called()
    assert result.status == "cancelled"
    assert result.reason == "completed"
    assert result.exit_code == 0


def test_finalize_crest_job_allows_forced_reason_with_default_status(tmp_path: Path) -> None:
    running, _process = _running_job(tmp_path, poll_result=9)

    result = finalize_crest_job(running, forced_reason="manual_override")

    assert result.status == "failed"
    assert result.reason == "manual_override"
    assert result.exit_code == 9
