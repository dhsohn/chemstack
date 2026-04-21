from __future__ import annotations

import runpy
import warnings
from argparse import Namespace
from pathlib import Path

import pytest

from chemstack.crest.commands import init as init_cmd
from chemstack.crest.commands import run_dir as run_dir_cmd

_MISSING = object()


def _write_config(tmp_path: Path) -> Path:
    allowed_root = tmp_path / "allowed"
    organized_root = tmp_path / "organized"
    allowed_root.mkdir()
    organized_root.mkdir()
    config_path = tmp_path / "chemstack.yaml"
    config_path.write_text(
        "\n".join(
            [
                "runtime:",
                f"  allowed_root: {allowed_root}",
                f"  organized_root: {organized_root}",
                "resources:",
                "  max_cores_per_task: 4",
                "  max_memory_gb_per_task: 8",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return config_path


def test_cli_module_main_raises_system_exit_with_main_return_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = _write_config(tmp_path)
    monkeypatch.setattr("sys.argv", ["crest_auto", "--config", str(config_path), "list"])

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r"'crest_auto\.cli' found in sys\.modules .* prior to execution of 'crest_auto\.cli'",
            category=RuntimeWarning,
        )
        with pytest.raises(SystemExit) as exc_info:
            runpy.run_module("chemstack.crest.cli", run_name="__main__")

    assert exc_info.value.code == 0
    assert capsys.readouterr().out == "No CREST jobs found.\n"


@pytest.mark.parametrize(
    "root_value",
    [_MISSING, "", "   "],
    ids=["missing-root-attr", "empty-root", "blank-root"],
)
def test_cmd_init_returns_error_when_root_is_missing_or_blank(
    tmp_path: Path,
    root_value: object,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = _write_config(tmp_path)
    args = Namespace(config=str(config_path))
    if root_value is not _MISSING:
        args.root = root_value

    assert init_cmd.cmd_init(args) == 1
    assert capsys.readouterr().out == "error: init requires --root\n"


@pytest.mark.parametrize(
    ("job_dir", "reaction_dir"),
    [
        (None, None),
        ("", None),
        ("   ", None),
        (None, ""),
        (None, "   "),
    ],
    ids=[
        "both-missing",
        "empty-job-dir",
        "blank-job-dir",
        "empty-reaction-dir",
        "blank-reaction-dir",
    ],
)
def test_cmd_run_dir_requires_non_blank_job_dir_or_reaction_dir(
    tmp_path: Path,
    job_dir: str | None,
    reaction_dir: str | None,
) -> None:
    config_path = _write_config(tmp_path)

    with pytest.raises(ValueError, match="job directory path is required"):
        run_dir_cmd.cmd_run_dir(
            Namespace(
                config=str(config_path),
                path=None,
                job_dir=job_dir,
                reaction_dir=reaction_dir,
                priority=10,
            )
        )
