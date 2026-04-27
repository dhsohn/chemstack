from __future__ import annotations

import runpy
import warnings
from argparse import Namespace
from pathlib import Path

import pytest

from chemstack.crest.commands import run_dir as run_dir_cmd


def _write_config(tmp_path: Path) -> Path:
    workflow_root = tmp_path / "workflow_root"
    allowed_root = workflow_root / "wf_001" / "01_crest"
    allowed_root.mkdir(parents=True)
    config_path = tmp_path / "chemstack.yaml"
    config_path.write_text(
        "\n".join(
            [
                "workflow:",
                f"  root: {workflow_root}",
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
            message=r"'chemstack\.crest\._internal_cli' found in sys\.modules .* prior to execution of 'chemstack\.crest\._internal_cli'",
            category=RuntimeWarning,
        )
        with pytest.raises(SystemExit) as exc_info:
            runpy.run_module("chemstack.crest._internal_cli", run_name="__main__")

    assert exc_info.value.code == 0
    assert capsys.readouterr().out == "No CREST jobs found.\n"


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
