from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import pytest
import yaml

from chemstack.xtb.commands import init as init_cmd


def _write_config(tmp_path: Path) -> tuple[Path, Path]:
    workflow_root = tmp_path / "workflow_root"
    allowed_root = workflow_root / "internal" / "xtb" / "runs"
    allowed_root.mkdir(parents=True)
    config_path = tmp_path / "chemstack.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "workflow": {
                    "root": str(workflow_root),
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return config_path, allowed_root


@pytest.mark.parametrize(
    ("job_type", "expected_error"),
    [
        ("banana", "error: unsupported scaffold job_type: banana"),
    ],
)
def test_cmd_init_rejects_unsupported_job_type(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    job_type: str,
    expected_error: str,
) -> None:
    config_path, _ = _write_config(tmp_path)

    exit_code = init_cmd.cmd_init(
        Namespace(
            config=str(config_path),
            root=str(tmp_path / "job"),
            job_type=job_type,
        )
    )

    captured = capsys.readouterr().out
    assert exit_code == 1
    assert expected_error in captured


def test_cmd_init_requires_root(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config_path, _ = _write_config(tmp_path)

    exit_code = init_cmd.cmd_init(
        Namespace(
            config=str(config_path),
            root="",
            job_type="path_search",
        )
    )

    captured = capsys.readouterr().out
    assert exit_code == 1
    assert "error: scaffold requires --root" in captured


@pytest.mark.parametrize(
    ("helper_name", "call_args"),
    [
        ("_scaffold_manifest", ("banana",)),
        ("_scaffold_readme", (Path("/tmp/job"), "banana")),
    ],
)
def test_scaffold_helpers_reject_unsupported_job_type(
    helper_name: str,
    call_args: tuple[object, ...],
) -> None:
    helper = getattr(init_cmd, helper_name)

    with pytest.raises(ValueError, match="Unsupported scaffold job_type: banana"):
        helper(*call_args)


def test_write_if_missing_returns_false_for_existing_file(tmp_path: Path) -> None:
    path = tmp_path / "existing.txt"
    path.write_text("original\n", encoding="utf-8")

    assert init_cmd._write_if_missing(path, "new\n") is False
    assert path.read_text(encoding="utf-8") == "original\n"


@pytest.mark.parametrize(
    ("job_type", "expected_files", "manifest_snippets", "readme_snippets"),
    [
        (
            "opt",
            ("input.xyz", "xtb_job.yaml", "README.md"),
            ("job_type: opt", "input_xyz: input.xyz"),
            ("optimize", "input.xyz"),
        ),
        (
            "sp",
            ("input.xyz", "xtb_job.yaml", "README.md"),
            ("job_type: sp", "input_xyz: input.xyz"),
            ("single-point", "input.xyz"),
        ),
        (
            "ranking",
            ("xtb_job.yaml", "README.md"),
            ("job_type: ranking", "candidates_dir: candidates", "top_n: 3"),
            ("candidate `.xyz` files", "rank them by xTB energy"),
        ),
    ],
)
def test_cmd_init_scaffolds_supported_job_types_and_reports_skips(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    job_type: str,
    expected_files: tuple[str, ...],
    manifest_snippets: tuple[str, ...],
    readme_snippets: tuple[str, ...],
) -> None:
    config_path, allowed_root = _write_config(tmp_path)
    job_dir = allowed_root / f"{job_type}-job"

    first_exit_code = init_cmd.cmd_init(
        Namespace(
            config=str(config_path),
            root=str(job_dir),
            job_type=job_type,
        )
    )

    first_output = capsys.readouterr().out
    assert first_exit_code == 0
    assert f"job_type: {job_type}" in first_output
    assert f"created: {len(expected_files)}" in first_output
    assert "skipped: 0" in first_output

    if job_type == "ranking":
        assert (job_dir / "candidates").is_dir()
    else:
        assert (job_dir / "input.xyz").exists()

    manifest_text = (job_dir / "xtb_job.yaml").read_text(encoding="utf-8")
    readme_text = (job_dir / "README.md").read_text(encoding="utf-8")
    for snippet in manifest_snippets:
        assert snippet in manifest_text
    for snippet in readme_snippets:
        assert snippet in readme_text
    for name in expected_files:
        assert (job_dir / name).exists()

    second_exit_code = init_cmd.cmd_init(
        Namespace(
            config=str(config_path),
            root=str(job_dir),
            job_type=job_type,
        )
    )

    second_output = capsys.readouterr().out
    assert second_exit_code == 0
    assert "created: 0" in second_output
    assert f"skipped: {len(expected_files)}" in second_output
    for name in expected_files:
        assert f"skipped_file: {name}" in second_output
