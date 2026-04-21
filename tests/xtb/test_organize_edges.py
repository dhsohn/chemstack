from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from typing import cast

import pytest
import yaml

from chemstack.xtb.commands import organize as organize_cmd
from chemstack.xtb.config import load_config
from chemstack.xtb.state import write_state


def _write_config(tmp_path: Path) -> tuple[Path, Path, Path]:
    workflow_root = tmp_path / "workflow_root"
    allowed_root = workflow_root / "internal" / "xtb" / "runs"
    organized_root = workflow_root / "internal" / "xtb" / "outputs"
    allowed_root.mkdir(parents=True)
    organized_root.mkdir(parents=True)
    config_path = tmp_path / "chemstack.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "workflow": {
                    "root": str(workflow_root),
                },
                "resources": {
                    "max_cores_per_task": 4,
                    "max_memory_gb_per_task": 8,
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return config_path, allowed_root, organized_root


def _write_state(job_dir: Path, **overrides: object) -> Path:
    job_dir.mkdir(parents=True, exist_ok=True)
    selected_xyz = job_dir / "selected.xyz"
    selected_xyz.write_text("1\nexample\nH 0 0 0\n", encoding="utf-8")
    payload: dict[str, object] = {
        "job_id": "job-1",
        "job_dir": str(job_dir),
        "status": "completed",
        "job_type": "ranking",
        "reaction_key": "rxn-1",
        "selected_input_xyz": str(selected_xyz),
    }
    payload.update(overrides)
    write_state(job_dir, payload)
    return selected_xyz


def _record_notify_call(
    calls: list[dict[str, object]],
    cfg_arg: object,
    *,
    organized_count: int,
    skipped_count: int,
    root: object,
) -> bool:
    calls.append(
        {
            "cfg": cfg_arg,
            "organized_count": organized_count,
            "skipped_count": skipped_count,
            "root": root,
        }
    )
    return True


def test_resolve_scope_rejects_mutually_exclusive_job_dir_and_root(tmp_path: Path) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    cfg = load_config(str(config_path))
    job_dir = allowed_root / "runs" / "job-scope"
    job_dir.mkdir(parents=True)

    with pytest.raises(ValueError, match="mutually exclusive"):
        organize_cmd._resolve_scope(
            cfg,
            Namespace(job_dir=str(job_dir), root=str(allowed_root)),
        )


def test_resolve_scope_rejects_scan_root_outside_allowed_root(tmp_path: Path) -> None:
    config_path, _, _ = _write_config(tmp_path)
    cfg = load_config(str(config_path))
    outside_root = tmp_path / "outside"
    outside_root.mkdir()

    with pytest.raises(ValueError, match="Scan root must be under allowed_root"):
        organize_cmd._resolve_scope(
            cfg,
            Namespace(job_dir=None, root=str(outside_root)),
        )


def test_resolve_scope_defaults_to_allowed_root_when_no_target_is_given(tmp_path: Path) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    cfg = load_config(str(config_path))

    job_dir, root = organize_cmd._resolve_scope(
        cfg,
        Namespace(job_dir=None, root=None),
    )

    assert job_dir is None
    assert root == allowed_root.resolve()


def test_collect_plan_for_dir_returns_skip_reasons_for_edge_cases(tmp_path: Path) -> None:
    config_path, allowed_root, organized_root = _write_config(tmp_path)
    cfg = load_config(str(config_path))

    missing_state_dir = allowed_root / "runs" / "missing-state"
    missing_state_dir.mkdir(parents=True)
    assert organize_cmd._collect_plan_for_dir(cfg, missing_state_dir) == {
        "action": "skip",
        "job_dir": str(missing_state_dir),
        "reason": "missing_state",
    }

    non_terminal_dir = allowed_root / "runs" / "non-terminal"
    _write_state(non_terminal_dir, status="")
    assert organize_cmd._collect_plan_for_dir(cfg, non_terminal_dir) == {
        "action": "skip",
        "job_dir": str(non_terminal_dir),
        "reason": "non_terminal:unknown",
    }

    missing_job_id_dir = allowed_root / "runs" / "missing-job-id"
    _write_state(missing_job_id_dir, job_id="", status="completed")
    assert organize_cmd._collect_plan_for_dir(cfg, missing_job_id_dir) == {
        "action": "skip",
        "job_dir": str(missing_job_id_dir),
        "reason": "missing_job_id",
    }

    already_organized_dir = organized_root / "ranking" / "rxn-1" / "job-1"
    _write_state(already_organized_dir)
    assert organize_cmd._collect_plan_for_dir(cfg, already_organized_dir) == {
        "action": "skip",
        "job_dir": str(already_organized_dir),
        "reason": "already_under_organized_root",
    }

    target_exists_dir = allowed_root / "runs" / "target-exists"
    _write_state(target_exists_dir, job_id="job-2", reaction_key="rxn-2")
    target_dir = organized_root / "ranking" / "rxn-2" / "job-2"
    target_dir.mkdir(parents=True)
    assert organize_cmd._collect_plan_for_dir(cfg, target_exists_dir) == {
        "action": "skip",
        "job_dir": str(target_exists_dir),
        "job_id": "job-2",
        "reason": "target_exists",
        "target_dir": str(target_dir),
    }


def test_rewrite_helpers_cover_fallback_and_nested_path_cases(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original_run_dir = tmp_path / "original"
    target_dir = tmp_path / "target"
    original_run_dir.mkdir()
    target_dir.mkdir()

    nested_source = original_run_dir / "inputs" / "selected.xyz"
    nested_source.parent.mkdir(parents=True)
    nested_source.write_text("1\nexample\nH 0 0 0\n", encoding="utf-8")

    nested_target = target_dir / "inputs" / "selected.xyz"
    nested_target.parent.mkdir(parents=True)
    nested_target.write_text("1\nexample\nH 0 0 0\n", encoding="utf-8")

    original_outputs = original_run_dir / "outputs"
    original_outputs.mkdir()
    target_outputs = target_dir / "outputs"
    target_outputs.mkdir()

    outside_source = tmp_path / "outside-artifact.txt"
    outside_source.write_text("outside\n", encoding="utf-8")
    outside_target = target_dir / outside_source.name
    outside_target.write_text("outside\n", encoding="utf-8")

    missing_source = original_run_dir / "missing.txt"

    original_resolve = organize_cmd.Path.resolve

    def fake_resolve(self: Path, strict: bool = False) -> Path:
        if self.name == "resolve-error.txt":
            raise OSError("forced resolution failure")
        return original_resolve(self, strict=strict)

    monkeypatch.setattr(organize_cmd.Path, "resolve", fake_resolve)

    assert organize_cmd._rewrite_artifact_path(original_run_dir, target_dir, "   ") == ""
    assert (
        organize_cmd._rewrite_artifact_path(original_run_dir, target_dir, str(nested_source))
        == str(nested_target.resolve())
    )
    assert (
        organize_cmd._rewrite_artifact_path(original_run_dir, target_dir, str(outside_source))
        == str(outside_target.resolve())
    )
    assert organize_cmd._rewrite_artifact_path(original_run_dir, target_dir, str(missing_source)) == str(
        missing_source
    )
    assert organize_cmd._rewrite_artifact_path(
        original_run_dir,
        target_dir,
        str(tmp_path / "resolve-error.txt"),
    ) == str(tmp_path / "resolve-error.txt")

    assert organize_cmd._rewrite_path_like_mapping(original_run_dir, target_dir, None) == {}

    rewritten_mapping = organize_cmd._rewrite_path_like_mapping(
        original_run_dir,
        target_dir,
        {
            "path": str(nested_source),
            "input_summary": {
                "output_dir": str(original_outputs),
                "candidate_paths": [str(nested_source), 4],
            },
            "details": [{"path": str(nested_source)}, {"nested": {"best_path": str(nested_source)}}],
            "untouched": ["a", "b"],
        },
    )
    assert rewritten_mapping["path"] == str(nested_target.resolve())
    assert rewritten_mapping["input_summary"]["output_dir"] == str(target_outputs.resolve())
    assert rewritten_mapping["input_summary"]["candidate_paths"] == [str(nested_target.resolve()), 4]
    assert rewritten_mapping["details"] == [
        {"path": str(nested_target.resolve())},
        {"nested": {"best_path": str(nested_target.resolve())}},
    ]
    assert rewritten_mapping["untouched"] == ["a", "b"]

    assert organize_cmd._rewrite_candidate_details(original_run_dir, target_dir, None) == []
    assert organize_cmd._rewrite_candidate_details(
        original_run_dir,
        target_dir,
        [
            "skip-me",
            {"path": str(nested_source)},
            {"path": str(outside_source)},
        ],
    ) == [
        {"path": str(nested_target.resolve())},
        {"path": str(outside_target.resolve())},
    ]


def test_organize_job_dir_skips_without_notifying(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    cfg = load_config(str(config_path))
    job_dir = allowed_root / "runs" / "missing-state"
    job_dir.mkdir(parents=True)

    called = False

    def fail_notify(*args: object, **kwargs: object) -> None:
        nonlocal called
        called = True
        raise AssertionError("notify_organize_summary should not run for skipped jobs")

    monkeypatch.setattr(organize_cmd, "notify_organize_summary", fail_notify)

    plan = organize_cmd.organize_job_dir(cfg, job_dir, notify_summary=True)

    assert plan == {
        "action": "skip",
        "job_dir": str(job_dir),
        "reason": "missing_state",
    }
    assert not called


def test_organize_job_dir_notifies_on_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, allowed_root, organized_root = _write_config(tmp_path)
    cfg = load_config(str(config_path))
    job_dir = allowed_root / "runs" / "job-1"
    _write_state(job_dir, job_id="job-1", reaction_key="rxn-1")
    target_dir = organized_root / "ranking" / "rxn-1" / "job-1"

    notified: list[dict[str, object]] = []
    monkeypatch.setattr(
        organize_cmd,
        "notify_organize_summary",
        lambda cfg_arg, *, organized_count, skipped_count, root: _record_notify_call(
            notified,
            cfg_arg,
            organized_count=organized_count,
            skipped_count=skipped_count,
            root=root,
        ),
    )

    plan = organize_cmd.organize_job_dir(cfg, job_dir, notify_summary=True)

    assert plan == {
        "action": "organized",
        "job_id": "job-1",
        "status": "completed",
        "job_dir": str(job_dir.resolve()),
        "target_dir": str(target_dir.resolve()),
        "job_type": "ranking",
        "reaction_key": "rxn-1",
    }
    assert target_dir.exists()
    assert notified == [
        {
            "cfg": cfg,
            "organized_count": 1,
            "skipped_count": 0,
            "root": job_dir,
        }
    ]


def test_cmd_organize_raises_when_scope_does_not_resolve_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, _, _ = _write_config(tmp_path)
    cfg = load_config(str(config_path))

    monkeypatch.setattr(organize_cmd, "_resolve_scope", lambda cfg_arg, args: (None, None))
    monkeypatch.setattr(organize_cmd, "load_config", lambda _path=None: cfg)

    with pytest.raises(ValueError, match="Scan root could not be resolved"):
        organize_cmd.cmd_organize(
            Namespace(
                config=str(config_path),
                job_dir=None,
                root=None,
                apply=False,
            )
        )


def test_cmd_organize_dry_run_reports_counts_and_candidates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path, allowed_root, organized_root = _write_config(tmp_path)
    cfg = load_config(str(config_path))

    organize_dir = allowed_root / "runs" / "job-organize"
    skip_dir = allowed_root / "runs" / "job-skip"
    _write_state(organize_dir, job_id="job-organize", reaction_key="rxn-a")
    _write_state(skip_dir, job_id="job-skip", status="", reaction_key="rxn-b")

    monkeypatch.setattr(organize_cmd, "load_config", lambda _path=None: cfg)

    exit_code = organize_cmd.cmd_organize(
        Namespace(config=str(config_path), job_dir=None, root=str(allowed_root), apply=False)
    )

    captured = capsys.readouterr().out
    assert exit_code == 0
    assert "action: dry_run" in captured
    assert "to_organize: 1" in captured
    assert "skipped: 1" in captured
    assert f"job-organize: {organize_dir.resolve()} -> {(organized_root / 'ranking' / 'rxn-a' / 'job-organize').resolve()}" in captured


def test_cmd_organize_apply_reports_failure_and_notifies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    cfg = load_config(str(config_path))
    job_dir = allowed_root / "runs" / "job-fail"
    _write_state(job_dir, job_id="job-fail", reaction_key="rxn-fail")

    monkeypatch.setattr(organize_cmd, "load_config", lambda _path=None: cfg)

    def boom(*args: object, **kwargs: object) -> dict[str, str]:
        raise RuntimeError("boom")

    notify_calls: list[dict[str, object]] = []
    monkeypatch.setattr(organize_cmd, "organize_job_dir", boom)
    monkeypatch.setattr(
        organize_cmd,
        "notify_organize_summary",
        lambda cfg_arg, *, organized_count, skipped_count, root: _record_notify_call(
            notify_calls,
            cfg_arg,
            organized_count=organized_count,
            skipped_count=skipped_count,
            root=root,
        ),
    )

    exit_code = organize_cmd.cmd_organize(
        Namespace(config=str(config_path), job_dir=str(job_dir), root=None, apply=True)
    )

    captured = capsys.readouterr().out
    assert exit_code == 1
    assert "action: apply" in captured
    assert "organized: 0" in captured
    assert "skipped: 0" in captured
    assert "failed: 1" in captured
    assert "failed: job-fail (boom)" in captured
    assert len(notify_calls) == 1
    assert notify_calls[0]["cfg"] is cfg
    assert notify_calls[0]["organized_count"] == 0
    assert notify_calls[0]["skipped_count"] == 1
    assert Path(cast(str, notify_calls[0]["root"])) == job_dir.resolve()
