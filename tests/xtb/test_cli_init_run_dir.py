from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from chemstack.core.queue import DuplicateQueueEntryError

from chemstack.xtb import cli
from chemstack.xtb.commands import run_dir
from chemstack.xtb.state import STATE_FILE_NAME, load_state


def _write_xyz(path: Path, comment: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "3",
                comment,
                "O 0.000000 0.000000 0.000000",
                "H 0.000000 0.000000 0.970000",
                "H 0.000000 0.750000 -0.240000",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def _write_manifest(job_dir: Path, payload: dict[str, object]) -> Path:
    path = job_dir / "xtb_job.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _write_config(tmp_path: Path) -> tuple[Path, Path, Path]:
    allowed_root = tmp_path / "allowed_root"
    organized_root = tmp_path / "organized_root"
    allowed_root.mkdir()
    organized_root.mkdir()
    config_path = tmp_path / "chemstack.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "runtime": {
                    "allowed_root": str(allowed_root),
                    "organized_root": str(organized_root),
                },
                "resources": {
                    "max_cores_per_task": 6,
                    "max_memory_gb_per_task": 24,
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return config_path, allowed_root, organized_root


def test_cmd_run_dir_path_search_submits_and_writes_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    job_dir = allowed_root / "SnAr Path Search"
    selected_reactant = _write_xyz(job_dir / "reactants" / "starter.xyz", "starter")
    _write_xyz(job_dir / "reactants" / "xtb_seed.xyz", "excluded")
    _write_xyz(job_dir / "products" / "coord.xyz", "excluded")
    selected_product = _write_xyz(job_dir / "products" / "product.xyz", "product")
    _write_manifest(
        job_dir,
        {
            "job_type": "path_search",
            "reaction_key": "SnAr Step 2",
            "resources": {"max_cores": 9, "max_memory_gb": 40},
        },
    )

    enqueue_calls: list[dict[str, Any]] = []
    upsert_calls: list[dict[str, Any]] = []
    notify_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(run_dir, "new_job_id", lambda: "xtb_job_001")

    def fake_enqueue(root: str, **kwargs: Any) -> SimpleNamespace:
        enqueue_calls.append({"root": root, **kwargs})
        return SimpleNamespace(queue_id="q_001", priority=kwargs["priority"])

    def fake_notify_job_queued(cfg: Any, **kwargs: Any) -> bool:
        notify_calls.append(kwargs)
        return True

    monkeypatch.setattr(run_dir, "enqueue", fake_enqueue)
    monkeypatch.setattr(run_dir, "upsert_job_record", lambda cfg, **kwargs: upsert_calls.append(kwargs))
    monkeypatch.setattr(run_dir, "notify_job_queued", fake_notify_job_queued)

    result = run_dir.cmd_run_dir(
        SimpleNamespace(config=str(config_path), path=str(job_dir), priority=7)
    )

    captured = capsys.readouterr()
    queued_state = load_state(job_dir)

    assert result == 0
    assert queued_state is not None
    assert queued_state["job_id"] == "xtb_job_001"
    assert queued_state["job_type"] == "path_search"
    assert queued_state["reaction_key"] == "snar_step_2"
    assert queued_state["selected_input_xyz"] == str(selected_reactant.resolve())
    assert queued_state["input_summary"] == {
        "reactant_xyz": str(selected_reactant.resolve()),
        "product_xyz": str(selected_product.resolve()),
        "reactant_count": 2,
        "product_count": 2,
    }
    assert queued_state["resource_request"] == {"max_cores": 9, "max_memory_gb": 40}
    assert queued_state["resource_actual"] == {"max_cores": 9, "max_memory_gb": 40}

    assert enqueue_calls == [
        {
            "root": str(allowed_root),
            "app_name": "xtb_auto",
            "task_id": "xtb_job_001",
            "task_kind": "xtb_path_search",
            "engine": "xtb",
            "priority": 7,
            "metadata": {
                "job_dir": str(job_dir.resolve()),
                "selected_input_xyz": str(selected_reactant.resolve()),
                "secondary_input_xyz": str(selected_product.resolve()),
                "job_type": "path_search",
                "reaction_key": "snar_step_2",
                "input_summary": {
                    "reactant_xyz": str(selected_reactant.resolve()),
                    "product_xyz": str(selected_product.resolve()),
                    "reactant_count": 2,
                    "product_count": 2,
                },
                "manifest_present": "true",
                "candidate_paths": [],
                "resource_request": {"max_cores": 9, "max_memory_gb": 40},
                "resource_actual": {"max_cores": 9, "max_memory_gb": 40},
            },
        }
    ]
    assert upsert_calls == [
        {
            "job_id": "xtb_job_001",
            "status": "queued",
            "job_dir": job_dir.resolve(),
            "job_type": "path_search",
            "selected_input_xyz": str(selected_reactant.resolve()),
            "reaction_key": "snar_step_2",
            "resource_request": {"max_cores": 9, "max_memory_gb": 40},
            "resource_actual": {"max_cores": 9, "max_memory_gb": 40},
        }
    ]
    assert notify_calls == [
        {
            "job_id": "xtb_job_001",
            "queue_id": "q_001",
            "job_dir": job_dir.resolve(),
            "job_type": "path_search",
            "reaction_key": "snar_step_2",
            "selected_xyz": selected_reactant.resolve(),
        }
    ]
    assert "status: queued" in captured.out
    assert "job_id: xtb_job_001" in captured.out
    assert "selected_input_xyz: starter.xyz" in captured.out


def test_cmd_run_dir_ranking_reports_candidate_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    job_dir = allowed_root / "Ranking Batch"
    first_candidate = _write_xyz(job_dir / "candidates" / "a.xyz", "first")
    second_candidate = _write_xyz(job_dir / "candidates" / "b.xyz", "second")
    _write_manifest(
        job_dir,
        {
            "job_type": "ranking",
            "top_n": 2,
        },
    )

    enqueue_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(run_dir, "new_job_id", lambda: "xtb_rank_001")

    def fake_enqueue(root: str, **kwargs: Any) -> SimpleNamespace:
        enqueue_calls.append({"root": root, **kwargs})
        return SimpleNamespace(queue_id="q_rank", priority=kwargs["priority"])

    monkeypatch.setattr(run_dir, "enqueue", fake_enqueue)
    monkeypatch.setattr(run_dir, "upsert_job_record", lambda cfg, **kwargs: kwargs)
    monkeypatch.setattr(run_dir, "notify_job_queued", lambda cfg, **kwargs: True)

    result = run_dir.cmd_run_dir(
        SimpleNamespace(config=str(config_path), path=str(job_dir), priority=3)
    )

    captured = capsys.readouterr()
    queued_state = load_state(job_dir)

    assert result == 0
    assert queued_state is not None
    assert queued_state["job_type"] == "ranking"
    assert queued_state["candidate_count"] == 2
    assert queued_state["candidate_paths"] == [
        str(first_candidate.resolve()),
        str(second_candidate.resolve()),
    ]
    assert enqueue_calls[0]["metadata"]["candidate_paths"] == [
        str(first_candidate.resolve()),
        str(second_candidate.resolve()),
    ]
    assert enqueue_calls[0]["metadata"]["secondary_input_xyz"] == ""
    assert "candidate_count: 2" in captured.out


def test_cmd_run_dir_duplicate_queue_entry_returns_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    job_dir = allowed_root / "Duplicate Entry"
    selected_input = _write_xyz(job_dir / "input.xyz", "single input")
    _write_manifest(
        job_dir,
        {
            "job_type": "sp",
            "input_xyz": selected_input.name,
        },
    )

    monkeypatch.setattr(run_dir, "new_job_id", lambda: "xtb_dup_001")
    monkeypatch.setattr(
        run_dir,
        "enqueue",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            DuplicateQueueEntryError("Active queue entry already exists")
        ),
    )
    monkeypatch.setattr(
        run_dir,
        "upsert_job_record",
        lambda *args, **kwargs: pytest.fail("upsert_job_record should not be called"),
    )
    monkeypatch.setattr(
        run_dir,
        "notify_job_queued",
        lambda *args, **kwargs: pytest.fail("notify_job_queued should not be called"),
    )

    result = run_dir.cmd_run_dir(
        SimpleNamespace(config=str(config_path), path=str(job_dir), priority=5)
    )

    captured = capsys.readouterr()

    assert result == 1
    assert "error: Active queue entry already exists" in captured.out
    assert not (job_dir / STATE_FILE_NAME).exists()


def test_cmd_run_dir_requires_job_dir_argument(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, _, _ = _write_config(tmp_path)
    monkeypatch.setattr(run_dir, "load_config", lambda _path=None: SimpleNamespace())

    with pytest.raises(ValueError, match="job directory path is required"):
        run_dir.cmd_run_dir(
            SimpleNamespace(config=str(config_path), path=None, priority=5)
        )


def test_cli_main_init_creates_job_scaffold(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    job_dir = allowed_root / "fresh_job"

    result = cli.main(
        ["--config", str(config_path), "init", "--root", str(job_dir), "--job-type", "path_search"]
    )

    captured = capsys.readouterr()
    manifest = yaml.safe_load((job_dir / "xtb_job.yaml").read_text(encoding="utf-8"))

    assert result == 0
    assert (job_dir / "reactants" / "r1.xyz").exists()
    assert (job_dir / "products" / "p1.xyz").exists()
    assert (job_dir / "README.md").exists()
    assert manifest["job_type"] == "path_search"
    assert manifest["reactant_xyz"] == "r1.xyz"
    assert "created_file: reactants/r1.xyz" in captured.out
    assert "created_file: products/p1.xyz" in captured.out


def test_cli_main_run_dir_accepts_positional_job_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    job_dir = allowed_root / "positional_job"
    job_dir.mkdir()

    captured_args: list[SimpleNamespace] = []

    def fake_cmd_run_dir(args: SimpleNamespace) -> int:
        captured_args.append(args)
        return 23

    monkeypatch.setattr(cli, "cmd_run_dir", fake_cmd_run_dir)

    result = cli.main(["--config", str(config_path), "run-dir", str(job_dir)])

    assert result == 23
    assert len(captured_args) == 1
    assert captured_args[0].path == str(job_dir)
    assert captured_args[0].priority == 10
