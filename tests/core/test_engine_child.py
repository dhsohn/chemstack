from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from chemstack.core.queue import engine_child
from chemstack.core.queue.child_entrypoint import ChildWorkerEntrypointJob


def test_build_engine_worker_child_command_supports_admission_root_modes(
    tmp_path: Path,
) -> None:
    with_root = engine_child.WorkerChildCommandSpec("chemstack.xtb.worker_execution")
    without_root = engine_child.WorkerChildCommandSpec(
        "chemstack.crest.worker_execution",
        include_admission_root=False,
    )

    xtb_command = engine_child.build_engine_worker_child_command(
        spec=with_root,
        config_path="/tmp/chemstack.yaml",
        queue_root=tmp_path / "queue",
        queue_id="queue-1",
        admission_root="/tmp/admission",
        admission_token="slot-1",
    )
    crest_command = engine_child.build_engine_worker_child_command(
        spec=without_root,
        config_path="/tmp/chemstack.yaml",
        queue_root=tmp_path / "queue",
        queue_id="queue-2",
        admission_token="slot-2",
    )

    assert "--admission-root" in xtb_command
    assert "/tmp/admission" in xtb_command
    assert "--admission-root" not in crest_command
    assert "--admission-token" in crest_command


def test_run_child_job_with_admission_scope_releases_and_returns_status(
    tmp_path: Path,
) -> None:
    cfg = SimpleNamespace(admission_root=tmp_path / "admission")
    job = ChildWorkerEntrypointJob(
        cfg=cfg,
        queue_root=tmp_path / "queue",
        entry=SimpleNamespace(queue_id="queue-1"),
        _admission_root_fn=lambda loaded_cfg: loaded_cfg.admission_root,
    )
    released: list[tuple[Path, str]] = []

    result = engine_child.run_child_job_with_admission_scope(
        job,
        "slot-1",
        release_slot_fn=lambda root, token: released.append((Path(root), token)),
        run_job_fn=lambda loaded_job: 7 if loaded_job is job else 1,
    )

    assert result == 7
    assert released == [(cfg.admission_root, "slot-1")]


def test_outcome_exit_code_maps_terminal_statuses() -> None:
    assert (
        engine_child.outcome_exit_code(SimpleNamespace(result=SimpleNamespace(status="completed")))
        == 0
    )
    assert (
        engine_child.outcome_exit_code(SimpleNamespace(result=SimpleNamespace(status="cancelled")))
        == 0
    )
    assert engine_child.outcome_exit_code(SimpleNamespace(result=SimpleNamespace(status="failed"))) == 1
