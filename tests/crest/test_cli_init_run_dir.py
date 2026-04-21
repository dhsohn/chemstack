from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path
from typing import Any

import pytest

from chemstack.core.indexing import get_job_location
from chemstack.core.queue import list_queue

from chemstack.crest import cli
from chemstack.crest.commands import init as init_cmd
from chemstack.crest.commands import queue as queue_cmd
from chemstack.crest.commands import run_dir as run_dir_cmd
from chemstack.crest.runner import CrestRunResult
from chemstack.crest.state import load_organized_ref, load_report_json, load_state


def _write_config(tmp_path: Path) -> tuple[Path, Path, Path]:
    allowed_root = tmp_path / "allowed"
    organized_root = tmp_path / "organized"
    allowed_root.mkdir()
    organized_root.mkdir()
    config_path = tmp_path / "chemstack.yaml"
    config_path.write_text(
        "\n".join(
            [
                "runtime:",
                f"  allowed_root: {json.dumps(str(allowed_root))}",
                f"  organized_root: {json.dumps(str(organized_root))}",
                "resources:",
                "  max_cores_per_task: 6",
                "  max_memory_gb_per_task: 14",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return config_path, allowed_root, organized_root


def _write_xyz(path: Path, label: str = "sample") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"1\n{label}\nH 0.0 0.0 0.0\n", encoding="utf-8")


def test_build_parser_supports_run_dir_alias_and_queue_subcommands() -> None:
    parser = cli.build_parser()

    run_dir_args = parser.parse_args(["run-dir", "jobs/demo", "--priority", "3"])
    worker_args = parser.parse_args(["queue", "worker", "--once", "--auto-organize"])
    cancel_args = parser.parse_args(["queue", "cancel", "q-123"])

    assert run_dir_args.command == "run-dir"
    assert run_dir_args.path == "jobs/demo"
    assert run_dir_args.priority == 3

    assert worker_args.command == "queue"
    assert worker_args.queue_command == "worker"
    assert worker_args.once is True
    assert worker_args.auto_organize is True
    assert worker_args.no_auto_organize is False

    assert cancel_args.command == "queue"
    assert cancel_args.queue_command == "cancel"
    assert cancel_args.target == "q-123"


@pytest.mark.parametrize(
    ("argv", "attr_name", "expected_result"),
    [
        (["init", "--root", "/tmp/job"], "cmd_init", 11),
        (["run-dir", "/tmp/job"], "cmd_run_dir", 12),
        (["list"], "cmd_list", 13),
        (["organize"], "cmd_organize", 14),
        (["reindex"], "cmd_reindex", 15),
        (["summary", "job-123"], "cmd_summary", 16),
    ],
)
def test_main_dispatches_top_level_commands(
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
    attr_name: str,
    expected_result: int,
) -> None:
    seen: list[Any] = []

    def _stub(args: Any) -> int:
        seen.append(args)
        return expected_result

    monkeypatch.setattr(cli, attr_name, _stub)

    assert cli.main(argv) == expected_result
    assert len(seen) == 1


def test_main_dispatches_queue_worker_and_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    worker_calls: list[Any] = []
    cancel_calls: list[Any] = []

    def _worker(args: Any) -> int:
        worker_calls.append(args)
        return 21

    def _cancel(args: Any) -> int:
        cancel_calls.append(args)
        return 22

    monkeypatch.setattr(cli, "cmd_queue_worker", _worker)
    monkeypatch.setattr(cli, "cmd_queue_cancel", _cancel)

    assert cli.main(["queue", "worker", "--once", "--no-auto-organize"]) == 21
    assert cli.main(["queue", "cancel", "job-123"]) == 22

    assert len(worker_calls) == 1
    assert worker_calls[0].once is True
    assert worker_calls[0].no_auto_organize is True
    assert len(cancel_calls) == 1
    assert cancel_calls[0].target == "job-123"


def test_cmd_queue_rejects_unknown_subcommand() -> None:
    with pytest.raises(ValueError, match="Unsupported queue subcommand: noop"):
        cli._cmd_queue(Namespace(queue_command="noop"))


def test_cmd_init_creates_scaffold_files(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    job_dir = allowed_root / "runs" / "job-init"

    rc = init_cmd.cmd_init(Namespace(config=str(config_path), root=str(job_dir)))

    output = capsys.readouterr().out
    assert rc == 0
    assert f"job_dir: {job_dir.resolve()}" in output
    assert "created: 3" in output
    assert "skipped: 0" in output
    assert "created_file: input.xyz" in output
    assert "created_file: crest_job.yaml" in output
    assert "created_file: README.md" in output

    assert (job_dir / "input.xyz").read_text(encoding="utf-8").startswith("3\nchemstack CREST scaffold\n")
    assert (job_dir / "crest_job.yaml").read_text(encoding="utf-8") == (
        "# chemstack CREST scaffold manifest\n"
        "mode: standard\n"
        "speed: quick\n"
        "gfn: 2\n"
        "input_xyz: input.xyz\n"
    )
    assert (
        f"This directory was created by `python -m chemstack.crest.cli init --root {job_dir.resolve()}`."
        in (job_dir / "README.md").read_text(encoding="utf-8")
    )


def test_main_run_dir_accepts_positional_job_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    job_dir = allowed_root / "runs" / "job-cli"
    job_dir.mkdir(parents=True)

    captured_args: list[Any] = []

    def _stub(args: Any) -> int:
        captured_args.append(args)
        return 19

    monkeypatch.setattr(cli, "cmd_run_dir", _stub)

    assert cli.main(["--config", str(config_path), "run-dir", str(job_dir)]) == 19
    assert len(captured_args) == 1
    assert captured_args[0].path == str(job_dir)


def test_cmd_init_is_idempotent_and_preserves_existing_files(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    job_dir = allowed_root / "runs" / "job-idempotent"

    first_rc = init_cmd.cmd_init(Namespace(config=str(config_path), root=str(job_dir)))
    assert first_rc == 0
    capsys.readouterr()

    custom_xyz = "1\ncustom\nHe 0.0 0.0 0.0\n"
    (job_dir / "input.xyz").write_text(custom_xyz, encoding="utf-8")

    second_rc = init_cmd.cmd_init(Namespace(config=str(config_path), root=str(job_dir)))

    output = capsys.readouterr().out
    assert second_rc == 0
    assert "created: 0" in output
    assert "skipped: 3" in output
    assert "skipped_file: input.xyz" in output
    assert "skipped_file: crest_job.yaml" in output
    assert "skipped_file: README.md" in output
    assert (job_dir / "input.xyz").read_text(encoding="utf-8") == custom_xyz


def test_cmd_run_dir_queues_job_updates_state_and_index(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    job_dir = allowed_root / "runs" / "job-queue"
    job_dir.mkdir(parents=True)
    _write_xyz(job_dir / "fallback.xyz", "fallback")
    _write_xyz(job_dir / "preferred.xyz", "preferred")
    (job_dir / "crest_job.yaml").write_text(
        "mode: nci\ninput_xyz: preferred.xyz\nresources:\n  max_cores: 9\n  max_memory_gb: 21\n",
        encoding="utf-8",
    )

    notifications: list[dict[str, Any]] = []
    monkeypatch.setattr(run_dir_cmd, "new_job_id", lambda: "crest-fixed-id")

    def fake_notify_job_queued(cfg: Any, **kwargs: Any) -> bool:
        notifications.append(kwargs)
        return True

    monkeypatch.setattr(run_dir_cmd, "notify_job_queued", fake_notify_job_queued)

    rc = run_dir_cmd.cmd_run_dir(
        Namespace(
            config=str(config_path),
            path=str(job_dir),
            priority=4,
        )
    )

    output = capsys.readouterr().out
    queue_entries = list_queue(allowed_root)
    state = load_state(job_dir)
    record = get_job_location(allowed_root, "crest-fixed-id")

    assert rc == 0
    assert "status: queued" in output
    assert f"job_dir: {job_dir.resolve()}" in output
    assert "job_id: crest-fixed-id" in output
    assert "priority: 4" in output
    assert "selected_input_xyz: preferred.xyz" in output

    assert len(queue_entries) == 1
    entry = queue_entries[0]
    assert entry.task_id == "crest-fixed-id"
    assert entry.priority == 4
    assert entry.metadata == {
        "job_dir": str(job_dir.resolve()),
        "selected_input_xyz": str((job_dir / "preferred.xyz").resolve()),
        "mode": "nci",
        "molecule_key": "preferred",
        "manifest_present": "true",
        "resource_request": {"max_cores": 9, "max_memory_gb": 21},
        "resource_actual": {"max_cores": 9, "max_memory_gb": 21},
    }

    assert state is not None
    assert state["job_id"] == "crest-fixed-id"
    assert state["job_dir"] == str(job_dir.resolve())
    assert state["selected_input_xyz"] == str((job_dir / "preferred.xyz").resolve())
    assert state["status"] == "queued"
    assert state["mode"] == "nci"
    assert state["molecule_key"] == "preferred"
    assert state["resource_request"] == {"max_cores": 9, "max_memory_gb": 21}
    assert state["resource_actual"] == {"max_cores": 9, "max_memory_gb": 21}

    assert record is not None
    assert record.job_id == "crest-fixed-id"
    assert record.status == "queued"
    assert record.job_type == "crest_nci_conformer_search"
    assert record.original_run_dir == str(job_dir.resolve())
    assert record.latest_known_path == str(job_dir.resolve())
    assert record.selected_input_xyz == str((job_dir / "preferred.xyz").resolve())
    assert record.resource_request == {"max_cores": 9, "max_memory_gb": 21}
    assert record.resource_actual == {"max_cores": 9, "max_memory_gb": 21}

    assert notifications == [
        {
            "job_id": "crest-fixed-id",
            "queue_id": entry.queue_id,
            "job_dir": job_dir.resolve(),
            "mode": "nci",
            "selected_xyz": (job_dir / "preferred.xyz").resolve(),
        }
    ]


def test_cmd_run_dir_reports_duplicate_queue_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path, allowed_root, _ = _write_config(tmp_path)
    job_dir = allowed_root / "runs" / "job-duplicate"
    job_dir.mkdir(parents=True)
    _write_xyz(job_dir / "input.xyz", "input")

    notifications: list[dict[str, Any]] = []
    monkeypatch.setattr(run_dir_cmd, "new_job_id", lambda: "crest-duplicate-id")

    def fake_notify_job_queued(cfg: Any, **kwargs: Any) -> bool:
        notifications.append(kwargs)
        return True

    monkeypatch.setattr(run_dir_cmd, "notify_job_queued", fake_notify_job_queued)

    first_rc = run_dir_cmd.cmd_run_dir(
        Namespace(
            config=str(config_path),
            path=str(job_dir),
            priority=10,
        )
    )
    first_output = capsys.readouterr().out

    second_rc = run_dir_cmd.cmd_run_dir(
        Namespace(
            config=str(config_path),
            path=str(job_dir),
            priority=10,
        )
    )
    second_output = capsys.readouterr().out

    queue_entries = list_queue(allowed_root)
    state = load_state(job_dir)

    assert first_rc == 0
    assert "status: queued" in first_output
    assert second_rc == 1
    assert "error: Active queue entry already exists for app=crest_auto task_id=crest-duplicate-id" in second_output

    assert len(queue_entries) == 1
    assert queue_entries[0].task_id == "crest-duplicate-id"
    assert state is not None
    assert state["job_id"] == "crest-duplicate-id"
    assert len(notifications) == 1


def test_cli_end_to_end_smoke_path_submission_worker_organize_and_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path, allowed_root, organized_root = _write_config(tmp_path)
    job_dir = allowed_root / "runs" / "job-e2e"
    queued_notifications: list[dict[str, Any]] = []
    started_notifications: list[dict[str, Any]] = []
    finished_notifications: list[dict[str, Any]] = []

    monkeypatch.setattr(run_dir_cmd, "new_job_id", lambda: "crest-e2e-001")

    def fake_notify_job_queued(cfg: Any, **kwargs: Any) -> bool:
        queued_notifications.append(kwargs)
        return True

    def fake_notify_job_started(cfg: Any, **kwargs: Any) -> bool:
        started_notifications.append(kwargs)
        return True

    def fake_notify_job_finished(cfg: Any, **kwargs: Any) -> bool:
        finished_notifications.append(kwargs)
        return True

    monkeypatch.setattr(run_dir_cmd, "notify_job_queued", fake_notify_job_queued)
    monkeypatch.setattr(queue_cmd, "notify_job_started", fake_notify_job_started)
    monkeypatch.setattr(queue_cmd, "notify_job_finished", fake_notify_job_finished)

    class _FakeProcess:
        def poll(self) -> int | None:
            return 0

    def fake_start_crest_job(cfg: Any, *, job_dir: Path, selected_xyz: Path) -> Any:
        return type("Running", (), {"process": _FakeProcess()})()

    def fake_finalize_crest_job(running: Any) -> CrestRunResult:
        selected_xyz = job_dir / "input.xyz"
        stdout_log = job_dir / "crest.stdout.log"
        stderr_log = job_dir / "crest.stderr.log"
        retained_path = job_dir / "crest_best.xyz"
        stdout_log.write_text("stdout\n", encoding="utf-8")
        stderr_log.write_text("stderr\n", encoding="utf-8")
        retained_path.write_text("1\nretained\nH 0.0 0.0 0.0\n", encoding="utf-8")
        return CrestRunResult(
            status="completed",
            reason="ok",
            command=("crest", selected_xyz.name, "--T", "6"),
            exit_code=0,
            started_at="2026-04-20T00:00:00+00:00",
            finished_at="2026-04-20T00:05:00+00:00",
            stdout_log=str(stdout_log.resolve()),
            stderr_log=str(stderr_log.resolve()),
            selected_input_xyz=str(selected_xyz.resolve()),
            mode="standard",
            retained_conformer_count=1,
            retained_conformer_paths=(str(retained_path.resolve()),),
            manifest_path=str((job_dir / "crest_job.yaml").resolve()),
            resource_request={"max_cores": 6, "max_memory_gb": 14},
            resource_actual={"assigned_cores": 6, "memory_limit_gb": 14},
        )

    monkeypatch.setattr(queue_cmd, "start_crest_job", fake_start_crest_job)
    monkeypatch.setattr(queue_cmd, "finalize_crest_job", fake_finalize_crest_job)

    assert cli.main(["--config", str(config_path), "init", "--root", str(job_dir)]) == 0
    init_output = capsys.readouterr().out
    assert "created: 3" in init_output

    assert cli.main(["--config", str(config_path), "run-dir", str(job_dir), "--priority", "2"]) == 0
    run_output = capsys.readouterr().out
    assert "status: queued" in run_output
    assert "job_id: crest-e2e-001" in run_output

    assert cli.main(
        [
            "--config",
            str(config_path),
            "queue",
            "worker",
            "--once",
            "--auto-organize",
        ]
    ) == 0
    worker_output = capsys.readouterr().out
    organized_target = organized_root / "standard" / "input" / "crest-e2e-001"
    assert "status: completed" in worker_output
    assert f"organized_output_dir: {organized_target.resolve()}" in worker_output

    queue_entries = list_queue(allowed_root)
    assert len(queue_entries) == 1
    assert queue_entries[0].task_id == "crest-e2e-001"
    assert queue_entries[0].status.value == "completed"

    organized_ref = load_organized_ref(job_dir)
    assert organized_ref is not None
    assert organized_ref["job_id"] == "crest-e2e-001"
    assert organized_ref["organized_output_dir"] == str(organized_target.resolve())

    state = load_state(organized_target)
    report = load_report_json(organized_target)
    assert state is not None
    assert report is not None
    assert state["status"] == "completed"
    assert report["status"] == "completed"
    assert report["retained_conformer_count"] == 1

    record = get_job_location(allowed_root, "crest-e2e-001")
    assert record is not None
    assert record.original_run_dir == str(job_dir.resolve())
    assert record.organized_output_dir == str(organized_target.resolve())
    assert record.latest_known_path == str(organized_target.resolve())

    assert len(queued_notifications) == 1
    assert queued_notifications[0]["job_id"] == "crest-e2e-001"
    assert len(started_notifications) == 1
    assert started_notifications[0]["job_id"] == "crest-e2e-001"
    assert len(finished_notifications) == 1
    assert finished_notifications[0]["status"] == "completed"
    assert finished_notifications[0]["organized_output_dir"] == organized_target.resolve()

    assert cli.main(["--config", str(config_path), "summary", "crest-e2e-001", "--json"]) == 0
    summary_json = json.loads(capsys.readouterr().out)
    assert summary_json["target"] == "crest-e2e-001"
    assert summary_json["job_dir"] == str(organized_target.resolve())
    assert summary_json["index_record"]["latest_known_path"] == str(organized_target.resolve())

    assert cli.main(["--config", str(config_path), "summary", str(job_dir.resolve())]) == 0
    summary_text = capsys.readouterr().out
    assert f"job_dir: {organized_target.resolve()}" in summary_text
    assert "job_id: crest-e2e-001" in summary_text
