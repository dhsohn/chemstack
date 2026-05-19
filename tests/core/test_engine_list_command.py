from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from chemstack.core.commands import engine_list


def _entry(
    queue_id: str,
    *,
    task_id: str,
    priority: int,
    enqueued_at: str,
    metadata: dict[str, object] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        queue_id=queue_id,
        task_id=task_id,
        priority=priority,
        enqueued_at=enqueued_at,
        metadata=metadata or {},
    )


def test_format_row_applies_widths_and_metadata_columns() -> None:
    entry = _entry(
        "q-1",
        task_id="job-1",
        priority=5,
        enqueued_at="2026-05-01T00:00:00Z",
        metadata={"job_dir": "/tmp/work/job-1", "kind": "ranking"},
    )

    row = engine_list.format_row(
        entry,
        (
            engine_list.EngineListColumn(lambda item: item.task_id, width=8),
            engine_list.EngineListColumn(engine_list.metadata_text_column("kind"), width=7),
            engine_list.EngineListColumn(engine_list.metadata_path_name_column("job_dir")),
        ),
    )

    assert row == "job-1    ranking job-1"


def test_cmd_list_reports_empty_queue(
    capsys: pytest.CaptureFixture[str],
) -> None:
    spec = engine_list.EngineListSpec(
        engine_label="xTB",
        header="JOB",
        separator="---",
        columns=(engine_list.EngineListColumn(lambda item: item.task_id),),
    )

    exit_code = engine_list.cmd_list(
        SimpleNamespace(config="cfg.yaml"),
        load_config_fn=lambda config: SimpleNamespace(config=config),
        runtime_roots_for_cfg_fn=lambda _cfg: (Path("/queue"),),
        list_queue_fn=lambda _root: [],
        spec=spec,
    )

    assert exit_code == 0
    assert capsys.readouterr().out == "No xTB jobs found.\n"


def test_cmd_list_sorts_entries_and_prints_spec_columns(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    entries = {
        first: [
            _entry("q-3", task_id="job-c", priority=9, enqueued_at="2026-05-01T00:00:00Z"),
            _entry("q-2", task_id="job-b", priority=1, enqueued_at="2026-05-01T00:00:02Z"),
        ],
        second: [
            _entry(
                "q-1",
                task_id="job-a",
                priority=1,
                enqueued_at="2026-05-01T00:00:01Z",
                metadata={"job_dir": str(tmp_path / "job-a")},
            )
        ],
    }
    spec = engine_list.EngineListSpec(
        engine_label="CREST",
        header="JOB      DIR",
        separator="-----------",
        columns=(
            engine_list.EngineListColumn(lambda item: item.task_id, width=8),
            engine_list.EngineListColumn(engine_list.metadata_path_name_column("job_dir")),
        ),
    )

    exit_code = engine_list.cmd_list(
        SimpleNamespace(config=None),
        load_config_fn=lambda _config: SimpleNamespace(runtime=SimpleNamespace(allowed_root=first)),
        runtime_roots_for_cfg_fn=lambda _cfg: (first, second),
        list_queue_fn=lambda root: entries[root],
        spec=spec,
    )

    assert exit_code == 0
    assert capsys.readouterr().out.splitlines() == [
        "CREST queue: 3 entries",
        "",
        "JOB      DIR",
        "-----------",
        "job-a    job-a",
        "job-b    -",
        "job-c    -",
    ]
