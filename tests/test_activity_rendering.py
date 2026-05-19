from __future__ import annotations

from datetime import datetime, timezone

from chemstack import activity_rendering as rendering


def test_queue_elapsed_uses_restart_metadata_and_clamps_negative_durations() -> None:
    item = {
        "status": "completed",
        "submitted_at": "2026-05-20T00:00:00+00:00",
        "updated_at": "2026-05-20T00:00:05+00:00",
        "metadata": {
            "restart_summary": {"restarted_at": "2026-05-20T00:01:00+00:00"}
        },
    }

    assert rendering._queue_elapsed_text(item) == "00:00:00"

    running = {
        "status": "running",
        "updated_at": "2026-05-20T00:00:00+00:00",
        "metadata": {"last_restarted_at": "2026-05-20T00:00:10Z"},
    }

    assert rendering._queue_elapsed_text(
        running,
        now=datetime(2026, 5, 20, 0, 1, 15, tzinfo=timezone.utc),
    ) == "00:01:05"


def test_queue_table_lines_truncates_wide_unicode_without_column_drift(monkeypatch) -> None:
    monkeypatch.setattr(
        rendering,
        "_queue_table_now",
        lambda: datetime(2026, 5, 20, 0, 10, 0, tzinfo=timezone.utc),
    )
    rows = [
        (
            0,
            {
                "activity_id": "wf_한국어_very_long_identifier",
                "kind": "workflow",
                "status": "running",
                "submitted_at": "2026-05-20T00:00:00+00:00",
                "metadata": {
                    "template_name": "reaction_ts_search",
                    "workspace_dir": "/tmp/매우긴워크플로우이름_very_long_workflow_name",
                    "request_parameters": {"crest_mode": "nci"},
                },
            },
        ),
        (
            1,
            {
                "activity_id": "orca_1",
                "kind": "job",
                "engine": "orca",
                "status": "submitted",
                "updated_at": "2026-05-20T00:00:00+00:00",
                "metadata": {
                    "workflow_id": "wf_한국어_very_long_identifier",
                    "selected_inp_name": "긴파일이름_opt_ts_freq.inp",
                },
            },
        ),
    ]

    lines = rendering.queue_table_lines(rows)
    widths = [rendering._queue_display_width(line) for line in lines]

    assert len(set(widths)) == 1
    assert "..." in "\n".join(lines)
    assert "매우긴워크플로우" in "\n".join(lines)
