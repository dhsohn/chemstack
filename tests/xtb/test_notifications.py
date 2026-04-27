from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from chemstack.core.config import CommonRuntimeConfig, TelegramConfig
from chemstack.core.notifications import TelegramSendResult

from chemstack.xtb.config import AppConfig
from chemstack.xtb import notifications


class _FakeTransport:
    def __init__(self, result: TelegramSendResult) -> None:
        self.result = result
        self.messages: list[str] = []

    def send_text(self, text: str) -> TelegramSendResult:
        self.messages.append(text)
        return self.result


def _make_cfg(tmp_path: Path, *, enabled: bool = False) -> AppConfig:
    allowed_root = tmp_path / "allowed"
    organized_root = tmp_path / "organized"
    allowed_root.mkdir()
    organized_root.mkdir()
    telegram = TelegramConfig(
        bot_token="token" if enabled else "",
        chat_id="chat" if enabled else "",
    )
    return AppConfig(
        runtime=CommonRuntimeConfig(
            allowed_root=str(allowed_root),
            organized_root=str(organized_root),
        ),
        telegram=telegram,
    )


def test_send_returns_true_when_real_transport_skips_disabled_telegram(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path, enabled=False)

    assert notifications._send(cfg, ["line 1", "line 2"])


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        (TelegramSendResult(sent=True), True),
        (TelegramSendResult(sent=False, skipped=True), True),
        (TelegramSendResult(sent=False, skipped=False, error="boom"), False),
    ],
)
def test_send_joins_lines_and_maps_transport_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    result: TelegramSendResult,
    expected: bool,
) -> None:
    cfg = _make_cfg(tmp_path, enabled=True)
    transport = _FakeTransport(result)
    monkeypatch.setattr(notifications, "build_telegram_transport", lambda _cfg: transport)

    sent = notifications._send(cfg, ["line 1", "line 2"])

    assert sent is expected
    assert transport.messages == ["line 1\nline 2"]


def test_notify_job_queued_and_started_render_expected_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path, enabled=True)
    transport = _FakeTransport(TelegramSendResult(sent=True))
    monkeypatch.setattr(notifications, "build_telegram_transport", lambda _cfg: transport)
    job_dir = tmp_path / "job-001"
    selected_xyz = tmp_path / "inputs" / "reactant.xyz"

    assert notifications.notify_job_queued(
        cfg,
        job_id="job-001",
        queue_id="queue-001",
        job_dir=job_dir,
        job_type="ranking",
        reaction_key="rxn-1",
        selected_xyz=selected_xyz,
    )
    assert notifications.notify_job_started(
        cfg,
        job_id="job-001",
        queue_id="queue-001",
        job_dir=job_dir,
        job_type="ranking",
        reaction_key="rxn-1",
        selected_xyz=selected_xyz,
    )

    assert transport.messages == [
        "\n".join(
            [
                "[xtb_auto] Job queued",
                "job_id: job-001",
                "queue_id: queue-001",
                "job_type: ranking",
                "reaction_key: rxn-1",
                "job_dir: job-001",
                "selected_input_xyz: reactant.xyz",
            ]
        ),
        "\n".join(
            [
                "[xtb_auto] Job started",
                "job_id: job-001",
                "queue_id: queue-001",
                "job_type: ranking",
                "reaction_key: rxn-1",
                "job_dir: job-001",
                "selected_input_xyz: reactant.xyz",
            ]
        ),
    ]


def test_notify_job_terminal_includes_extra_lines(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path, enabled=True)
    transport = _FakeTransport(TelegramSendResult(sent=True))
    monkeypatch.setattr(notifications, "build_telegram_transport", lambda _cfg: transport)

    assert notifications.notify_job_terminal(
        cfg,
        headline="Job failed",
        job_id="job-002",
        queue_id="queue-002",
        status="failed",
        reason="xtb_error",
        job_type="sp",
        reaction_key="rxn-2",
        job_dir=tmp_path / "job-002",
        selected_xyz=tmp_path / "inputs" / "candidate.xyz",
        candidate_count=3,
        extra_lines=["organized_output_dir: /tmp/out", "resource_actual: {'max_cores': 4}"],
    )

    assert transport.messages == [
        "\n".join(
            [
                "[xtb_auto] Job failed",
                "job_id: job-002",
                "queue_id: queue-002",
                "status: failed",
                "reason: xtb_error",
                "job_type: sp",
                "reaction_key: rxn-2",
                "job_dir: job-002",
                "selected_input_xyz: candidate.xyz",
                "candidate_count: 3",
                "organized_output_dir: /tmp/out",
                "resource_actual: {'max_cores': 4}",
            ]
        )
    ]


@pytest.mark.parametrize(
    ("status", "headline"),
    [
        ("completed", "Job finished"),
        ("failed", "Job failed"),
        ("cancelled", "Job cancelled"),
        ("running", "Job finished"),
    ],
)
def test_notify_job_finished_maps_headlines_and_optional_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    headline: str,
) -> None:
    cfg = _make_cfg(tmp_path, enabled=True)
    transport = _FakeTransport(TelegramSendResult(sent=True))
    monkeypatch.setattr(notifications, "build_telegram_transport", lambda _cfg: transport)
    organized_output_dir: Path | None = None
    resource_request: dict[str, int] | None = None
    resource_actual: dict[str, int] | None = None
    if status == "completed":
        organized_output_dir = tmp_path / "organized" / "job-003"
        resource_request = {"max_cores": 8, "max_memory_gb": 16}
        resource_actual = {"max_cores": 6, "max_memory_gb": 12}

    assert notifications.notify_job_finished(
        cfg,
        job_id="job-003",
        queue_id="queue-003",
        status=status,
        reason="done",
        job_type="opt",
        reaction_key="rxn-3",
        job_dir=tmp_path / "job-003",
        selected_xyz=tmp_path / "inputs" / "optimized.xyz",
        candidate_count=1,
        organized_output_dir=organized_output_dir,
        resource_request=cast(dict[str, int] | None, resource_request),
        resource_actual=cast(dict[str, int] | None, resource_actual),
    )

    message = transport.messages[-1]
    assert message.startswith(f"[xtb_auto] {headline}\n")
    assert "job_id: job-003" in message
    assert f"status: {status}" in message
    assert "job_dir: job-003" in message
    assert "selected_input_xyz: optimized.xyz" in message
    assert "candidate_count: 1" in message
    if status == "completed":
        assert "organized_output_dir: " in message
        assert "resource_request: {'max_cores': 8, 'max_memory_gb': 16}" in message
        assert "resource_actual: {'max_cores': 6, 'max_memory_gb': 12}" in message
    else:
        assert "organized_output_dir: " not in message
        assert "resource_request: " not in message
        assert "resource_actual: " not in message


def test_notify_organize_summary_formats_counts_and_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path, enabled=True)
    transport = _FakeTransport(TelegramSendResult(sent=True))
    monkeypatch.setattr(notifications, "build_telegram_transport", lambda _cfg: transport)
    root = tmp_path / "organized-root"

    assert notifications.notify_organize_summary(
        cfg,
        organized_count=4,
        skipped_count=2,
        root=root,
    )

    assert transport.messages == [
        "\n".join(
            [
                "[xtb_auto] Organize summary",
                f"root: {root}",
                "organized: 4",
                "skipped: 2",
            ]
        )
    ]


def test_workflow_child_notifications_are_suppressed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path, enabled=True)
    transport = _FakeTransport(TelegramSendResult(sent=True))
    monkeypatch.setattr(notifications, "build_telegram_transport", lambda _cfg: transport)
    workflow_job_dirs = [
        tmp_path / "workflow_jobs" / "wf-1" / "stage_02_xtb" / "job-004",
        tmp_path / "wf-1" / "internal" / "xtb" / "runs" / "xtb_path_search_01",
    ]

    for workflow_job_dir in workflow_job_dirs:
        assert notifications.notify_job_queued(
            cfg,
            job_id="job-004",
            queue_id="queue-004",
            job_dir=workflow_job_dir,
            job_type="path_search",
            reaction_key="rxn-4",
            selected_xyz=workflow_job_dir / "ts.xyz",
        )
        assert notifications.notify_job_finished(
            cfg,
            job_id="job-004",
            queue_id="queue-004",
            status="completed",
            reason="done",
            job_type="path_search",
            reaction_key="rxn-4",
            job_dir=workflow_job_dir,
            selected_xyz=workflow_job_dir / "ts.xyz",
            candidate_count=2,
        )
    assert transport.messages == []
