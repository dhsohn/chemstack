from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.notifications import TelegramSendResult
from chemstack.flow import workflow_notifications


class _FakeTransport:
    def __init__(self) -> None:
        self.messages: list[str] = []
        self.parse_modes: list[str | None] = []

    def send_text(self, text: str, *, parse_mode: str | None = None) -> TelegramSendResult:
        self.messages.append(text)
        self.parse_modes.append(parse_mode)
        return TelegramSendResult(sent=True)


def _write_config(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "telegram:",
                "  bot_token: token",
                "  chat_id: chat",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_maybe_notify_workflow_phase_summary_sends_crest_summary_once(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "chemstack.yaml"
    _write_config(config_path)
    transport = _FakeTransport()
    monkeypatch.setattr(workflow_notifications, "build_telegram_transport", lambda _cfg: transport)
    payload: dict[str, Any] = {
        "workflow_id": "wf_crest_1",
        "template_name": "reaction_ts_search",
        "metadata": {},
        "stages": [
            {
                "stage_id": "crest_reactant",
                "status": "completed",
                "task": {
                    "engine": "crest",
                    "status": "completed",
                    "payload": {"input_role": "reactant"},
                },
                "metadata": {},
                "output_artifacts": [{"path": "a.xyz"}, {"path": "b.xyz"}],
            },
            {
                "stage_id": "crest_product",
                "status": "failed",
                "task": {
                    "engine": "crest",
                    "status": "failed",
                    "payload": {"input_role": "product"},
                },
                "metadata": {},
                "output_artifacts": [],
            },
        ],
    }

    assert workflow_notifications.maybe_notify_workflow_phase_summary(
        payload=payload,
        config_path=str(config_path),
        phase_engine="crest",
    )
    assert not workflow_notifications.maybe_notify_workflow_phase_summary(
        payload=payload,
        config_path=str(config_path),
        phase_engine="crest",
    )
    assert len(transport.messages) == 1
    assert transport.parse_modes == ["HTML"]
    message = transport.messages[0]
    assert "<b>chem_flow CREST phase summary</b>" in message
    assert "<b>Stages</b>: <code>2</code>" in message
    assert "<b>reactant</b>" in message
    assert "retained_conformers=<code>2</code>" in message
    assert "<b>product</b>" in message
    assert "retained_conformers=<code>0</code>" in message
    assert payload["metadata"]["phase_notifications"]["crest_summary"]["sent_at"]


def test_maybe_notify_workflow_phase_summary_sends_xtb_ready_counts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "chemstack.yaml"
    _write_config(config_path)
    transport = _FakeTransport()
    monkeypatch.setattr(workflow_notifications, "build_telegram_transport", lambda _cfg: transport)
    payload: dict[str, Any] = {
        "workflow_id": "wf_<xtb>_1",
        "template_name": "reaction_ts_search",
        "metadata": {},
        "stages": [
            {
                "stage_id": "xtb_path_search_01",
                "status": "completed",
                "task": {
                    "engine": "xtb",
                    "status": "completed",
                    "payload": {"reaction_key": "rxn_<01>"},
                },
                "metadata": {
                    "reaction_handoff_status": "ready",
                    "xtb_attempts": [{"attempt_number": 0, "candidate_count": 3}],
                },
                "output_artifacts": [],
            },
            {
                "stage_id": "xtb_path_search_02",
                "status": "failed",
                "task": {
                    "engine": "xtb",
                    "status": "failed",
                    "payload": {"reaction_key": "rxn_02"},
                },
                "metadata": {
                    "reaction_handoff_status": "failed",
                    "xtb_attempts": [{"attempt_number": 0, "candidate_count": 0}],
                },
                "output_artifacts": [],
            },
        ],
    }

    assert workflow_notifications.maybe_notify_workflow_phase_summary(
        payload=payload,
        config_path=str(config_path),
        phase_engine="xtb",
        extra_lines=["planned_orca_stages: 1"],
    )

    assert len(transport.messages) == 1
    assert transport.parse_modes == ["HTML"]
    message = transport.messages[0]
    assert "<b>chem_flow xTB phase summary</b>" in message
    assert "wf_&lt;xtb&gt;_1" in message
    assert "<b>Ready for ORCA</b>: <code>1</code>" in message
    assert "planned_orca_stages: <code>1</code>" in message
    assert "<b>rxn_&lt;01&gt;</b>" in message
    assert "handoff=<code>ready</code>" in message
    assert "candidates=<code>3</code>" in message
    assert "<b>rxn_02</b>" in message
    assert "handoff=<code>failed</code>" in message
    assert "candidates=<code>0</code>" in message
