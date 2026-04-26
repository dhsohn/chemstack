from __future__ import annotations

from pathlib import Path

from chemstack.core.notifications import TelegramSendResult
from chemstack.flow import workflow_notifications


class _FakeTransport:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def send_text(self, text: str) -> TelegramSendResult:
        self.messages.append(text)
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
    payload = {
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
    message = transport.messages[0]
    assert "[chem_flow] CREST phase summary" in message
    assert "completed: 1" in message
    assert "failed: 1" in message
    assert "- reactant: status=completed retained_conformers=2" in message
    assert "- product: status=failed retained_conformers=0" in message
    assert payload["metadata"]["phase_notifications"]["crest_summary"]["sent_at"]


def test_maybe_notify_workflow_phase_summary_sends_xtb_ready_counts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "chemstack.yaml"
    _write_config(config_path)
    transport = _FakeTransport()
    monkeypatch.setattr(workflow_notifications, "build_telegram_transport", lambda _cfg: transport)
    payload = {
        "workflow_id": "wf_xtb_1",
        "template_name": "reaction_ts_search",
        "metadata": {},
        "stages": [
            {
                "stage_id": "xtb_path_search_01",
                "status": "completed",
                "task": {
                    "engine": "xtb",
                    "status": "completed",
                    "payload": {"reaction_key": "rxn_01"},
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
    message = transport.messages[0]
    assert "[chem_flow] XTB phase summary" in message
    assert "ready_for_orca: 1" in message
    assert "planned_orca_stages: 1" in message
    assert "- rxn_01: status=completed handoff=ready candidates=3" in message
    assert "- rxn_02: status=failed handoff=failed candidates=0" in message
