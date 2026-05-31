from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from chemstack.flow.submitters import orca as orca_submitter


def install_orca_workflow_io(
    monkeypatch: pytest.MonkeyPatch,
    *,
    payload: dict[str, Any],
    workspace_dir: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    saved_payloads: list[dict[str, Any]] = []
    sync_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        orca_submitter,
        "resolve_workflow_workspace",
        lambda target, workflow_root: workspace_dir,
    )
    monkeypatch.setattr(
        orca_submitter,
        "load_workflow_payload",
        lambda current_workspace_dir: payload,
    )

    def fake_write_workflow_payload(
        current_workspace_dir: Path, current_payload: dict[str, Any]
    ) -> None:
        saved_payloads.append(
            {
                "workspace_dir": current_workspace_dir,
                "payload": deepcopy(current_payload),
            }
        )

    def fake_sync_workflow_registry(
        workflow_root: Path,
        current_workspace_dir: Path,
        current_payload: dict[str, Any],
    ) -> None:
        sync_calls.append(
            {
                "workflow_root": workflow_root,
                "workspace_dir": current_workspace_dir,
                "payload": deepcopy(current_payload),
            }
        )

    monkeypatch.setattr(orca_submitter, "write_workflow_payload", fake_write_workflow_payload)
    monkeypatch.setattr(orca_submitter, "sync_workflow_registry", fake_sync_workflow_registry)
    return saved_payloads, sync_calls


def install_orca_timestamps(monkeypatch: pytest.MonkeyPatch, *timestamps: str) -> None:
    values = iter(timestamps)
    monkeypatch.setattr(orca_submitter, "now_utc_iso", lambda: next(values))
