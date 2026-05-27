from __future__ import annotations

from pathlib import Path

from chemstack.core.config.files import (
    engine_config_mapping,
    shared_workflow_root_from_config,
    workflow_root_from_mapping,
)


def test_workflow_root_from_mapping_accepts_only_canonical_root_key(tmp_path: Path) -> None:
    workflow_root = tmp_path / "workflow-root"

    assert workflow_root_from_mapping({"workflow": {"root": str(workflow_root)}}) == str(
        workflow_root.resolve()
    )
    assert workflow_root_from_mapping({"workflow": {"workflow_root": str(workflow_root)}}) == ""


def test_shared_workflow_root_from_config_ignores_removed_workflow_root_alias(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "chemstack.yaml"
    config_path.write_text(
        f"workflow:\n  workflow_root: {tmp_path / 'legacy-workflows'}\n",
        encoding="utf-8",
    )

    assert shared_workflow_root_from_config(config_path) is None


def test_engine_config_mapping_requires_engine_section() -> None:
    raw = {
        "runtime": {"allowed_root": "/tmp/legacy"},
        "paths": {"orca_executable": "/tmp/orca"},
        "scheduler": {"max_active_simulations": 4},
    }

    assert engine_config_mapping(raw, "orca", inherit_keys=("scheduler",)) == {}
