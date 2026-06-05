from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from orca_auto.flow.engines.crest import worker_context


def test_molecule_key_prefers_metadata_and_falls_back_to_selected_xyz(tmp_path: Path) -> None:
    job_dir = tmp_path / "job"
    selected_xyz = job_dir / "Selected Input.xyz"

    assert (
        worker_context.molecule_key(
            SimpleNamespace(metadata={"molecule_key": " fixed-key "}),
            selected_xyz,
            job_dir,
        )
        == "fixed-key"
    )
    assert (
        worker_context.molecule_key(
            SimpleNamespace(metadata={"molecule_key": " "}),
            selected_xyz,
            job_dir,
        )
        == "selected_input"
    )


def test_build_execution_context_resolves_entry_metadata(tmp_path: Path) -> None:
    job_dir = tmp_path / "job-1"
    selected_xyz = job_dir / "input.xyz"
    entry = SimpleNamespace(
        metadata={
            "job_dir": str(job_dir),
            "selected_input_xyz": str(selected_xyz),
            "mode": "nci",
        }
    )
    cfg = SimpleNamespace(
        resources=SimpleNamespace(max_cores_per_task=4, max_memory_gb_per_task=8)
    )

    context = worker_context.build_execution_context(
        cfg,
        entry,
        molecule_key_resolver=lambda actual_entry, actual_selected, actual_job_dir: (
            f"{actual_entry is entry}:{actual_selected == selected_xyz.resolve()}:"
            f"{actual_job_dir == job_dir.resolve()}"
        ),
    )

    assert context == worker_context.ExecutionContext(
        entry=entry,
        job_dir=job_dir.resolve(),
        selected_xyz=selected_xyz.resolve(),
        molecule_key="True:True:True",
        mode="nci",
        resource_request={"max_cores": 4, "max_memory_gb": 8},
    )
