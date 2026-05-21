from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RunDirManifestSections:
    resources: dict[str, Any]
    crest: dict[str, Any]
    xtb: dict[str, Any]
    endpoint_pairing: dict[str, Any]
    orca: dict[str, Any]


@dataclass(frozen=True)
class RunDirWorkflowOptions:
    workflow_root: str
    crest_mode: str
    priority: int
    max_cores: int
    max_memory_gb: int
    max_orca_stages: int
    orca_route_line: str
    charge: int
    multiplicity: int
    max_crest_candidates: int
    max_xtb_stages: int


@dataclass(frozen=True)
class RunDirWorkflowConfig:
    workflow_dir: Path
    manifest: dict[str, Any]
    sections: RunDirManifestSections
    reactant_xyz: str
    product_xyz: str
    input_xyz: str
    workflow_type: str

    @property
    def resources_manifest(self) -> dict[str, Any]:
        return self.sections.resources

    @property
    def crest_manifest(self) -> dict[str, Any]:
        return self.sections.crest

    @property
    def xtb_manifest(self) -> dict[str, Any]:
        return self.sections.xtb

    @property
    def endpoint_pairing(self) -> dict[str, Any]:
        return self.sections.endpoint_pairing

    @property
    def orca_manifest(self) -> dict[str, Any]:
        return self.sections.orca


__all__ = [
    "RunDirManifestSections",
    "RunDirWorkflowConfig",
    "RunDirWorkflowOptions",
]
