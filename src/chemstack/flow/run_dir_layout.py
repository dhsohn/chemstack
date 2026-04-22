from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

WORKFLOW_MANIFEST_FILENAMES = ("flow.yaml",)
STANDARD_REACTION_REACTANT_FILENAME = "reactant.xyz"
STANDARD_REACTION_PRODUCT_FILENAME = "product.xyz"
STANDARD_CONFORMER_INPUT_FILENAME = "input.xyz"


@dataclass(frozen=True)
class WorkflowRunDirLayout:
    has_manifest: bool
    has_reaction_inputs: bool
    has_conformer_input: bool

    @property
    def is_ambiguous(self) -> bool:
        return self.has_reaction_inputs and self.has_conformer_input

    @property
    def inferred_workflow_type(self) -> str | None:
        if self.is_ambiguous:
            return None
        if self.has_reaction_inputs:
            return "reaction_ts_search"
        if self.has_conformer_input:
            return "conformer_screening"
        return None

    @property
    def is_workflow_dir(self) -> bool:
        return self.has_manifest or self.has_reaction_inputs or self.has_conformer_input


def inspect_workflow_run_dir(path: str | Path) -> WorkflowRunDirLayout:
    target = Path(path).expanduser().resolve()
    has_manifest = any((target / name).is_file() for name in WORKFLOW_MANIFEST_FILENAMES)
    has_reaction_inputs = (
        (target / STANDARD_REACTION_REACTANT_FILENAME).is_file()
        and (target / STANDARD_REACTION_PRODUCT_FILENAME).is_file()
    )
    has_conformer_input = (target / STANDARD_CONFORMER_INPUT_FILENAME).is_file()
    return WorkflowRunDirLayout(
        has_manifest=has_manifest,
        has_reaction_inputs=has_reaction_inputs,
        has_conformer_input=has_conformer_input,
    )


__all__ = [
    "STANDARD_CONFORMER_INPUT_FILENAME",
    "STANDARD_REACTION_PRODUCT_FILENAME",
    "STANDARD_REACTION_REACTANT_FILENAME",
    "WORKFLOW_MANIFEST_FILENAMES",
    "WorkflowRunDirLayout",
    "inspect_workflow_run_dir",
]
