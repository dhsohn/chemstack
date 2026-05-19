from importlib import import_module
from types import ModuleType

from .contracts import (
    CrestArtifactContract,
    CrestDownstreamPolicy,
    WorkflowArtifactRef,
    WorkflowArtifactRefPayload,
    WorkflowPlan,
    WorkflowPlanPayload,
    WorkflowStage,
    WorkflowStagePayload,
    WorkflowStageInput,
    WorkflowStageWithTaskPayload,
    WorkflowTask,
    WorkflowTaskPayload,
    WorkflowTemplateRequest,
    WorkflowTemplateRequestPayload,
    XtbArtifactContract,
    XtbCandidateArtifact,
    XtbDownstreamPolicy,
)

_LAZY_MODULES = frozenset({"cli", "operations", "registry", "runtime", "state", "xyz_utils"})

__all__ = [
    "cli",
    "operations",
    "registry",
    "runtime",
    "state",
    "xyz_utils",
    "CrestArtifactContract",
    "CrestDownstreamPolicy",
    "WorkflowArtifactRef",
    "WorkflowArtifactRefPayload",
    "WorkflowPlan",
    "WorkflowPlanPayload",
    "WorkflowStage",
    "WorkflowStagePayload",
    "WorkflowStageInput",
    "WorkflowStageWithTaskPayload",
    "WorkflowTask",
    "WorkflowTaskPayload",
    "WorkflowTemplateRequest",
    "WorkflowTemplateRequestPayload",
    "XtbArtifactContract",
    "XtbCandidateArtifact",
    "XtbDownstreamPolicy",
]

__version__ = "0.1.0"


def __getattr__(name: str) -> ModuleType:
    if name in _LAZY_MODULES:
        module = import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
