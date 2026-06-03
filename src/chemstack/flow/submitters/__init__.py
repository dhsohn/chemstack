from __future__ import annotations

from importlib import import_module
from types import ModuleType
from typing import Any

_EXPORTS: dict[str, tuple[str, str]] = {
    "cancel_crest_target": (".crest", "cancel_target"),
    "cancel_reaction_ts_search_workflow": (".orca", "cancel_reaction_ts_search_workflow"),
    "cancel_xtb_target": (".xtb", "cancel_target"),
    "submit_crest_job_dir": (".crest", "submit_job_dir"),
    "submit_reaction_ts_search_workflow": (".orca", "submit_reaction_ts_search_workflow"),
    "submit_xtb_job_dir": (".xtb", "submit_job_dir"),
}

_SUBMODULES: dict[str, str] = {
    "crest": ".crest",
    "internal_engine": ".internal_engine",
    "internal_engine_builder": ".internal_engine_builder",
    "internal_engine_cancellation": ".internal_engine_cancellation",
    "internal_engine_models": ".internal_engine_models",
    "internal_engine_submission": ".internal_engine_submission",
    "orca": ".orca",
    "orca_cancellation": ".orca_cancellation",
    "orca_models": ".orca_models",
    "orca_submission": ".orca_submission",
    "xtb": ".xtb",
}


def _load_export(name: str) -> Any:
    module_name, attr_name = _EXPORTS[name]
    value = getattr(import_module(module_name, __name__), attr_name)
    globals()[name] = value
    return value


def _load_submodule(name: str) -> ModuleType:
    module = import_module(_SUBMODULES[name], __name__)
    globals()[name] = module
    return module


def __getattr__(name: str) -> Any:
    if name in _EXPORTS:
        return _load_export(name)
    if name in _SUBMODULES:
        return _load_submodule(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS) | set(_SUBMODULES))


__all__ = [
    "cancel_crest_target",
    "cancel_reaction_ts_search_workflow",
    "cancel_xtb_target",
    "submit_crest_job_dir",
    "submit_reaction_ts_search_workflow",
    "submit_xtb_job_dir",
]
