from __future__ import annotations

from importlib import import_module
from types import ModuleType
from typing import Any

_EXPORTS: dict[str, tuple[str, str]] = {
    "load_crest_artifact_contract": (".crest", "load_crest_artifact_contract"),
    "load_orca_artifact_contract": (".orca", "load_orca_artifact_contract"),
    "load_xtb_artifact_contract": (".xtb", "load_xtb_artifact_contract"),
    "select_crest_downstream_inputs": (".crest", "select_crest_downstream_inputs"),
    "select_xtb_downstream_inputs": (".xtb", "select_xtb_downstream_inputs"),
}

_SUBMODULES: dict[str, str] = {
    "_engine_adapter_helpers": "._engine_adapter_helpers",
    "_orca_contract_assembly": "._orca_contract_assembly",
    "_orca_contract_context": "._orca_contract_context",
    "_orca_contract_status": "._orca_contract_status",
    "_orca_local_lookup": "._orca_local_lookup",
    "_orca_path_helpers": "._orca_path_helpers",
    "_orca_tracking": "._orca_tracking",
    "crest": ".crest",
    "orca": ".orca",
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
    "load_crest_artifact_contract",
    "load_orca_artifact_contract",
    "load_xtb_artifact_contract",
    "select_crest_downstream_inputs",
    "select_xtb_downstream_inputs",
]
