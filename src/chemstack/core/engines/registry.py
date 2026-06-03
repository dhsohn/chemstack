from __future__ import annotations

from importlib import import_module
from typing import Final

from .definitions import EngineDefinition

_ENGINE_MODULES: Final[dict[str, str]] = {
    "orca": "chemstack.orca.engine",
    "xtb": "chemstack.xtb.engine",
    "crest": "chemstack.crest.engine",
}


def known_engine_ids() -> tuple[str, ...]:
    return tuple(_ENGINE_MODULES)


def get_engine_definition(engine: str) -> EngineDefinition:
    engine_id = str(engine or "").strip().lower()
    module_name = _ENGINE_MODULES.get(engine_id)
    if module_name is None:
        supported = ", ".join(known_engine_ids())
        raise ValueError(f"unsupported engine: {engine_id or '<blank>'} (supported: {supported})")
    module = import_module(module_name)
    definition = module.ENGINE_DEFINITION
    if not isinstance(definition, EngineDefinition):
        raise TypeError(f"{module_name}.ENGINE_DEFINITION is not an EngineDefinition")
    return definition


__all__ = ["get_engine_definition", "known_engine_ids"]
