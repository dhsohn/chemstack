from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ArgumentSpec:
    flags: tuple[str, ...]
    kwargs: dict[str, Any]


@dataclass(frozen=True)
class WorkflowParserSpec:
    name: str
    help: str
    func_name: str
    target_help: str = ""
    workflow_root: bool = False
    workflow_root_required: bool = False
    chemstack_config: bool = False
    chemstack_config_required: bool = False
    chemstack_config_help: str = "Path to shared chemstack.yaml"
    json: bool = True
    arguments: tuple[ArgumentSpec, ...] = ()


def add_argument_specs(
    parser: argparse.ArgumentParser,
    specs: tuple[ArgumentSpec, ...],
) -> None:
    for spec in specs:
        parser.add_argument(*spec.flags, **spec.kwargs)
