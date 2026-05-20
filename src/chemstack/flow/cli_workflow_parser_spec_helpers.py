from __future__ import annotations

from .cli_parser_specs import ArgumentSpec


def orca_materialization_argument_specs(route_default: str) -> tuple[ArgumentSpec, ...]:
    return (
        ArgumentSpec(
            ("--charge",),
            {"type": int, "default": 0, "help": "Charge for materialized ORCA inputs"},
        ),
        ArgumentSpec(
            ("--multiplicity",),
            {"type": int, "default": 1, "help": "Multiplicity for materialized ORCA inputs"},
        ),
        ArgumentSpec(
            ("--max-cores",),
            {"type": int, "default": 8, "help": "Maximum cores per planned ORCA task"},
        ),
        ArgumentSpec(
            ("--max-memory-gb",),
            {"type": int, "default": 32, "help": "Maximum memory GiB per planned ORCA task"},
        ),
        ArgumentSpec(
            ("--orca-route-line",),
            {"default": route_default, "help": "Route line for materialized ORCA inputs"},
        ),
    )
