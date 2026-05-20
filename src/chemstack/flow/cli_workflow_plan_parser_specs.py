from __future__ import annotations

from .cli_parser_specs import ArgumentSpec, WorkflowParserSpec
from .cli_workflow_parser_spec_helpers import orca_materialization_argument_specs


def workflow_planning_specs() -> tuple[WorkflowParserSpec, ...]:
    return (
        WorkflowParserSpec(
            name="reaction-ts-search",
            help="Build a reaction_ts_search workflow plan from xTB results.",
            func_name="cmd_workflow_reaction_ts_search",
            target_help="xTB job_id or job directory",
            arguments=(
                ArgumentSpec(
                    ("--xtb-index-root",),
                    {"required": True, "help": "xTB index root, usually allowed_root"},
                ),
                ArgumentSpec(
                    ("--max-orca-stages",),
                    {
                        "type": int,
                        "default": 3,
                        "help": "Maximum number of ORCA stage payloads to emit",
                    },
                ),
                ArgumentSpec(
                    ("--include-unselected",),
                    {
                        "action": "store_true",
                        "help": "Consider non-selected xTB candidate_details when planning",
                    },
                ),
                ArgumentSpec(
                    ("--workspace-root",),
                    {
                        "help": (
                            "If provided, materialize a workflow workspace with ORCA "
                            "reaction directories and workflow.json"
                        )
                    },
                ),
                *orca_materialization_argument_specs("! r2scan-3c OptTS Freq TightSCF"),
                ArgumentSpec(
                    ("--priority",),
                    {"type": int, "default": 10, "help": "Planned queue priority"},
                ),
            ),
        ),
        WorkflowParserSpec(
            name="conformer-screening",
            help=(
                "Build a conformer_screening workflow plan from CREST results "
                "(`standard` or `nci`)."
            ),
            func_name="cmd_workflow_conformer_screening",
            target_help="CREST job_id or job directory",
            arguments=(
                ArgumentSpec(
                    ("--crest-index-root",),
                    {"required": True, "help": "CREST index root, usually allowed_root"},
                ),
                ArgumentSpec(
                    ("--max-orca-stages",),
                    {
                        "type": int,
                        "default": 3,
                        "help": "Maximum number of ORCA stage payloads to emit",
                    },
                ),
                ArgumentSpec(
                    ("--workspace-root",),
                    {"help": "If provided, materialize a workflow workspace"},
                ),
                *orca_materialization_argument_specs("! r2scan-3c Opt TightSCF"),
                ArgumentSpec(
                    ("--priority",),
                    {"type": int, "default": 10, "help": "Planned queue priority"},
                ),
            ),
        ),
    )


def workflow_creation_specs() -> tuple[WorkflowParserSpec, ...]:
    return (
        WorkflowParserSpec(
            name="create-reaction-ts-search",
            help=(
                "Create a raw-input reaction_ts_search workflow from reactant/product "
                "precomplex XYZ inputs."
            ),
            func_name="cmd_workflow_create_reaction_ts_search",
            workflow_root=True,
            workflow_root_required=True,
            arguments=(
                ArgumentSpec(
                    ("--reactant-xyz",),
                    {
                        "dest": "reactant_xyz",
                        "required": True,
                        "help": "Reactant-side precomplex XYZ input",
                    },
                ),
                ArgumentSpec(
                    ("--product-xyz",),
                    {"dest": "product_xyz", "required": True, "help": "Product-side XYZ input"},
                ),
                ArgumentSpec(
                    ("--crest-mode",),
                    {
                        "default": "standard",
                        "help": "CREST mode for initial stages (`standard` or `nci`)",
                    },
                ),
                ArgumentSpec(("--priority",), {"type": int, "default": 10}),
                ArgumentSpec(("--max-crest-candidates",), {"type": int, "default": 3}),
                ArgumentSpec(("--max-xtb-stages",), {"type": int, "default": 3}),
                ArgumentSpec(("--max-orca-stages",), {"type": int, "default": 3}),
                *orca_materialization_argument_specs("! r2scan-3c OptTS Freq TightSCF"),
            ),
        ),
        WorkflowParserSpec(
            name="create-conformer-screening",
            help=(
                "Create a raw-input conformer_screening workflow that can be advanced "
                "through CREST and ORCA (`standard` or `nci`)."
            ),
            func_name="cmd_workflow_create_conformer_screening",
            workflow_root=True,
            workflow_root_required=True,
            arguments=(
                ArgumentSpec(
                    ("--input-xyz",),
                    {"required": True, "help": "Input XYZ for the molecule to screen"},
                ),
                ArgumentSpec(
                    ("--crest-mode",),
                    {"default": "standard", "help": "CREST mode for the initial stage"},
                ),
                ArgumentSpec(("--priority",), {"type": int, "default": 10}),
                ArgumentSpec(("--max-orca-stages",), {"type": int, "default": 3}),
                *orca_materialization_argument_specs("! r2scan-3c Opt TightSCF"),
            ),
        ),
    )
