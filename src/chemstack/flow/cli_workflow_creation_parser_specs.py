from __future__ import annotations

from .cli_parser_specs import WorkflowParserSpec, argument_spec, int_spec
from .cli_workflow_parser_spec_helpers import orca_materialization_argument_specs


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
                argument_spec(
                    "--reactant-xyz",
                    dest="reactant_xyz",
                    required=True,
                    help="Reactant-side precomplex XYZ input",
                ),
                argument_spec(
                    "--product-xyz",
                    dest="product_xyz",
                    required=True,
                    help="Product-side XYZ input",
                ),
                argument_spec(
                    "--crest-mode",
                    default="standard",
                    help="CREST mode for initial stages (`standard` or `nci`)",
                ),
                int_spec("--priority", default=10),
                int_spec("--max-crest-candidates", default=3),
                int_spec("--max-xtb-stages", default=3),
                int_spec("--max-orca-stages", default=3),
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
                argument_spec(
                    "--input-xyz",
                    required=True,
                    help="Input XYZ for the molecule to screen",
                ),
                argument_spec(
                    "--crest-mode",
                    default="standard",
                    help="CREST mode for the initial stage",
                ),
                int_spec("--priority", default=10),
                int_spec("--max-orca-stages", default=3),
                *orca_materialization_argument_specs("! r2scan-3c Opt TightSCF"),
            ),
        ),
    )


__all__ = ["workflow_creation_specs"]
