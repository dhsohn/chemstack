from __future__ import annotations

from .cli_parser_specs import WorkflowParserSpec, argument_spec, int_spec, store_true_spec


def workflow_runtime_specs() -> tuple[WorkflowParserSpec, ...]:
    target_help = "workflow_id, workflow workspace directory, or workflow.json path"
    return (
        WorkflowParserSpec(
            name="advance",
            help=(
                "Advance a materialized workflow by syncing/submitting actionable CREST, "
                "xTB, and ORCA stages."
            ),
            func_name="cmd_workflow_advance",
            target_help=target_help,
            workflow_root=True,
            workflow_root_required=True,
            chemstack_config=True,
            arguments=(
                store_true_spec(
                    "--no-submit",
                    help="Only sync and append stages; do not submit newly actionable stages",
                ),
            ),
        ),
        WorkflowParserSpec(
            name="worker",
            help="Continuously advance non-terminal workflows from the registry.",
            func_name="cmd_workflow_worker",
            workflow_root=True,
            chemstack_config=True,
            arguments=(
                store_true_spec(
                    "--no-submit",
                    help="Only sync/append stages; do not submit newly actionable stages",
                ),
                store_true_spec(
                    "--once",
                    help="Run exactly one orchestration cycle",
                ),
                int_spec(
                    "--max-cycles",
                    default=0,
                    help="Optional cycle limit; 0 means run forever",
                ),
                argument_spec(
                    "--interval-seconds",
                    type=float,
                    default=30.0,
                    help="Sleep interval between orchestration cycles",
                ),
                argument_spec(
                    "--lock-timeout-seconds",
                    type=float,
                    default=5.0,
                    help="How long to wait for the worker lock",
                ),
                store_true_spec(
                    "--refresh-registry",
                    help="Reindex the workflow registry before the first cycle",
                ),
                store_true_spec(
                    "--refresh-each-cycle",
                    help="Reindex the workflow registry before every cycle",
                ),
            ),
        ),
        WorkflowParserSpec(
            name="submit-reaction-ts-search",
            help="Submit a materialized reaction_ts_search workflow into chemstack ORCA.",
            func_name="cmd_workflow_submit_reaction_ts_search",
            target_help=target_help,
            workflow_root=True,
            chemstack_config=True,
            chemstack_config_required=True,
            arguments=(
                store_true_spec(
                    "--resubmit",
                    help="Retry stages already marked as submitted",
                ),
            ),
        ),
    )
