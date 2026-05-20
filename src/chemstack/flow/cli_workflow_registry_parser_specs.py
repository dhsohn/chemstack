from __future__ import annotations

from .cli_parser_specs import ArgumentSpec, WorkflowParserSpec


def workflow_registry_specs() -> tuple[WorkflowParserSpec, ...]:
    target_help = "workflow_id, workflow workspace directory, or workflow.json path"
    return (
        WorkflowParserSpec(
            name="list",
            help="List materialized workflows under a workflow root.",
            func_name="cmd_workflow_list",
            workflow_root=True,
            workflow_root_required=True,
            arguments=(
                ArgumentSpec(
                    ("--limit",),
                    {
                        "type": int,
                        "default": 0,
                        "help": "Optional maximum number of workflows to print",
                    },
                ),
                ArgumentSpec(
                    ("--refresh",),
                    {
                        "action": "store_true",
                        "help": "Rebuild the registry from workflow workspaces before listing",
                    },
                ),
            ),
        ),
        WorkflowParserSpec(
            name="get",
            help="Inspect one materialized workflow.",
            func_name="cmd_workflow_get",
            target_help=target_help,
            workflow_root=True,
        ),
        WorkflowParserSpec(
            name="artifacts",
            help="List known materialized artifacts for one workflow.",
            func_name="cmd_workflow_artifacts",
            target_help=target_help,
            workflow_root=True,
        ),
        WorkflowParserSpec(
            name="cancel",
            help=(
                "Cancel a materialized workflow and request queue cancellation for submitted "
                "engine stages."
            ),
            func_name="cmd_workflow_cancel",
            target_help=target_help,
            workflow_root=True,
            chemstack_config=True,
            chemstack_config_help=(
                "Path to shared chemstack.yaml; required if submitted stages exist"
            ),
        ),
        WorkflowParserSpec(
            name="reindex",
            help="Rebuild the workflow registry from workflow workspaces.",
            func_name="cmd_workflow_reindex",
            workflow_root=True,
            workflow_root_required=True,
        ),
        WorkflowParserSpec(
            name="runtime-status",
            help="Show the current worker heartbeat/state for a workflow root.",
            func_name="cmd_workflow_runtime_status",
            workflow_root=True,
            workflow_root_required=True,
        ),
        WorkflowParserSpec(
            name="journal",
            help="Show recent append-only orchestration journal events.",
            func_name="cmd_workflow_journal",
            workflow_root=True,
            workflow_root_required=True,
            arguments=(
                ArgumentSpec(
                    ("--limit",),
                    {
                        "type": int,
                        "default": 50,
                        "help": "Maximum number of recent events to show",
                    },
                ),
            ),
        ),
        WorkflowParserSpec(
            name="telemetry",
            help="Summarize registry status, worker heartbeat, and recent journal activity.",
            func_name="cmd_workflow_telemetry",
            workflow_root=True,
            workflow_root_required=True,
            arguments=(
                ArgumentSpec(
                    ("--limit",),
                    {
                        "type": int,
                        "default": 200,
                        "help": "Maximum number of recent journal events to summarize",
                    },
                ),
            ),
        ),
    )
