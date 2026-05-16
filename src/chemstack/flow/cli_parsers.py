from __future__ import annotations

import argparse
from typing import Any


def _commands() -> Any:
    from chemstack.flow import cli as commands

    return commands


def _add_json_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true", help="Print JSON output")


def _add_workflow_root_argument(parser: argparse.ArgumentParser, *, required: bool = False) -> None:
    parser.add_argument(
        "--workflow-root",
        required=required,
        help="Root that directly contains workflow workspaces.",
    )


def _add_chemstack_config_argument(
    parser: argparse.ArgumentParser,
    *,
    required: bool = False,
    help_text: str = "Path to shared chemstack.yaml",
) -> None:
    parser.add_argument("--chemstack-config", required=required, help=help_text)


def _add_orca_materialization_arguments(
    parser: argparse.ArgumentParser,
    *,
    route_default: str,
) -> None:
    parser.add_argument("--charge", type=int, default=0, help="Charge for materialized ORCA inputs")
    parser.add_argument(
        "--multiplicity", type=int, default=1, help="Multiplicity for materialized ORCA inputs"
    )
    parser.add_argument(
        "--max-cores", type=int, default=8, help="Maximum cores per planned ORCA task"
    )
    parser.add_argument(
        "--max-memory-gb", type=int, default=32, help="Maximum memory GiB per planned ORCA task"
    )
    parser.add_argument(
        "--orca-route-line",
        default=route_default,
        help="Route line for materialized ORCA inputs",
    )


def _register_run_dir_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    commands = _commands()
    run_dir_parser = subparsers.add_parser(
        "run-dir",
        help="Create a workflow from an input directory containing reactant/product or input XYZ files.",
    )
    run_dir_parser.add_argument(
        "workflow_dir", help="Directory that contains workflow input XYZ files"
    )
    run_dir_parser.add_argument(
        "--workflow-type",
        help="Optional workflow type override: reaction_ts_search or conformer_screening",
    )
    _add_workflow_root_argument(run_dir_parser)
    run_dir_parser.add_argument("--reactant-xyz", help="Optional reactant XYZ override")
    run_dir_parser.add_argument("--product-xyz", help="Optional product XYZ override")
    run_dir_parser.add_argument("--input-xyz", help="Optional conformer input XYZ override")
    run_dir_parser.add_argument("--crest-mode", help="CREST mode (`standard` or `nci`)")
    run_dir_parser.add_argument("--priority", type=int, default=None)
    run_dir_parser.add_argument("--max-cores", type=int, default=None)
    run_dir_parser.add_argument("--max-memory-gb", type=int, default=None)
    run_dir_parser.add_argument("--max-crest-candidates", type=int, default=None)
    run_dir_parser.add_argument("--max-xtb-stages", type=int, default=None)
    run_dir_parser.add_argument("--max-orca-stages", type=int, default=None)
    run_dir_parser.add_argument("--orca-route-line")
    run_dir_parser.add_argument("--charge", type=int, default=None)
    run_dir_parser.add_argument("--multiplicity", type=int, default=None)
    run_dir_parser.add_argument(
        "--force",
        action="store_true",
        help="Allow restarting an existing workflow workspace outside failed status",
    )
    _add_json_argument(run_dir_parser)
    run_dir_parser.set_defaults(func=commands.cmd_run_dir)


def _register_activity_parsers(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    commands = _commands()
    activity_list_parser = subparsers.add_parser(
        "list", help="List workflows and standalone engine activities together."
    )
    _add_workflow_root_argument(activity_list_parser)
    activity_list_parser.add_argument(
        "--limit", type=int, default=0, help="Optional maximum number of activities to print"
    )
    activity_list_parser.add_argument(
        "--refresh", action="store_true", help="Refresh workflow registry before listing"
    )
    _add_chemstack_config_argument(activity_list_parser)
    _add_json_argument(activity_list_parser)
    activity_list_parser.set_defaults(func=commands.cmd_activity_list)

    activity_cancel_parser = subparsers.add_parser(
        "cancel", help="Cancel a workflow or standalone engine activity."
    )
    activity_cancel_parser.add_argument(
        "target", help="Activity id, workflow id, queue id, run id, or known path alias"
    )
    _add_workflow_root_argument(activity_cancel_parser)
    _add_chemstack_config_argument(activity_cancel_parser)
    _add_json_argument(activity_cancel_parser)
    activity_cancel_parser.set_defaults(func=commands.cmd_activity_cancel)

    bot_parser = subparsers.add_parser("bot", help="Run the ChemStack flow Telegram bot.")
    bot_parser.set_defaults(func=commands.cmd_bot)


def _register_engine_inspect_parsers(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    commands = _commands()
    xtb_parser = subparsers.add_parser("xtb", help="Inspect and adapt xTB artifacts.")
    xtb_subparsers = xtb_parser.add_subparsers(dest="xtb_command", required=True)

    inspect_parser = xtb_subparsers.add_parser(
        "inspect", help="Load a normalized xTB artifact contract."
    )
    inspect_parser.add_argument("target", help="xTB job_id or job directory")
    inspect_parser.add_argument(
        "--xtb-index-root", required=True, help="xTB index root, usually allowed_root"
    )
    _add_json_argument(inspect_parser)
    inspect_parser.set_defaults(func=commands.cmd_xtb_inspect)

    candidates_parser = xtb_subparsers.add_parser(
        "candidates", help="Select downstream-ready xTB candidate inputs."
    )
    candidates_parser.add_argument("target", help="xTB job_id or job directory")
    candidates_parser.add_argument(
        "--xtb-index-root", required=True, help="xTB index root, usually allowed_root"
    )
    candidates_parser.add_argument(
        "--max-candidates", type=int, default=3, help="Maximum number of candidates to emit"
    )
    candidates_parser.add_argument(
        "--preferred-kind",
        dest="preferred_kinds",
        action="append",
        help="Preferred candidate kind in priority order; may be passed more than once",
    )
    candidates_parser.add_argument(
        "--include-unselected",
        action="store_true",
        help="Consider non-selected candidate_details when building downstream inputs",
    )
    _add_json_argument(candidates_parser)
    candidates_parser.set_defaults(func=commands.cmd_xtb_candidates)

    crest_parser = subparsers.add_parser("crest", help="Inspect CREST artifacts.")
    crest_subparsers = crest_parser.add_subparsers(dest="crest_command", required=True)
    crest_inspect_parser = crest_subparsers.add_parser(
        "inspect", help="Load a normalized CREST artifact contract."
    )
    crest_inspect_parser.add_argument("target", help="CREST job_id or job directory")
    crest_inspect_parser.add_argument(
        "--crest-index-root", required=True, help="CREST index root, usually allowed_root"
    )
    _add_json_argument(crest_inspect_parser)
    crest_inspect_parser.set_defaults(func=commands.cmd_crest_inspect)


def _register_workflow_registry_parsers(
    workflow_subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    commands = _commands()
    list_parser = workflow_subparsers.add_parser(
        "list", help="List materialized workflows under a workflow root."
    )
    _add_workflow_root_argument(list_parser, required=True)
    list_parser.add_argument(
        "--limit", type=int, default=0, help="Optional maximum number of workflows to print"
    )
    list_parser.add_argument(
        "--refresh",
        action="store_true",
        help="Rebuild the registry from workflow workspaces before listing",
    )
    _add_json_argument(list_parser)
    list_parser.set_defaults(func=commands.cmd_workflow_list)

    get_parser = workflow_subparsers.add_parser("get", help="Inspect one materialized workflow.")
    get_parser.add_argument(
        "target", help="workflow_id, workflow workspace directory, or workflow.json path"
    )
    _add_workflow_root_argument(get_parser)
    _add_json_argument(get_parser)
    get_parser.set_defaults(func=commands.cmd_workflow_get)

    artifacts_parser = workflow_subparsers.add_parser(
        "artifacts",
        help="List known materialized artifacts for one workflow.",
    )
    artifacts_parser.add_argument(
        "target", help="workflow_id, workflow workspace directory, or workflow.json path"
    )
    _add_workflow_root_argument(artifacts_parser)
    _add_json_argument(artifacts_parser)
    artifacts_parser.set_defaults(func=commands.cmd_workflow_artifacts)

    cancel_parser = workflow_subparsers.add_parser(
        "cancel",
        help="Cancel a materialized workflow and request queue cancellation for submitted engine stages.",
    )
    cancel_parser.add_argument(
        "target", help="workflow_id, workflow workspace directory, or workflow.json path"
    )
    _add_workflow_root_argument(cancel_parser)
    _add_chemstack_config_argument(
        cancel_parser,
        help_text="Path to shared chemstack.yaml; required if submitted stages exist",
    )
    _add_json_argument(cancel_parser)
    cancel_parser.set_defaults(func=commands.cmd_workflow_cancel)

    reindex_parser = workflow_subparsers.add_parser(
        "reindex", help="Rebuild the workflow registry from workflow workspaces."
    )
    _add_workflow_root_argument(reindex_parser, required=True)
    _add_json_argument(reindex_parser)
    reindex_parser.set_defaults(func=commands.cmd_workflow_reindex)

    runtime_status_parser = workflow_subparsers.add_parser(
        "runtime-status",
        help="Show the current worker heartbeat/state for a workflow root.",
    )
    _add_workflow_root_argument(runtime_status_parser, required=True)
    _add_json_argument(runtime_status_parser)
    runtime_status_parser.set_defaults(func=commands.cmd_workflow_runtime_status)

    journal_parser = workflow_subparsers.add_parser(
        "journal",
        help="Show recent append-only orchestration journal events.",
    )
    _add_workflow_root_argument(journal_parser, required=True)
    journal_parser.add_argument(
        "--limit", type=int, default=50, help="Maximum number of recent events to show"
    )
    _add_json_argument(journal_parser)
    journal_parser.set_defaults(func=commands.cmd_workflow_journal)

    telemetry_parser = workflow_subparsers.add_parser(
        "telemetry",
        help="Summarize registry status, worker heartbeat, and recent journal activity.",
    )
    _add_workflow_root_argument(telemetry_parser, required=True)
    telemetry_parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum number of recent journal events to summarize",
    )
    _add_json_argument(telemetry_parser)
    telemetry_parser.set_defaults(func=commands.cmd_workflow_telemetry)


def _register_workflow_planning_parsers(
    workflow_subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    commands = _commands()
    reaction_ts_parser = workflow_subparsers.add_parser(
        "reaction-ts-search",
        help="Build a reaction_ts_search workflow plan from xTB results.",
    )
    reaction_ts_parser.add_argument("target", help="xTB job_id or job directory")
    reaction_ts_parser.add_argument(
        "--xtb-index-root", required=True, help="xTB index root, usually allowed_root"
    )
    reaction_ts_parser.add_argument(
        "--max-orca-stages",
        type=int,
        default=3,
        help="Maximum number of ORCA stage payloads to emit",
    )
    reaction_ts_parser.add_argument(
        "--include-unselected",
        action="store_true",
        help="Consider non-selected xTB candidate_details when planning",
    )
    reaction_ts_parser.add_argument(
        "--workspace-root",
        help="If provided, materialize a workflow workspace with ORCA reaction directories and workflow.json",
    )
    _add_orca_materialization_arguments(
        reaction_ts_parser,
        route_default="! r2scan-3c OptTS Freq TightSCF",
    )
    reaction_ts_parser.add_argument("--priority", type=int, default=10, help="Planned queue priority")
    _add_json_argument(reaction_ts_parser)
    reaction_ts_parser.set_defaults(func=commands.cmd_workflow_reaction_ts_search)

    conformer_parser = workflow_subparsers.add_parser(
        "conformer-screening",
        help="Build a conformer_screening workflow plan from CREST results (`standard` or `nci`).",
    )
    conformer_parser.add_argument("target", help="CREST job_id or job directory")
    conformer_parser.add_argument(
        "--crest-index-root", required=True, help="CREST index root, usually allowed_root"
    )
    conformer_parser.add_argument(
        "--max-orca-stages",
        type=int,
        default=3,
        help="Maximum number of ORCA stage payloads to emit",
    )
    conformer_parser.add_argument("--workspace-root", help="If provided, materialize a workflow workspace")
    _add_orca_materialization_arguments(
        conformer_parser,
        route_default="! r2scan-3c Opt TightSCF",
    )
    conformer_parser.add_argument("--priority", type=int, default=10, help="Planned queue priority")
    _add_json_argument(conformer_parser)
    conformer_parser.set_defaults(func=commands.cmd_workflow_conformer_screening)


def _register_workflow_creation_parsers(
    workflow_subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    commands = _commands()
    create_reaction_parser = workflow_subparsers.add_parser(
        "create-reaction-ts-search",
        help="Create a raw-input reaction_ts_search workflow from reactant/product precomplex XYZ inputs.",
    )
    create_reaction_parser.add_argument(
        "--reactant-xyz",
        dest="reactant_xyz",
        required=True,
        help="Reactant-side precomplex XYZ input",
    )
    create_reaction_parser.add_argument(
        "--product-xyz",
        dest="product_xyz",
        required=True,
        help="Product-side XYZ input",
    )
    _add_workflow_root_argument(create_reaction_parser, required=True)
    create_reaction_parser.add_argument(
        "--crest-mode",
        default="standard",
        help="CREST mode for initial stages (`standard` or `nci`)",
    )
    create_reaction_parser.add_argument("--priority", type=int, default=10)
    create_reaction_parser.add_argument("--max-crest-candidates", type=int, default=3)
    create_reaction_parser.add_argument("--max-xtb-stages", type=int, default=3)
    create_reaction_parser.add_argument("--max-orca-stages", type=int, default=3)
    _add_orca_materialization_arguments(
        create_reaction_parser,
        route_default="! r2scan-3c OptTS Freq TightSCF",
    )
    _add_json_argument(create_reaction_parser)
    create_reaction_parser.set_defaults(func=commands.cmd_workflow_create_reaction_ts_search)

    create_conformer_parser = workflow_subparsers.add_parser(
        "create-conformer-screening",
        help="Create a raw-input conformer_screening workflow that can be advanced through CREST and ORCA (`standard` or `nci`).",
    )
    create_conformer_parser.add_argument(
        "--input-xyz", required=True, help="Input XYZ for the molecule to screen"
    )
    _add_workflow_root_argument(create_conformer_parser, required=True)
    create_conformer_parser.add_argument(
        "--crest-mode", default="standard", help="CREST mode for the initial stage"
    )
    create_conformer_parser.add_argument("--priority", type=int, default=10)
    create_conformer_parser.add_argument("--max-orca-stages", type=int, default=3)
    _add_orca_materialization_arguments(
        create_conformer_parser,
        route_default="! r2scan-3c Opt TightSCF",
    )
    _add_json_argument(create_conformer_parser)
    create_conformer_parser.set_defaults(func=commands.cmd_workflow_create_conformer_screening)


def _register_workflow_runtime_parsers(
    workflow_subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    commands = _commands()
    advance_parser = workflow_subparsers.add_parser(
        "advance",
        help="Advance a materialized workflow by syncing/submitting actionable CREST, xTB, and ORCA stages.",
    )
    advance_parser.add_argument(
        "target", help="workflow_id, workflow workspace directory, or workflow.json path"
    )
    _add_workflow_root_argument(advance_parser, required=True)
    _add_chemstack_config_argument(advance_parser)
    advance_parser.add_argument(
        "--no-submit",
        action="store_true",
        help="Only sync and append stages; do not submit newly actionable stages",
    )
    _add_json_argument(advance_parser)
    advance_parser.set_defaults(func=commands.cmd_workflow_advance)

    worker_parser = workflow_subparsers.add_parser(
        "worker",
        help="Continuously advance non-terminal workflows from the registry.",
    )
    worker_parser.add_argument(
        "--workflow-root",
        help="Root that directly contains workflow workspaces. Defaults to workflow.root in chemstack.yaml.",
    )
    _add_chemstack_config_argument(worker_parser)
    worker_parser.add_argument(
        "--no-submit",
        action="store_true",
        help="Only sync/append stages; do not submit newly actionable stages",
    )
    worker_parser.add_argument("--once", action="store_true", help="Run exactly one orchestration cycle")
    worker_parser.add_argument(
        "--max-cycles", type=int, default=0, help="Optional cycle limit; 0 means run forever"
    )
    worker_parser.add_argument(
        "--interval-seconds",
        type=float,
        default=30.0,
        help="Sleep interval between orchestration cycles",
    )
    worker_parser.add_argument(
        "--lock-timeout-seconds",
        type=float,
        default=5.0,
        help="How long to wait for the worker lock",
    )
    worker_parser.add_argument(
        "--refresh-registry",
        action="store_true",
        help="Reindex the workflow registry before the first cycle",
    )
    worker_parser.add_argument(
        "--refresh-each-cycle",
        action="store_true",
        help="Reindex the workflow registry before every cycle",
    )
    _add_json_argument(worker_parser)
    worker_parser.set_defaults(func=commands.cmd_workflow_worker)

    submit_parser = workflow_subparsers.add_parser(
        "submit-reaction-ts-search",
        help="Submit a materialized reaction_ts_search workflow into chemstack ORCA.",
    )
    submit_parser.add_argument(
        "target", help="workflow_id, workflow workspace directory, or workflow.json path"
    )
    _add_workflow_root_argument(submit_parser)
    _add_chemstack_config_argument(submit_parser, required=True)
    submit_parser.add_argument(
        "--resubmit", action="store_true", help="Retry stages already marked as submitted"
    )
    _add_json_argument(submit_parser)
    submit_parser.set_defaults(func=commands.cmd_workflow_submit_reaction_ts_search)


def _register_workflow_parsers(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    workflow_parser = subparsers.add_parser("workflow", help="Build chemistry workflow plans.")
    workflow_subparsers = workflow_parser.add_subparsers(dest="workflow_command", required=True)
    _register_workflow_registry_parsers(workflow_subparsers)
    _register_workflow_planning_parsers(workflow_subparsers)
    _register_workflow_creation_parsers(workflow_subparsers)
    _register_workflow_runtime_parsers(workflow_subparsers)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.flow.cli")
    subparsers = parser.add_subparsers(dest="command", required=True)
    _register_run_dir_parser(subparsers)
    _register_activity_parsers(subparsers)
    _register_engine_inspect_parsers(subparsers)
    _register_workflow_parsers(subparsers)
    return parser
