from __future__ import annotations

import argparse

from chemstack import cli_handlers
from chemstack import cli_queue
from chemstack import cli_systemd
from chemstack import cli_workers

_WORKFLOW_SCAFFOLD_SHORTCUTS = (
    ("ts_search", "reaction_ts_search", "Create a reaction TS-search scaffold."),
    ("conformer_search", "conformer_screening", "Create a conformer-screening scaffold."),
)


def _add_workflow_scaffold_shortcut(
    scaffold_subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    *,
    name: str,
    workflow_type: str,
    help_text: str,
) -> None:
    parser = scaffold_subparsers.add_parser(name, help=help_text)
    parser.add_argument("root", help="Workflow input directory to create")
    parser.set_defaults(
        func=cli_handlers.cmd_workflow_scaffold,
        workflow_type=workflow_type,
    )


def _add_engine_config_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--chemstack-config",
        "--config",
        dest="config",
        default=None,
        help="Path to shared chemstack.yaml",
    )


def _add_orca_logging_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--log-file", default=None, help="Write logs to file (with rotation, max 10MB x 5)"
    )


def _add_json_argument(parser: argparse.ArgumentParser, *, help_text: str = "Print JSON output") -> None:
    parser.add_argument("--json", action="store_true", help=help_text)


def _add_resource_override_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--max-cores",
        type=int,
        default=None,
        help="Override max cores recorded for this queued run or workflow",
    )
    parser.add_argument(
        "--max-memory-gb",
        type=int,
        default=None,
        help="Override max memory (GB) recorded for this queued run or workflow",
    )


def _add_queue_list_parser(
    queue_subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    list_parser = queue_subparsers.add_parser(
        "list", help="List workflows and engine activities together."
    )
    list_parser.add_argument(
        "action",
        nargs="?",
        choices=["clear"],
        help="Remove completed/failed/cancelled entries from the unified activity list",
    )
    list_parser.add_argument(
        "--chemstack-config",
        "--config",
        dest="chemstack_config",
        help="Path to shared chemstack.yaml",
    )
    list_parser.add_argument(
        "--limit", type=int, default=0, help="Optional maximum number of activities to print"
    )
    list_parser.add_argument(
        "--refresh", action="store_true", help="Refresh workflow registry before listing"
    )
    list_parser.add_argument(
        "--engine",
        action="append",
        choices=["orca", "xtb", "crest", "workflow"],
        help="Filter by engine; may be passed more than once",
    )
    list_parser.add_argument(
        "--status", action="append", help="Filter by status; may be passed more than once"
    )
    list_parser.add_argument(
        "--kind",
        action="append",
        choices=["job", "workflow"],
        help="Filter by activity kind; may be passed more than once",
    )
    _add_json_argument(list_parser)
    list_parser.set_defaults(func=cli_queue.cmd_queue_list)


def _add_queue_cancel_parser(
    queue_subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    cancel_parser = queue_subparsers.add_parser(
        "cancel", help="Cancel a workflow or engine activity."
    )
    cancel_parser.add_argument(
        "target", help="Activity id, workflow id, queue id, run id, or known path alias"
    )
    cancel_parser.add_argument(
        "--chemstack-config",
        "--config",
        dest="chemstack_config",
        help="Path to shared chemstack.yaml",
    )
    _add_json_argument(cancel_parser)
    cancel_parser.set_defaults(func=cli_queue.cmd_queue_cancel)


def _add_queue_worker_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--app",
        action="append",
        choices=["orca", "workflow"],
        help="Worker app to supervise; may be passed more than once",
    )
    parser.add_argument("--workflow-root", help="Workflow root for workflow supervision")
    parser.add_argument(
        "--chemstack-config",
        "--config",
        dest="chemstack_config",
        help="Path to shared chemstack.yaml",
    )
    auto_group = parser.add_mutually_exclusive_group()
    auto_group.add_argument(
        "--auto-organize",
        action="store_true",
        help="Enable ORCA auto-organization in the supervised worker",
    )
    auto_group.add_argument(
        "--no-auto-organize",
        action="store_true",
        help="Disable ORCA auto-organization in the supervised worker",
    )
    parser.add_argument(
        "--no-submit",
        action="store_true",
        help="Only sync/append workflow stages; do not submit newly actionable stages",
    )
    parser.add_argument(
        "--refresh-registry",
        action="store_true",
        help="Reindex the workflow registry before the first worker cycle",
    )
    parser.add_argument(
        "--refresh-each-cycle",
        action="store_true",
        help="Reindex the workflow registry before every worker cycle",
    )
    parser.add_argument("--max-cycles", type=int, default=0, help="Workflow cycle limit")
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=0.0,
        help="Workflow worker sleep interval",
    )
    parser.add_argument(
        "--lock-timeout-seconds",
        type=float,
        default=0.0,
        help="Workflow worker lock timeout",
    )
    _add_json_argument(parser, help_text="Print worker commands as JSON without starting them")


def _add_queue_worker_parser(
    queue_subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    worker_parser = queue_subparsers.add_parser(
        "worker", help="Run the unified worker supervisor."
    )
    _add_queue_worker_options(worker_parser)
    worker_parser.set_defaults(func=cli_workers.cmd_queue_worker)


def _add_queue_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    queue_parser = subparsers.add_parser(
        "queue",
        help="Unified queue and worker commands across ORCA, workflow-managed internal engines, and workflows.",
    )
    queue_subparsers = queue_parser.add_subparsers(dest="queue_command", required=True)
    _add_queue_list_parser(queue_subparsers)
    _add_queue_cancel_parser(queue_subparsers)
    _add_queue_worker_parser(queue_subparsers)


def _add_run_dir_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    run_dir_parser = subparsers.add_parser(
        "run-dir",
        help="Submit an ORCA job directory or workflow input directory through the unified CLI.",
    )
    _add_engine_config_argument(run_dir_parser)
    _add_orca_logging_arguments(run_dir_parser)
    run_dir_parser.add_argument("path", help="ORCA job directory or workflow input directory")
    run_dir_parser.add_argument(
        "--force",
        action="store_true",
        help="Force ORCA re-run, or allow restarting an existing workflow workspace outside failed status",
    )
    run_dir_parser.add_argument(
        "--priority",
        type=int,
        default=None,
        help="Queue priority when submission is enqueued (lower = higher)",
    )
    _add_resource_override_arguments(run_dir_parser)
    _add_json_argument(run_dir_parser, help_text="Print JSON output for workflow submission")
    run_dir_parser.set_defaults(func=cli_handlers.cmd_run_dir)


def _add_init_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    init_parser = subparsers.add_parser(
        "init",
        help="Interactively create or update the shared chemstack.yaml config.",
    )
    _add_engine_config_argument(init_parser)
    _add_orca_logging_arguments(init_parser)
    init_parser.add_argument(
        "--force", action="store_true", help="Overwrite existing config without confirmation"
    )
    init_parser.set_defaults(func=cli_handlers.cmd_init)


def _add_scaffold_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    scaffold_parser = subparsers.add_parser(
        "scaffold",
        help="Create raw input workflow scaffold directories.",
    )
    scaffold_subparsers = scaffold_parser.add_subparsers(dest="scaffold_app", required=True)

    for name, workflow_type, help_text in _WORKFLOW_SCAFFOLD_SHORTCUTS:
        _add_workflow_scaffold_shortcut(
            scaffold_subparsers,
            name=name,
            workflow_type=workflow_type,
            help_text=help_text,
        )


def _add_organize_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    organize_parser = subparsers.add_parser(
        "organize",
        help="Plan or apply organization of terminal engine outputs.",
    )
    organize_subparsers = organize_parser.add_subparsers(dest="organize_app", required=True)

    orca_organize_parser = organize_subparsers.add_parser(
        "orca", help="Plan or apply organization into orca_outputs"
    )
    _add_engine_config_argument(orca_organize_parser)
    _add_orca_logging_arguments(orca_organize_parser)
    orca_organize_parser.add_argument(
        "--reaction-dir", default=None, help="Single job directory to organize"
    )
    orca_organize_parser.add_argument(
        "--root",
        default=None,
        help="Root directory to scan (mutually exclusive with --reaction-dir)",
    )
    orca_organize_parser.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Actually move files (default is dry-run)",
    )
    orca_organize_parser.add_argument(
        "--rebuild-index",
        action="store_true",
        default=False,
        help="Rebuild JSONL index from organized directories",
    )
    orca_organize_parser.set_defaults(func=cli_handlers.cmd_orca_organize)


def _add_summary_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    summary_parser = subparsers.add_parser(
        "summary",
        help="Show the ORCA/workflow summary or send its Telegram digest.",
    )
    _add_engine_config_argument(summary_parser)
    _add_orca_logging_arguments(summary_parser)
    summary_parser.add_argument(
        "--no-send",
        action="store_true",
        default=False,
        help="Print summary without sending Telegram",
    )
    summary_parser.set_defaults(func=cli_handlers.cmd_summary)


def _add_monitor_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    monitor_parser = subparsers.add_parser(
        "monitor",
        help="Send ORCA Telegram alerts for newly discovered DFT results or scan failures.",
    )
    _add_engine_config_argument(monitor_parser)
    _add_orca_logging_arguments(monitor_parser)
    monitor_parser.set_defaults(func=cli_handlers.cmd_orca_monitor)


def _add_systemd_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    systemd_parser = subparsers.add_parser(
        "systemd",
        help="Install ChemStack systemd runtime units.",
    )
    systemd_subparsers = systemd_parser.add_subparsers(dest="systemd_command", required=True)

    install_parser = systemd_subparsers.add_parser(
        "install",
        help="Render, install, reload, and optionally enable ChemStack systemd units.",
    )
    install_parser.add_argument(
        "--user",
        dest="target_user",
        required=True,
        help="Linux user name used for the templated systemd instance",
    )
    install_parser.add_argument(
        "--repo",
        required=True,
        help="Absolute path to the ChemStack repository checkout",
    )
    install_parser.add_argument(
        "--config",
        default=None,
        help=argparse.SUPPRESS,
    )
    install_parser.add_argument(
        "--unit-dir",
        default=str(cli_systemd.DEFAULT_SYSTEMD_UNIT_DIR),
        help=argparse.SUPPRESS,
    )
    install_parser.add_argument(
        "--worker-only",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    install_parser.add_argument(
        "--no-enable",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    install_parser.add_argument(
        "--no-start",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    install_parser.add_argument(
        "--dry-run",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    install_parser.add_argument(
        "--no-sudo",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    install_parser.set_defaults(func=cli_systemd.cmd_systemd_install)


def _add_service_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    service_parser = subparsers.add_parser(
        "service",
        help="Check or restart ChemStack systemd services.",
    )
    service_subparsers = service_parser.add_subparsers(dest="service_command", required=True)

    status_parser = service_subparsers.add_parser(
        "status",
        help="Show ChemStack service status.",
    )
    status_parser.set_defaults(func=cli_systemd.cmd_service_status)

    restart_parser = service_subparsers.add_parser(
        "restart",
        help="Restart the ChemStack runtime or queue worker service.",
    )
    restart_parser.set_defaults(func=cli_systemd.cmd_service_restart)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="chemstack")
    subparsers = parser.add_subparsers(dest="command", required=True)
    _add_queue_parser(subparsers)
    _add_run_dir_parser(subparsers)
    _add_init_parser(subparsers)
    _add_scaffold_parser(subparsers)
    _add_organize_parser(subparsers)
    _add_summary_parser(subparsers)
    _add_monitor_parser(subparsers)
    _add_systemd_parser(subparsers)
    _add_service_parser(subparsers)
    return parser
