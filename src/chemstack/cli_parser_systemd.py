from __future__ import annotations

import argparse

from chemstack import cli_systemd


def add_systemd_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
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


def add_service_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
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


__all__ = ["add_service_parser", "add_systemd_parser"]
