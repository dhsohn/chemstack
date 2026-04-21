from __future__ import annotations

CHEMSTACK_ORCA_MODULE = "chemstack.orca.cli"
CHEMSTACK_XTB_MODULE = "chemstack.xtb.cli"
CHEMSTACK_CREST_MODULE = "chemstack.crest.cli"
CHEMSTACK_FLOW_MODULE = "chemstack.flow.cli"

CHEMSTACK_ORCA_COMMAND = f"python -m {CHEMSTACK_ORCA_MODULE}"
CHEMSTACK_XTB_COMMAND = f"python -m {CHEMSTACK_XTB_MODULE}"
CHEMSTACK_CREST_COMMAND = f"python -m {CHEMSTACK_CREST_MODULE}"
CHEMSTACK_FLOW_COMMAND = f"python -m {CHEMSTACK_FLOW_MODULE}"
CHEMSTACK_CLI_MODULE = "chemstack.cli"
CHEMSTACK_CLI_COMMAND = f"python -m {CHEMSTACK_CLI_MODULE}"

CHEMSTACK_ORCA_APP_NAME = "chemstack_orca"
LEGACY_ORCA_APP_NAME = "orca_auto"
ORCA_APP_NAMES = frozenset({CHEMSTACK_ORCA_APP_NAME, LEGACY_ORCA_APP_NAME})

CHEMSTACK_ORCA_SOURCE = "chemstack_orca"
LEGACY_ORCA_SOURCE = "orca_auto"
ORCA_SOURCES = frozenset({CHEMSTACK_ORCA_SOURCE, LEGACY_ORCA_SOURCE})

CHEMSTACK_ORCA_SUBMITTER = "chemstack_orca_cli"
LEGACY_ORCA_SUBMITTER = "orca_auto_cli"
ORCA_SUBMITTERS = frozenset({CHEMSTACK_ORCA_SUBMITTER, LEGACY_ORCA_SUBMITTER})

# Internal compatibility label used by older queue/task metadata.
CHEMSTACK_EXECUTABLE = "orca_auto"
ORCA_EXECUTABLE_ALIASES = frozenset({CHEMSTACK_EXECUTABLE})

CHEMSTACK_CONFIG_ENV_VAR = "CHEMSTACK_CONFIG"

CHEMSTACK_REPO_ROOT_ENV_VAR = "CHEMSTACK_REPO_ROOT"
LEGACY_ORCA_REPO_ROOT_ENV_VAR = "ORCA_AUTO_REPO_ROOT"


def is_orca_app_name(value: object | None) -> bool:
    return str(value or "").strip() in ORCA_APP_NAMES


def is_orca_source(value: object | None) -> bool:
    return str(value or "").strip() in ORCA_SOURCES


def is_orca_submitter(value: object | None) -> bool:
    return str(value or "").strip() in ORCA_SUBMITTERS
