from __future__ import annotations

CHEMSTACK_ORCA_MODULE = "chemstack.orca.cli"
CHEMSTACK_ORCA_INTERNAL_MODULE = "chemstack.orca._internal_cli"
CHEMSTACK_XTB_MODULE = "chemstack.xtb._internal_cli"
CHEMSTACK_CREST_MODULE = "chemstack.crest._internal_cli"
CHEMSTACK_FLOW_MODULE = "chemstack.flow.cli"

CHEMSTACK_ORCA_COMMAND = f"python -m {CHEMSTACK_ORCA_MODULE}"
CHEMSTACK_XTB_COMMAND = f"python -m {CHEMSTACK_XTB_MODULE}"
CHEMSTACK_CREST_COMMAND = f"python -m {CHEMSTACK_CREST_MODULE}"
CHEMSTACK_FLOW_COMMAND = f"python -m {CHEMSTACK_FLOW_MODULE}"
CHEMSTACK_CLI_MODULE = "chemstack.cli"
CHEMSTACK_CLI_COMMAND = f"python -m {CHEMSTACK_CLI_MODULE}"

CHEMSTACK_ORCA_APP_NAME = "chemstack_orca"

CHEMSTACK_ORCA_SOURCE = "chemstack_orca"

CHEMSTACK_ORCA_SUBMITTER = "chemstack_orca_cli"
ORCA_SUBMITTERS = frozenset({CHEMSTACK_ORCA_SUBMITTER})

CHEMSTACK_EXECUTABLE = "chemstack"

CHEMSTACK_CONFIG_ENV_VAR = "CHEMSTACK_CONFIG"

CHEMSTACK_REPO_ROOT_ENV_VAR = "CHEMSTACK_REPO_ROOT"


def is_orca_app_name(value: object | None) -> bool:
    return str(value or "").strip() == CHEMSTACK_ORCA_APP_NAME


def is_orca_source(value: object | None) -> bool:
    return str(value or "").strip() == CHEMSTACK_ORCA_SOURCE


def is_orca_submitter(value: object | None) -> bool:
    return str(value or "").strip() in ORCA_SUBMITTERS
