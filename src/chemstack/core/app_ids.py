from __future__ import annotations

CHEMSTACK_CLI_MODULE = "chemstack.cli"
CHEMSTACK_CLI_COMMAND = f"python -m {CHEMSTACK_CLI_MODULE}"
CHEMSTACK_WORKFLOW_WORKER_MODULE = "chemstack.flow.cli_workflow"
CHEMSTACK_WORKFLOW_WORKER_COMMAND = f"python -m {CHEMSTACK_WORKFLOW_WORKER_MODULE}"

CHEMSTACK_ORCA_APP_NAME = "chemstack_orca"

CHEMSTACK_ORCA_SOURCE = "chemstack_orca"

CHEMSTACK_ORCA_SUBMITTER = "chemstack_orca"
ORCA_SUBMITTERS = frozenset({CHEMSTACK_ORCA_SUBMITTER})

CHEMSTACK_EXECUTABLE = "chemstack"

CHEMSTACK_CONFIG_ENV_VAR = "CHEMSTACK_CONFIG"

CHEMSTACK_REPO_ROOT_ENV_VAR = "CHEMSTACK_REPO_ROOT"


def is_orca_submitter(value: object | None) -> bool:
    return str(value or "").strip() in ORCA_SUBMITTERS
