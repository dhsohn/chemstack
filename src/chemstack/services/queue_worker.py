from __future__ import annotations

import os
from types import SimpleNamespace

from chemstack import cli as unified_cli
from chemstack.core.app_ids import CHEMSTACK_CONFIG_ENV_VAR


def _service_args() -> SimpleNamespace:
    return SimpleNamespace(
        app=None,
        once=False,
        auto_organize=False,
        no_auto_organize=False,
        workflow_root=None,
        chemstack_config=str(os.getenv(CHEMSTACK_CONFIG_ENV_VAR, "")).strip() or None,
        no_submit=False,
        refresh_registry=False,
        refresh_each_cycle=False,
        max_cycles=0,
        interval_seconds=0.0,
        lock_timeout_seconds=0.0,
        json=False,
    )


def main() -> int:
    return int(unified_cli.cmd_queue_worker(_service_args()))


if __name__ == "__main__":
    raise SystemExit(main())
