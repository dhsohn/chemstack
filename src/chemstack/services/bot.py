from __future__ import annotations

import os

from chemstack.core.app_ids import CHEMSTACK_CONFIG_ENV_VAR
from chemstack.flow.telegram_bot import run_bot, settings_from_config


def main() -> int:
    config_path = str(os.getenv(CHEMSTACK_CONFIG_ENV_VAR, "")).strip() or None
    return int(run_bot(settings_from_config(config_path)))


if __name__ == "__main__":
    raise SystemExit(main())

