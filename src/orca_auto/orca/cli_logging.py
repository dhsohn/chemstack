from __future__ import annotations

import argparse
import logging
import logging.handlers
import sys

_ORCA_AUTO_HANDLER_ATTR = "_orca_auto_managed_handler"


def configure_logging(args: argparse.Namespace) -> None:
    log_level = logging.DEBUG if getattr(args, "verbose", False) else logging.INFO
    log_file = getattr(args, "log_file", None)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    remove_managed_handlers(root_logger)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    if log_file:
        handler: logging.Handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
    else:
        handler = logging.StreamHandler(sys.stderr)

    handler.setFormatter(formatter)
    setattr(handler, _ORCA_AUTO_HANDLER_ATTR, True)
    root_logger.addHandler(handler)


def remove_managed_handlers(root_logger: logging.Logger) -> None:
    for handler in list(root_logger.handlers):
        if not getattr(handler, _ORCA_AUTO_HANDLER_ATTR, False):
            continue
        root_logger.removeHandler(handler)
        try:
            handler.close()
        except Exception as exc:  # noqa: BLE001
            logging.getLogger(__name__).debug("log_handler_close_failed: error=%s", exc)
