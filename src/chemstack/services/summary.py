from __future__ import annotations

from argparse import Namespace

from chemstack.cli_summary import cmd_summary


def main() -> int:
    return int(
        cmd_summary(
            Namespace(
                command="summary",
                summary_app="combined",
                chemstack_config=None,
                global_config=None,
                config=None,
                no_send=False,
                verbose=False,
                log_file=None,
            )
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
