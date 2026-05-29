# ChemStack Quickstart

This guide is the shortest path from a fresh checkout to a supervised
ChemStack queue worker.

## 1) Install

```bash
cd <repo_root>
bash scripts/bootstrap_wsl.sh
source .venv/bin/activate
```

The bootstrap script creates `.venv`, installs ChemStack, and creates
`config/chemstack.yaml` from the example template when needed.

## 2) Configure

```bash
chemstack init
```

Use absolute Linux paths for ORCA, xTB, CREST, and run directories. If you want
Telegram notifications, set `telegram.bot_token` and `telegram.chat_id` during
init or edit `config/chemstack.yaml` afterward.

## 3) Install The Runtime Service

```bash
chemstack systemd install --user "$(whoami)" --repo "$(pwd)"
```

If Telegram is configured, ChemStack enables the full runtime target. If
Telegram is still empty, ChemStack enables only the queue worker.

## 4) Check Or Restart Services

```bash
chemstack service status
chemstack service restart
```

`service status` shows the runtime target, queue worker, and Telegram bot.
`service restart` restarts the full runtime target when it is enabled; otherwise
it restarts the queue worker.

## 5) Submit Work

```bash
chemstack run-dir '/home/user/orca_runs/sample_rxn'
chemstack run-dir '/home/user/workflow_inputs/reaction_case'
```

`run-dir` queues work durably. Closing the terminal after a successful queue
submission is safe because the systemd worker performs the actual execution.

## 6) Watch The Queue

```bash
chemstack queue list
chemstack queue list --engine orca
chemstack queue cancel <target>
```

Use `chemstack queue list clear` when you want to prune completed, failed, and
cancelled entries from the unified activity list.

## Troubleshooting

```bash
chemstack service status
chemstack service restart
chemstack queue list --refresh
```

If a service still does not behave as expected, use the deeper systemd commands
in `systemd/README.md`.
