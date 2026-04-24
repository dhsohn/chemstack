# ChemStack

[![CI](https://github.com/dhsohn/chemstack/actions/workflows/ci.yml/badge.svg)](https://github.com/dhsohn/chemstack/actions/workflows/ci.yml)

ChemStack is a queue-first interface for ORCA and workflow orchestration on Linux and WSL. xTB and CREST remain part of the runtime, but they are now used internally for workflow stages rather than as standalone public surfaces. It submits work durably, runs it under supervised workers, records per-job state and reports, and organizes completed outputs.

## Docs

- Runtime and command reference: [docs/REFERENCE.md](docs/REFERENCE.md)
- WSL and `systemd` runtime setup: [systemd/README.md](systemd/README.md)
- Package layout and development notes: [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)

## Install

Requirements:

- Python 3.10+
- Linux or WSL2
- ORCA installed at an absolute Linux path if you use ORCA
- xTB and CREST installed at absolute Linux paths if you use workflow stages that depend on them

Setup:

```bash
cd <repo_root>
bash scripts/bootstrap_wsl.sh
source .venv/bin/activate
```

`bootstrap_wsl.sh` creates `.venv`, installs the project, and seeds `config/chemstack.yaml` from the example template when needed.
If you do not activate the virtual environment, you can still run the installed CLI directly as `.venv/bin/chemstack ...`.

## Configure

Create or update `chemstack.yaml`:

```bash
chemstack init
```

Config search order:

1. `CHEMSTACK_CONFIG`
2. `<project_root>/config/chemstack.yaml`
3. `~/chemstack/config/chemstack.yaml`

Minimal example:

```yaml
scheduler:
  max_active_simulations: 4

workflow:
  root: /home/user/workflow_runs
  paths:
    xtb_executable: /home/user/bin/xtb-dist/bin/xtb
    crest_executable: /home/user/bin/crest/crest

telegram:
  bot_token: ""
  chat_id: ""

orca:
  runtime:
    allowed_root: /home/user/orca_runs
    organized_root: /home/user/orca_outputs
    default_max_retries: 2
  paths:
    orca_executable: /home/user/opt/orca/orca
```

Notes:

- Use Linux paths only.
- `default_max_retries: 2` means `1 initial + 2 retries = 3` total attempts.
- `scheduler.max_active_simulations` is the shared cap across ORCA, internal xTB workflow stages, and internal CREST workflow stages.
- `workflow.root` is the workflow root used by the unified CLI and workflow worker.
- Internal xTB/CREST runtimes no longer use a shared `workflow.root/internal/...` root.
- Workflow-managed xTB/CREST job dirs, per-workflow queues/indexes, and organized outputs live only under `workflow.root/<workflow_id>/internal/<engine>/{runs,outputs}`.
- The full template lives at [config/chemstack.yaml.example](config/chemstack.yaml.example).

## User Commands

User-facing submission, inspection, and maintenance commands use `chemstack ...`.

```bash
# create/update shared config
chemstack init

# create raw input scaffolds when they help
chemstack scaffold ts_search '/home/user/workflow_inputs/rxn_001'
chemstack scaffold conformer_search '/home/user/workflow_inputs/conf_001'

# submit work
chemstack run-dir '/home/user/orca_runs/sample_rxn'
chemstack run-dir '/home/user/workflow_inputs/reaction_case'

# inspect and maintain
chemstack queue list --engine orca
chemstack queue list clear
chemstack queue cancel <target>
chemstack organize orca --root '/home/user/orca_runs' --apply
chemstack summary orca --no-send
```

`queue list` groups workflow child simulations under their parent workflow with indentation.
Use `chemstack queue list clear` to prune completed, failed, and cancelled entries from
the unified list. The Telegram bot supports the same cleanup via `/list clear`.
The `active_simulations` line counts only simulations that currently consume the shared
`scheduler.max_active_simulations` slots.

Long-running services are managed through `systemd` only. After `chemstack.yaml`
is configured, enable the combined runtime target once and let `systemd` start
both the worker and the bot automatically:

```bash
cd <repo_root>
sudo cp systemd/chemstack-queue-worker@.service /etc/systemd/system/
sudo cp systemd/chemstack-bot@.service /etc/systemd/system/
sudo cp systemd/chemstack-runtime@.target /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now "chemstack-runtime@$(whoami).target"
sudo systemctl status "chemstack-runtime@$(whoami).target"
sudo systemctl status "chemstack-queue-worker@$(whoami)"
sudo systemctl status "chemstack-bot@$(whoami)"
```

Restart examples:

```bash
# restart the combined runtime target (worker + Telegram bot)
sudo systemctl restart "chemstack-runtime@$(whoami).target"

# restart only one service when needed
sudo systemctl restart "chemstack-queue-worker@$(whoami)"
sudo systemctl restart "chemstack-bot@$(whoami)"
```

If you edited files under `systemd/`, run `sudo systemctl daemon-reload` before restarting.

If you want only the worker managed automatically, enable
`chemstack-queue-worker@$(whoami)` instead.

## Runtime Notes

- `run-dir` enqueues work durably; workers perform execution.
- If no worker is running, queued jobs remain pending until one returns.
- ORCA selects the most recently modified `.inp` when execution starts.
- Completed ORCA runs write state and report files such as `run_state.json`, `run_report.json`, and `run_report.md`.
- Use the `systemd` assets in [systemd/README.md](systemd/README.md) for unattended WSL or Linux execution.

## Testing

```bash
ruff check .
mypy
pytest --cov --cov-report=term-missing -q
```
