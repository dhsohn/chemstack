# ChemStack

[![CI](https://github.com/dhsohn/chemstack/actions/workflows/ci.yml/badge.svg)](https://github.com/dhsohn/chemstack/actions/workflows/ci.yml)

ChemStack is a queue-first CLI for ORCA and workflow orchestration on Linux and WSL. xTB and CREST remain part of the runtime, but they are now used internally for workflow stages rather than as standalone public CLI surfaces. It submits work durably, runs it under supervised workers, records per-job state and reports, and organizes completed outputs.

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

## Configure

Create or update `chemstack.yaml`:

```bash
python -m chemstack.cli init
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
- Internal xTB/CREST runtime roots are derived automatically under `workflow.root/internal/...`.
- The full template lives at [config/chemstack.yaml.example](config/chemstack.yaml.example).

## Unified CLI

Public queue, submission, scaffold, organization, summary, and bot commands now use `python -m chemstack.cli ...`.

```bash
# create/update shared config
python -m chemstack.cli init

# create raw input scaffolds when they help
python -m chemstack.cli scaffold ts_search '/home/user/workflow_inputs/rxn_001'
python -m chemstack.cli scaffold conformer_search '/home/user/workflow_inputs/conf_001'

# start foreground services manually when not using systemd
python -m chemstack.cli queue worker
python -m chemstack.cli bot

# submit work
python -m chemstack.cli run-dir '/home/user/orca_runs/sample_rxn'
python -m chemstack.cli run-dir '/home/user/workflow_inputs/reaction_case'

# inspect and maintain
python -m chemstack.cli queue list --engine orca
python -m chemstack.cli queue cancel <target>
python -m chemstack.cli organize orca --root '/home/user/orca_runs' --apply
python -m chemstack.cli summary orca --no-send
```

`queue list` groups workflow child simulations under their parent workflow with indentation.
The `active_simulations` line counts only simulations that currently consume the shared
`scheduler.max_active_simulations` slots.

On WSL or Linux you usually should not type both long-running commands every
session. After `chemstack.yaml` is configured, enable the combined runtime
target once and let `systemd` start both the worker and the bot automatically:

```bash
cd <repo_root>
sudo cp systemd/chemstack-queue-worker@.service /etc/systemd/system/
sudo cp systemd/chemstack-bot@.service /etc/systemd/system/
sudo cp systemd/chemstack-runtime@.target /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now "chemstack-runtime@$(whoami)"
```

If you want only the worker managed automatically, enable
`chemstack-queue-worker@$(whoami)` instead.

Compatibility notes:

- `python -m chemstack.orca.cli` remains as the only public engine-specific wrapper.
- Standalone xTB and CREST CLI commands were removed. xTB job creation/execution and CREST job creation/execution now happen through workflow orchestration and internal runtime paths.
- ORCA-only commands that are not yet unified still live under `python -m chemstack.orca.cli`, such as `monitor`.
- Set top-level `workflow.root` in `chemstack.yaml` when you use workflow commands or want `python -m chemstack.cli queue worker` to supervise workflow activity, including internal xTB and CREST stages.
- Workflow scaffolds default to `crest_mode: standard`; switch to `nci` in `flow.yaml` when needed.
- `reaction_ts_search` expands all selected reactant x product CREST pairs into xTB path-search child jobs, and as each xTB child finishes it immediately queues the matching ORCA OptTS child job from that `ts_guess`.
- `conformer_search` starts with one CREST child job and then hands off up to 20 retained conformers to ORCA child jobs in the next workflow cycle.
- `python -m chemstack.cli bot` reuses the unified workflow activity layer, so `/list` shows workflow parents with indented child jobs and keeps standalone ORCA jobs top-level. `/cancel` can still target either a workflow or an individual job.

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
