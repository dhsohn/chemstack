# ChemStack

[![CI](https://github.com/dhsohn/chemstack/actions/workflows/ci.yml/badge.svg)](https://github.com/dhsohn/chemstack/actions/workflows/ci.yml)

ChemStack is a queue-first CLI for ORCA, xTB, CREST, and workflow orchestration on Linux and WSL. It submits work durably, runs it under supervised workers, records per-job state and reports, and organizes completed outputs.

## Docs

- Runtime and command reference: [docs/REFERENCE.md](docs/REFERENCE.md)
- WSL and `systemd` worker setup: [systemd/README.md](systemd/README.md)
- Package layout and development notes: [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)

## Install

Requirements:

- Python 3.10+
- Linux or WSL2
- ORCA installed at an absolute Linux path if you use ORCA
- xTB and CREST installed at absolute Linux paths if you use those engines

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
python -m chemstack.cli init orca
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
  root: /home/user/workflow_root

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

xtb:
  runtime:
    allowed_root: /home/user/xtb_runs
    organized_root: /home/user/xtb_outputs
  paths:
    xtb_executable: /home/user/bin/xtb-dist/bin/xtb

crest:
  runtime:
    allowed_root: /home/user/crest_runs
    organized_root: /home/user/crest_outputs
  paths:
    crest_executable: /home/user/bin/crest/crest
```

Notes:

- Use Linux paths only.
- `default_max_retries: 2` means `1 initial + 2 retries = 3` total attempts.
- `scheduler.max_active_simulations` is the shared cap across ORCA, xTB, and CREST.
- `workflow.root` is the workflow root used by the unified CLI and workflow worker.
- The full template lives at [config/chemstack.yaml.example](config/chemstack.yaml.example).

## Unified CLI

Public queue, submission, organization, and summary commands now use `python -m chemstack.cli ...`.

```bash
# create/update config or job scaffolds
python -m chemstack.cli init orca
python -m chemstack.cli init xtb --root '/home/user/xtb_runs/job_001' --job-type path_search
python -m chemstack.cli init crest --root '/home/user/crest_runs/job_001'

# start workers
python -m chemstack.cli queue worker

# submit work
python -m chemstack.cli run-dir orca '/home/user/orca_runs/sample_rxn'
python -m chemstack.cli run-dir xtb '/home/user/xtb_runs/job_001'
python -m chemstack.cli run-dir crest '/home/user/crest_runs/job_001'
python -m chemstack.cli run-dir workflow '/home/user/workflow_runs/reaction_case'

# inspect and maintain
python -m chemstack.cli queue list --engine orca
python -m chemstack.cli queue cancel <target>
python -m chemstack.cli organize orca --root '/home/user/orca_runs' --apply
python -m chemstack.cli organize xtb --root '/home/user/xtb_runs' --apply
python -m chemstack.cli organize crest --root '/home/user/crest_runs' --apply
python -m chemstack.cli summary orca --no-send
python -m chemstack.cli summary xtb <job_id_or_dir>
python -m chemstack.cli summary crest <job_id_or_dir>
```

Compatibility notes:

- `python -m chemstack.orca.cli`, `python -m chemstack.xtb.cli`, and `python -m chemstack.crest.cli` remain as thin wrappers for `queue`, `run-dir`, `init`, `organize`, and `summary`.
- ORCA-only commands that are not yet unified still live under `python -m chemstack.orca.cli`, such as `monitor` and `bot`.
- Set top-level `workflow.root` in `chemstack.yaml` when you use workflow commands or want `python -m chemstack.cli queue worker` to supervise workflow activity too.

## Runtime Notes

- `run-dir` enqueues work durably; workers perform execution.
- If no worker is running, queued jobs remain pending until one returns.
- ORCA selects the most recently modified `.inp` when execution starts.
- Completed ORCA runs write state and report files such as `run_state.json`, `run_report.json`, and `run_report.md`.
- Use the unified engine worker service in [systemd/README.md](systemd/README.md) for unattended WSL or Linux execution.

## Testing

```bash
ruff check .
mypy
pytest --cov --cov-report=term-missing -q
```
