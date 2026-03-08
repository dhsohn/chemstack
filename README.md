# ORCA Single-Input Recovery Runner

[![CI](https://github.com/dhsohn/orca_auto/actions/workflows/ci.yml/badge.svg)](https://github.com/dhsohn/orca_auto/actions/workflows/ci.yml)

> A Python CLI that automates failure analysis, input modification, retry, state recording, and result reporting so that humans don't have to deal with ORCA calculation failures in the middle of the night.

## What problem does it solve

ORCA calculations can run for hours or days before stopping due to reasons like `SCF NOT CONVERGED`, geometry issues, or unmet TS criteria. The real problem is not the failure itself, but what comes after.

- You need to track which input was used for the run.
- You need to read the output file and classify the failure cause.
- You need to conservatively modify the original input without breaking it.
- You need to consistently keep retry artifacts and final results in the same directory.
- Since jobs take a long time, duplicate runs, mid-run interruptions, and resume scenarios must also be safe.

This project was created to reduce those operational burdens.

## Why is it hard

- ORCA exit codes are not a sufficient signal. You need to read the output text to determine the actual failure cause.
- For TS calculations, `terminated normally` alone does not mean completion. You must also verify the number of imaginary frequencies and IRC conditions.
- Retry automation is not a simple loop — it requires preserving the original input, generating retry inputs, geometry restart, and state persistence to all work together.
- Because calculations take a long time, without locks, atomic state persistence, resume policies, and background execution UX, operations easily get tangled.

## What was built

- Selects the latest user-authored `.inp` under `allowed_root` and runs it
- Classifies failure causes by analyzing `.out` files
- Generates `*.retryNN.inp` with conservative retry recipes and automatically retries
- Produces `run_state.json`, `run_report.json`, `run_report.md`
- Skips if a completed `.out` already exists; re-runs with `--force`
- A production-ready CLI including `list`, `status`, `organize`, and a Telegram bot

## Design decisions

- Configuration is explicit: no longer silently assumes personal defaults like `~/orca_runs` or `~/opt/orca/orca`.
- Separation of concerns: runner, analyzer, retry engine, state store, and organizer are separated.
- Operational safety first: includes lock files, atomic writes, stale lock recovery, and resume determination.
- Recovery is conservative: does not overwrite the original `.inp`; only generates retry inputs.

## Recovery scenario example

```text
Run rxn.inp
  -> Detect SCF failure in rxn.out
  -> Generate rxn.retry01.inp
     - Add TightSCF / SlowConv to route
     - Apply %scf MaxIter 300
     - Geometry restart with the latest xyz
  -> rxn.retry01.out terminates normally
  -> Generate run_report.md / run_report.json
  -> Reflected in list as completed, attempts=2
```

## Verification basis

- Verified with Python `3.11`, `3.12`, `3.13` matrix on GitHub Actions
- Quality gates: `ruff`, `mypy`, `pytest --cov`
- Coverage gate: `80%`
- Unit tests: parser, retry rules, state/lock, organize/index, Telegram handlers
- Integration tests: verify the `run-inp -> retry -> report generation -> list reflection` flow with a fake ORCA executable

## Quick start

### 1) Installation

```bash
cd ~/orca_auto
bash scripts/bootstrap_wsl.sh
```

`bootstrap_wsl.sh` prepares the `.venv` and copies template configuration files.

### 2) Write the configuration file

`orca_auto` will immediately exit with a friendly error if the configuration file is missing or template placeholders remain.

```bash
cp config/orca_auto.yaml.example config/orca_auto.yaml
```

```yaml
runtime:
  allowed_root: "/absolute/path/to/orca_runs"
  organized_root: "/absolute/path/to/orca_outputs"
  default_max_retries: 2

paths:
  orca_executable: "/absolute/path/to/orca/orca"

telegram:
  bot_token: ""
  chat_id: ""
```

Notes:

- `runtime.allowed_root` and `paths.orca_executable` are required.
- If `runtime.organized_root` is omitted, the default is `orca_outputs` next to `allowed_root`.
- Windows legacy paths (`C:\...`, `/mnt/c/...`) are not supported.

### 3) Run a calculation

```bash
./bin/orca_auto run-inp --reaction-dir '/absolute/path/to/orca_runs/sample_rxn'
```

The default is background execution. To run in the foreground:

```bash
./bin/orca_auto run-inp --reaction-dir '/absolute/path/to/orca_runs/sample_rxn' --foreground
```

### 4) Check results

```bash
./bin/orca_auto status --reaction-dir '/absolute/path/to/orca_runs/sample_rxn'
./bin/orca_auto list
cat /absolute/path/to/orca_runs/sample_rxn/run_report.md
```

## Demo output example

```text
$ ./bin/orca_auto run-inp --reaction-dir '/absolute/path/to/orca_runs/sample_rxn' --foreground
status: completed
reaction_dir: /absolute/path/to/orca_runs/sample_rxn
selected_inp: /absolute/path/to/orca_runs/sample_rxn/rxn.inp
attempt_count: 2
reason: normal_termination
run_state: /absolute/path/to/orca_runs/sample_rxn/run_state.json
report_json: /absolute/path/to/orca_runs/sample_rxn/run_report.json
report_md: /absolute/path/to/orca_runs/sample_rxn/run_report.md

$ ./bin/orca_auto list --json
[
  {
    "dir": "sample_rxn",
    "status": "completed",
    "attempts": 2,
    "inp": "rxn.inp"
  }
]
```

## Main commands

| Command | Description |
|---------|-------------|
| `run-inp` | Select the latest `.inp`, then run/recover/retry |
| `status` | Check the state of an individual reaction directory |
| `list` | Query the status of all runs under `allowed_root` |
| `organize` | Move completed calculation results under `organized_root` and index them |
| `bot` | Run the Telegram long-polling bot |

Frequently used options:

| Option | Description | Example |
|--------|-------------|---------|
| `--force` | Force re-run even if the calculation is already completed | `run-inp --force` |
| `--max-retries N` | Adjust the number of retries | `run-inp --max-retries 8` |
| `--foreground` | Run in the foreground | `run-inp --foreground` |
| `--background` | Force background execution | `run-inp --background` |
| `--json` | JSON output | `list --json` |

## Telegram bot

You can query the status from Telegram.

```bash
./bin/orca_auto bot
bash scripts/start_bot.sh restart
```

Bot commands:

| Command | Description |
|---------|-------------|
| `/list` | Full simulation list |
| `/list running` | Running jobs only |
| `/list completed` | Completed jobs only |
| `/list failed` | Failed jobs only |
| `/help` | Help |

## Result organization and indexing

To organize multiple calculation results at once:

```bash
./bin/orca_auto organize --root /absolute/path/to/orca_runs
./bin/orca_auto organize --root /absolute/path/to/orca_runs --apply
```

Notes:

- `--root` must exactly match `runtime.allowed_root` in the configuration file.
- Dry-run is the default; `--apply` must be specified for actual moves to occur.
- Targets for organization are moved under `runtime.organized_root` and a JSONL index is maintained alongside them.

## DFT monitoring layer

In addition to the single runner, this repository also includes a library layer that automatically detects and structures ORCA results.

```text
File system (.out)
  -> dft_discovery
  -> orca_parser
  -> dft_index (SQLite)
  -> dft_monitor
  -> Telegram notifier / bot
```

Main modules:

| Module | Role |
|--------|------|
| `orca_runner.py` | ORCA subprocess execution and termination handling |
| `out_analyzer.py` | Completion/failure reason determination from `.out` files |
| `attempt_engine.py` | Retry loop and final state determination |
| `state_store.py` | State persistence, atomic writes, execution locks |
| `result_organizer.py` | Moving completed runs and synchronizing state |
| `dft_monitor.py` | Automatic detection and indexing of completed results |
| `telegram_bot.py` | Long-polling command reception |

## Project structure

```text
core/
  launcher.py
  cli.py
  commands/
  orca_runner.py
  out_analyzer.py
  attempt_engine.py
  state_store.py
  result_organizer.py
  dft_discovery.py
  dft_index.py
  dft_monitor.py
  telegram_bot.py
  telegram_notifier.py
tests/
scripts/
config/
docs/
```

`./bin/orca_auto` is a thin shim that preferentially uses the local `.venv`, and internally calls the same `core.launcher` as the installed `orca_auto`.

## Running tests

```bash
ruff check .
mypy
pytest --cov --cov-report=term-missing -q
```

Detailed behavioral rules and completion determination logic are covered in [REFERENCE.md](docs/REFERENCE.md).
