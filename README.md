# ORCA Auto — User Guide

[![CI](https://github.com/dhsohn/orca_auto/actions/workflows/ci.yml/badge.svg)](https://github.com/dhsohn/orca_auto/actions/workflows/ci.yml)

> A Python CLI that automates failure analysis, input modification, retry, state recording, and result reporting for ORCA quantum chemistry calculations.

---

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Basic Usage](#basic-usage)
5. [Task Queue](#task-queue)
6. [Failure Classification and Automatic Retry](#failure-classification-and-automatic-retry)
7. [Result Organization](#result-organization)
8. [DFT Monitoring](#dft-monitoring)
9. [Telegram Integration](#telegram-integration)
10. [Cron Automation](#cron-automation)
11. [Command Reference](#command-reference)
12. [Troubleshooting](#troubleshooting)
13. [Project Structure](#project-structure)
14. [Testing](#testing)

---

## Overview

ORCA calculations can run for hours or days before stopping due to `SCF NOT CONVERGED`, geometry non-convergence, unmet TS criteria, and more. When a failure occurs, the manual burden includes:

- Tracking which input file was used for the run
- Reading the output file and classifying the failure cause
- Conservatively modifying the original input without breaking it
- Consistently managing retry artifacts and final results
- Handling duplicate runs, mid-run interruptions, and resume scenarios

**ORCA Auto** automates all of this — from single calculation execution to batch processing (queue), result organization, monitoring, and Telegram notifications.

### Design Principles

- **Explicit configuration**: No silent personal defaults assumed
- **Separation of concerns**: Runner, analyzer, retry engine, state store, and organizer are each independent
- **Operational safety first**: Lock files, atomic writes, stale lock recovery, and resume detection
- **Conservative recovery**: Never overwrites the original `.inp`; only generates retry inputs

---

## Installation

### Requirements

- Python 3.10+ (tested with 3.11, 3.12, 3.13)
- Linux (WSL2 or native Linux)
- ORCA binary (absolute Linux path required)
- ORCA dependencies: OpenMPI, BLAS/LAPACK, etc.

### Setup

```bash
cd ~/orca_auto
bash scripts/bootstrap_wsl.sh
```

`bootstrap_wsl.sh` performs the following:
- Creates a Python virtual environment (`.venv`)
- Installs dependencies (`requirements.txt`)
- Seeds `config/orca_auto.yaml` from the example template if not present

---

## Configuration

### Interactive Setup

```bash
./bin/orca_auto init
```

You will be prompted for the following:

| Field | Description | Required |
|-------|-------------|----------|
| `paths.orca_executable` | Absolute path to the ORCA executable | Yes |
| `runtime.allowed_root` | Root directory for calculation input directories | Yes |
| `runtime.organized_root` | Directory for organized results (default: `orca_outputs` sibling to `allowed_root`) | No |
| `runtime.default_max_retries` | Maximum number of retries (default: 2) | No |
| `telegram.bot_token` | Telegram bot token | No |
| `telegram.chat_id` | Telegram chat ID | No |

### Configuration File Format

Location: `config/orca_auto.yaml`

```yaml
runtime:
  allowed_root: "/home/user/orca_runs"
  organized_root: "/home/user/orca_outputs"
  default_max_retries: 2

paths:
  orca_executable: "/opt/orca/orca"

telegram:
  bot_token: ""
  chat_id: ""
```

Config file search order:
1. Environment variable `ORCA_AUTO_CONFIG`
2. `<project_root>/config/orca_auto.yaml`
3. `~/orca_auto/config/orca_auto.yaml`

> **Note**: `default_max_retries=2` refers to the number of retries. Total executions = `1 initial + 2 retries = 3 maximum`.

> **Warning**: Windows paths (`C:\...`, `/mnt/c/...`) are not supported.

---

## Basic Usage

### Running a Single Calculation

```bash
# Default execution (background)
./bin/orca_auto run-inp --reaction-dir '/home/user/orca_runs/sample_rxn'

# Foreground execution
./bin/orca_auto run-inp --reaction-dir '/home/user/orca_runs/sample_rxn' --foreground

# Force re-run of a completed calculation
./bin/orca_auto run-inp --reaction-dir '/home/user/orca_runs/sample_rxn' --force
```

`run-inp` automatically selects the **most recently modified `.inp` file** in the directory.

By default it runs in the background, printing `status`, `pid`, and `log` path before returning immediately.
Set `ORCA_AUTO_RUN_INP_BACKGROUND=0` to change the default to foreground.

### Generated Files

The following files are created in each calculation directory:

| File | Description |
|------|-------------|
| `run_state.json` | Execution state, attempt list, final result |
| `run_report.json` | Structured result report |
| `run_report.md` | Human-readable result summary |
| `*.retryNN.inp` | Auto-generated retry input files |
| `*.retryNN.out` | Output files from retry executions |

### Example Output

```text
$ ./bin/orca_auto run-inp --reaction-dir '/home/user/orca_runs/sample_rxn' --foreground
status: completed
reaction_dir: /home/user/orca_runs/sample_rxn
selected_inp: /home/user/orca_runs/sample_rxn/rxn.inp
attempt_count: 2
reason: normal_termination
run_state: /home/user/orca_runs/sample_rxn/run_state.json
report_json: /home/user/orca_runs/sample_rxn/run_report.json
report_md: /home/user/orca_runs/sample_rxn/run_report.md
```

---

## Task Queue

The queue system provides **batch processing** for running multiple calculations sequentially or concurrently. It supports priority-based ordering, concurrency limits, cancellation, and daemon mode.

### Adding Jobs to the Queue

```bash
# Add with default priority (10)
./bin/orca_auto queue add --reaction-dir '/home/user/orca_runs/rxn_001'

# Add with higher priority (lower number = higher priority)
./bin/orca_auto queue add --reaction-dir '/home/user/orca_runs/rxn_002' --priority 1

# Re-enqueue a completed/failed job intentionally
./bin/orca_auto queue add --reaction-dir '/home/user/orca_runs/rxn_001' --force
```

- Priority: **lower number** = runs first (default: 10)
- Duplicate entries for the same directory are rejected
- Use `--force` to re-enqueue completed/failed jobs

### Viewing Simulation Status

The `list` command shows a unified view of all simulations — both queue-managed and standalone runs.

```bash
# Full listing (queue + standalone)
./bin/orca_auto list

# Filter by status
./bin/orca_auto list --filter pending
./bin/orca_auto list --filter running
./bin/orca_auto list --filter completed
./bin/orca_auto list --filter failed
./bin/orca_auto list --filter cancelled
```

Example output:

```text
Simulations: 3 total (1 running, 1 pending, 1 completed)

  ▶  q_20260310_120000_abc123  running    1   rxn_002  1h 23m  opt.inp  2
  ⏳ q_20260310_120100_def456  pending    10  rxn_001  5m      -        -
  ✅ run_rxn_003               completed  -   rxn_003  2h 10m  rxn.inp  1
```

### Starting the Queue Worker

The worker is a process that picks up pending jobs from the queue and executes them.

```bash
# Run worker in foreground (default: 4 concurrent jobs)
./bin/orca_auto queue worker

# Specify concurrency limit
./bin/orca_auto queue worker --max-concurrent 2

# Run as a background daemon
./bin/orca_auto queue worker --daemon
```

Worker behavior:
- Periodically polls the queue for `pending` jobs
- Limits concurrent executions via `--max-concurrent` (default: 4)
- Each job runs as an `orca_auto run-inp --foreground` subprocess
- Checks exit codes upon completion and updates job status
- Supports graceful shutdown via `SIGTERM` / `SIGINT`

### Stopping the Worker

```bash
./bin/orca_auto queue stop
```

### Cancelling Jobs

```bash
# Cancel by queue ID
./bin/orca_auto queue cancel q-abc123

# Cancel by reaction directory
./bin/orca_auto queue cancel /home/user/orca_runs/rxn_001

# Cancel all pending jobs
./bin/orca_auto queue cancel all-pending
```

### Clearing Completed Entries

```bash
# Remove completed, failed, and cancelled entries from the list
./bin/orca_auto list clear
```

This removes terminal queue entries and run state files for completed/failed simulations.

### Queue Workflow Example

```bash
# 1. Add multiple calculations to the queue
./bin/orca_auto queue add --reaction-dir '/home/user/orca_runs/rxn_A' --priority 1
./bin/orca_auto queue add --reaction-dir '/home/user/orca_runs/rxn_B' --priority 5
./bin/orca_auto queue add --reaction-dir '/home/user/orca_runs/rxn_C'

# 2. Start the worker as a daemon
./bin/orca_auto queue worker --daemon --max-concurrent 2

# 3. Check progress
./bin/orca_auto list

# 4. Cancel a job if needed
./bin/orca_auto queue cancel rxn_C

# 5. Clean up after all jobs complete
./bin/orca_auto list clear

# 6. Stop the worker
./bin/orca_auto queue stop
```

Queue data is persisted in `{allowed_root}/queue.json` and protected by a `queue.lock` file for concurrent access safety.

---

## Failure Classification and Automatic Retry

### Failure Classification

Output files (`.out`) are analyzed and classified into the following statuses:

| Status | Description |
|--------|-------------|
| `completed` | Successfully completed |
| `error_scf` | SCF convergence failure |
| `error_scfgrad_abort` | SCF gradient abort |
| `error_multiplicity_impossible` | Electron count / multiplicity mismatch |
| `error_disk_io` | Disk I/O error |
| `error_memory` | Out of memory |
| `geom_not_converged` | Geometry optimization did not converge |
| `ts_not_found` | Transition state not found |
| `incomplete` | Abnormal termination |

### TS Mode Completion Rules

When the input route line (`! ...`) contains `OptTS` or `NEB-TS`, TS mode is activated:

- `****ORCA TERMINATED NORMALLY****` must be present
- Exactly **1 imaginary frequency** required
- If the route contains `IRC`, the IRC completion marker is also required

### Automatic Retry Recipes

Conservative modifications are applied progressively at each retry step:

| Step | Modifications Applied |
|------|----------------------|
| Step 1 | Add `TightSCF SlowConv` to route + `%scf MaxIter 300` |
| Step 2 | `%geom Calc_Hess true`, `Recalc_Hess 5`, `MaxIter 300` |
| Step 3 | Increase memory + relax convergence (`LooseOpt`) |
| Step 4 | Hessian + memory + relaxed convergence combined |

### Geometry Restart

At each retry:
1. Finds the previous attempt's `.xyz` file and replaces the geometry block with `* xyzfile ...`
2. Falls back to the most recent `*_trj.xyz` in the directory if the direct match is not found
3. Keeps the original geometry block if no fallback candidates exist

Principles:
- Original `charge/multiplicity` is never changed
- Original `.inp` file is preserved
- Retry filenames follow the pattern `<name>.retry01.inp`, `<name>.retry02.inp`, ...

### Recovery Scenario Example

```text
Run rxn.inp
  -> Detect SCF failure in rxn.out
  -> Generate rxn.retry01.inp
     - Add TightSCF / SlowConv to route
     - Apply %scf MaxIter 300
     - Geometry restart with latest xyz
  -> rxn.retry01.out terminates normally
  -> Generate run_report.md / run_report.json
  -> Reflected in list as completed, attempts=2
```

---

## Result Organization

Moves completed calculation results to `organized_root` and maintains an index.

```bash
# Dry run (default — preview without moving)
./bin/orca_auto organize --root /home/user/orca_runs

# Apply actual moves
./bin/orca_auto organize --root /home/user/orca_runs --apply

# Organize a single directory
./bin/orca_auto organize --reaction-dir /home/user/orca_runs/sample_rxn --apply

# Rebuild the index
./bin/orca_auto organize --root /home/user/orca_runs --rebuild-index
```

- `--root` must exactly match `runtime.allowed_root` in the configuration
- Default is dry-run; specify `--apply` for actual moves
- Moved files are tracked in a JSONL index

---

## DFT Monitoring

Scans the filesystem to automatically detect and index newly discovered ORCA results.

```text
Filesystem (.out)
  -> dft_discovery  (file discovery)
  -> orca_parser    (result parsing)
  -> dft_index      (SQLite storage)
  -> dft_monitor    (change detection)
  -> Telegram notification
```

```bash
# Manual execution
./bin/orca_auto monitor
```

- Sends Telegram alerts only for newly discovered DFT results
- Reports parse failures with error details
- State is tracked in `.dft_monitor_state.json`

---

## Telegram Integration

### Enabling Notifications

Set `bot_token` and `chat_id` in `config/orca_auto.yaml` to activate notifications automatically.

### Notification Types

| Event | Content |
|-------|---------|
| Run started | Reaction directory, selected input, attempt info |
| Retry scheduled | Failure reason, patch actions, next input file |
| Run completed/failed | Final status, attempt count, completion reason |
| DFT discovery | Formula, method, energy of new results |
| Parse failure | Error messages from monitor scans |
| State summary | Active runs, blockers, progress |

### Telegram Bot

```bash
# Start the bot
./bin/orca_auto bot

# Or manage via script
bash scripts/start_bot.sh start
bash scripts/start_bot.sh restart
bash scripts/start_bot.sh stop
```

Bot commands:

| Command | Description |
|---------|-------------|
| `/list` | Full simulation list |
| `/list running` | Running jobs only |
| `/list completed` | Completed jobs only |
| `/list failed` | Failed jobs only |
| `/help` | Help |

### Periodic Summary Reports

```bash
# Send via Telegram
./bin/orca_auto summary

# Print without sending
./bin/orca_auto summary --no-send
```

Shows active run status, progress info (cycle count, energy, elapsed time, ETA), and jobs needing attention.

---

## Cron Automation

```bash
bash scripts/install_cron.sh
```

Installed schedules:

| Job | Schedule | Description |
|-----|----------|-------------|
| `dft_summary` | `0 9,21 * * *` | Twice daily — active runs and blocker digest |
| `dft_monitor` | `0 * * * *` | Hourly — scan for new DFT results and send alerts |
| `organize` | `0 0 * * 6` | Weekly (Saturday) — organize completed results |

### Role Split

| Command | Role |
|---------|------|
| `run-inp` | Immediate alerts — start, retry scheduling, completion, failure |
| `monitor` | Discovery alerts — newly found results from filesystem scans |
| `summary` | State digest — active jobs and attention-needed items (completed history excluded) |

---

## Command Reference

### Main Commands

| Command | Description |
|---------|-------------|
| `init` | Interactively create or update the config file |
| `run-inp` | Select the latest `.inp`, then run/recover/retry |
| `list` | Unified view of all simulations (queue + standalone) |
| `list clear` | Remove completed/failed/cancelled entries |
| `organize` | Move completed results to `organized_root` and index them |
| `monitor` | Send Telegram alerts for newly discovered DFT results |
| `summary` | Send a Telegram digest of active run state |
| `bot` | Start the Telegram long-polling bot |
| `queue` | Manage the task queue (see subcommands below) |

### Queue Subcommands

| Subcommand | Description |
|------------|-------------|
| `queue add` | Add a calculation directory to the queue |
| `queue cancel` | Cancel a pending or running job |
| `queue worker` | Start the queue worker |
| `queue stop` | Stop the running worker |

### Common Options

| Option | Description |
|--------|-------------|
| `--config <path>` | Use a non-default config file |
| `--verbose`, `-v` | Enable debug logging |
| `--log-file <path>` | Write logs to file (10MB x 5 rotation) |
| `--force` | Force re-run of completed calculations |
| `--foreground` | Run in the foreground |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `ORCA_AUTO_CONFIG` | Override config file path |
| `ORCA_AUTO_RUN_INP_BACKGROUND` | Default run-inp execution mode (`1`=background, `0`=foreground) |
| `ORCA_AUTO_LOG_DIR` | Override log directory |

---

## Troubleshooting

### `Reaction directory must be under allowed root`

The `--reaction-dir` path is not under `allowed_root`.
Check `allowed_root` in `config/orca_auto.yaml`.

### `Reaction directory not found`

Path string or quoting issue. Wrap the path in single quotes:

```bash
./bin/orca_auto run-inp --reaction-dir '/home/user/orca_runs/my_case'
```

### `State file not found`

`run-inp` has not been executed in that directory yet. Run `run-inp` first.

### `error_multiplicity_impossible`

The electron count and multiplicity combination is invalid. This tool uses a conservative policy and does not automatically change charge/multiplicity — manually edit the input file and re-run.

### Duplicate queue entry rejected

The same `reaction_dir` already exists in the queue with an active status (pending/running). Use `--force` to re-enqueue after completion or failure.

---

## Project Structure

```text
orca_auto/
├── bin/orca_auto              # Local .venv-first entry point shim
├── core/                      # Main application logic
│   ├── launcher.py            # Background/foreground execution handler
│   ├── cli.py                 # CLI argument parsing and command routing
│   ├── config.py              # YAML configuration loading
│   ├── commands/              # CLI command implementations
│   │   ├── run_inp.py         # run-inp command
│   │   ├── list_runs.py       # list command
│   │   ├── queue.py           # queue command
│   │   ├── organize.py        # organize command
│   │   ├── monitor.py         # monitor command
│   │   └── summary.py         # summary command
│   ├── orca_runner.py         # ORCA subprocess execution
│   ├── out_analyzer.py        # Output file analysis
│   ├── orca_parser.py         # ORCA output parsing (energies, frequencies, etc.)
│   ├── attempt_engine.py      # Retry loop orchestration
│   ├── inp_rewriter.py        # Retry input modification
│   ├── state_store.py         # State persistence, atomic writes, locks
│   ├── queue_store.py         # Persistent task queue
│   ├── queue_worker.py        # Queue worker daemon
│   ├── result_organizer.py    # Completed result relocation
│   ├── dft_index.py           # SQLite DFT index
│   ├── dft_discovery.py       # ORCA output file discovery
│   ├── dft_monitor.py         # Auto-discovery monitoring
│   ├── telegram_bot.py        # Telegram long-polling bot
│   └── telegram_notifier.py   # Telegram notification sender
├── config/                    # Configuration files
├── scripts/                   # Installation and automation scripts
├── tests/                     # Test code
└── docs/REFERENCE.md          # Detailed behavioral reference
```

---

## Testing

```bash
# Linting
ruff check .

# Type checking
mypy

# Run tests (80% coverage gate)
pytest --cov --cov-report=term-missing -q
```

Verified on GitHub Actions with Python 3.11, 3.12, 3.13 matrix.

---

For detailed behavioral rules and completion determination logic, see [REFERENCE.md](docs/REFERENCE.md).
