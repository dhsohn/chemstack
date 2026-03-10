# ORCA Auto Detailed Reference

An executor that automatically applies conservative modifications to input files (`.inp`) and retries when `ORCA` calculations fail midway or do not meet TS criteria, and organizes the resulting output.

## 1) Project Purpose

- Runs against a single user-specified `~/orca_runs/<reaction_dir>`
- Automatically selects the most recently modified `*.inp` file within that directory
- On failure/interruption/TS not met, generates `*.retryNN.inp` for automatic retry
- Records execution status and results in the same directory

## 2) Core Behavior Summary

- Input root restriction: Only subdirectories under the configured `allowed_root` are permitted
- Target file selection: The most recently modified `*.inp` file
- Default behavior: Skips if an existing `*.out` is in completed state
- Force re-execution: With `--force`, re-executes even if a completed `*.out` exists
- No execution time limit: Waits until the ORCA process terminates normally or abnormally
- State file: `run_state.json`
- Result report: `run_report.json`, `run_report.md`

## 3) Directory Structure

```text
~/orca_auto
  config/orca_auto.yaml
  bin/orca_auto            # Local .venv-first shim (same UX as installed orca_auto)
  core/
    launcher.py            # Common user entry point (background/foreground UX)
    commands/              # CLI command handlers
      _helpers.py          # Shared utilities (validation, formatting, config paths)
      run_inp.py           # run-inp command
      organize.py          # organize command
    config.py              # Configuration loading and dataclasses
    config_validation.py   # Configuration validation/normalization functions
    lock_utils.py          # Lock file parsing/process liveness check (shared)
    state_store.py         # State persistence/atomic writes/run lock
    organize_index.py      # JSONL index management/index lock
    attempt_engine.py      # Retry loop orchestration
    ...                    # Other domain modules
  scripts/*.sh / *.py
  tests/*.py
```

## 4) Required Environment

- Linux (WSL2 or native Linux)
- Access to ORCA Linux binary path (e.g., `/opt/orca/orca`)
- ORCA dependencies: OpenMPI, BLAS/LAPACK, etc.
- Python 3.10+
- Input data root: Explicit absolute path (ext4 filesystem recommended)

## 5) Installation and Initial Setup

```bash
cd ~/orca_auto
bash scripts/bootstrap_wsl.sh
```

`bootstrap_wsl.sh` performs the following:

- Verifies ORCA Linux binary exists
- Prepares Python venv (`.venv`)
- Installs dependencies (`requirements.txt`)
- Prepares `orca_auto` for execution

Notes:

- Within the repository, you can use `./bin/orca_auto`.
- The `orca_auto` command installed via the package entry point also calls the same `core.launcher`.
- This means the default background execution of `run-inp`, the `pid`/`log` output, and the `--foreground` handling are identical across both entry points.

## 6) Configuration File

Configuration file: `<project_root>/config/orca_auto.yaml`

Default configuration path search order:

1. Environment variable `ORCA_AUTO_CONFIG`
2. Relative path from execution code `<project_root>/config/orca_auto.yaml`
3. `~/orca_auto/config/orca_auto.yaml`

```yaml
runtime:
  allowed_root: "/path/to/orca_runs"
  organized_root: "/path/to/orca_outputs"
  default_max_retries: 2

paths:
  orca_executable: "/path/to/orca/orca"

```

Field descriptions:

- `runtime.allowed_root`: Root directory permitted for execution
- `runtime.organized_root`: Root directory for organize target (defaults to `orca_outputs` alongside `allowed_root` if omitted)
- `runtime.default_max_retries`: Maximum number of retries
- `paths.orca_executable`: Path to ORCA executable

Caution:

- `default_max_retries=2` refers to the number of retries.
- The total number of executions is `initial 1 + 2 retries = maximum 3 times`.
- Windows legacy paths (`C:\...`, `/mnt/c/...`) are not supported in the configuration.
## 7) CLI Usage

### 7.1 Execution

```bash
cd ~/orca_auto
./bin/orca_auto run-inp --reaction-dir '/absolute/path/to/orca_runs/Int1_DMSO'
```

When using the installed entry point:

```bash
orca_auto run-inp --reaction-dir '/absolute/path/to/orca_runs/Int1_DMSO'
```

Default behavior:

- `run-inp` runs in the background by default.
- Immediately after execution, it prints `status`, `pid`, and `log` path, then exits.
- If foreground execution is needed, add `--foreground`.
- To change the overall default to foreground, use the environment variable `ORCA_AUTO_RUN_INP_BACKGROUND=0`.

Options:

- `--reaction-dir` (required): Reaction directory
- The retry count is configured only through `runtime.default_max_retries` in `orca_auto.yaml`
- `--force` (optional): Force re-execution even if a completed `*.out` exists
- `--foreground` (optional): Run `run-inp` in the foreground

### 7.2 Result Organization

```bash
./bin/orca_auto organize --root '/absolute/path/to/orca_runs'
./bin/orca_auto organize --root '/absolute/path/to/orca_runs' --apply
```

Options:

- `--reaction-dir`: Organize a single reaction directory
- `--root`: Root scan organization (must exactly match `allowed_root`)
- `--root` scan recursively searches subdirectories and collects all completed runs that have a `run_state.json`
- `--apply`: Perform actual move (default is dry-run)
- `--rebuild-index`: Rebuild index

## 8) Completion Determination Rules

The mode is automatically determined based on the input route line (`! ...`).

- TS mode: Contains `OptTS` or `NEB-TS`
- Opt mode: Everything else

TS mode completion conditions:

- `****ORCA TERMINATED NORMALLY****` exists
- Exactly 1 imaginary frequency (`-xxx cm**-1`)
- If the route line contains `IRC`, the IRC marker is also required

Opt mode completion conditions:

- `****ORCA TERMINATED NORMALLY****` exists

## 9) Failure Classification and Automatic Recovery

Representative statuses:

- `completed`
- `error_scf`
- `error_scfgrad_abort`
- `error_multiplicity_impossible`
- `error_disk_io`
- `ts_not_found`
- `incomplete`
- `unknown_failure`

Input file modification order during retry:

1. Add `TightSCF SlowConv` to route + `%scf MaxIter 300`
2. `%geom Calc_Hess true`, `Recalc_Hess 5`, `MaxIter 300`
3. No additional recipe (reuses step 2 recipe)

Common geometry restart rules:

- On each retry, finds a `*.xyz` file with the same stem as the previous attempt's input and replaces with `* xyzfile ...`
- Example: When generating `foo.retry02.inp` from `foo.retry01.inp`, uses `foo.retry01.xyz`
- If the previous `*.xyz` does not exist, searches the directory for the most recent `*_trj.xyz/xyz` as a fallback
- If no fallback candidates exist, skips geometry replacement and keeps the original geometry block

Principles:

- The original `charge/multiplicity` is never changed.
- The original input file is preserved.
- Retry filenames follow the pattern `<name>.retry01.inp`, `<name>.retry02.inp`, ... (up to the `max_retries` value)

## 10) Output File Description

Generated in the execution target directory (`<allowed_root>/<reaction_dir>`):

- `<stem>.out`, `<stem>.retryNN.out`
- `run_state.json`
- `run_report.json`
- `run_report.md`

`run_state.json` key fields:

- `run_id`
- `reaction_dir`
- `selected_inp`
- `status`
- `attempts[]`
- `final_result`

`attempts[]` entries:

- `index`
- `inp_path`
- `out_path`
- `return_code`
- `analyzer_status`
- `analyzer_reason`
- `markers`
- `patch_actions`
- `started_at`
- `ended_at`

## 11) Operations Guide

- It is recommended to clearly manage one input file per directory.
- If multiple `*.inp` files exist, the most recently modified file is selected.
- Use `--force` if a forced restart is needed.
- Interrupting with `Ctrl+C` will also attempt to terminate the running ORCA process tree, and the status is recorded as `interrupted_by_user`.
## 12) Frequently Encountered Issues

1. `Reaction directory must be under allowed root`
- Cause: `--reaction-dir` is outside `allowed_root`
- Action: Check `allowed_root` in `config/orca_auto.yaml`

2. `Reaction directory not found`
- Cause: Path string/quoting issue
- Action: Wrap the path in single quotes

Example:

```bash
./bin/orca_auto run-inp --reaction-dir '/home/daehyupsohn/orca_runs/my_case'
```

3. `State file not found`
- Cause: `run-inp` has not been executed in that directory yet
- Action: Run `run-inp` first

4. `error_multiplicity_impossible`
- Cause: Electron count and multiplicity combination mismatch
- Action: This tool uses a conservative policy and does not automatically change charge/multiplicity, so manually edit the input and re-execute

## 13) Migration Utilities

Scripts available for Linux transition:

- `scripts/preflight_check.sh`: Cutover pre-check (processes, locks, state, disk)
- `scripts/audit_input_path_literals.py`: Detects Windows paths inside `.inp` files
- `scripts/validate_runtime_config.py`: Configuration validity verification

Notification commands:

- `run-inp`: Immediate Telegram alerts for run start, retry scheduling, and terminal completion/failure when Telegram is configured.
- `monitor`: Scan-oriented Telegram alert. Reports only newly discovered DFT results and parse failures from periodic filesystem scans.
- `summary`: Digest-oriented Telegram report. Shows active runs and current blockers at the reporting time; completed history is omitted.

## 14) Testing

```bash
cd ~/orca_auto
pytest -q
```

## 15) Recommended Workflow

1. Prepare input directory (`~/orca_runs/<case>`)
2. Run `run-inp`
3. Review final summary with `run_report.md`
4. Re-execute with `--force` if needed
