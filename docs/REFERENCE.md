# ORCA Auto Detailed Reference

ORCA Auto is a queue-first executor for ORCA calculations. It conservatively retries failed runs, records state in each reaction directory, and organizes completed results for later review.

## 1) Project Purpose

- Work only within the configured `allowed_root`
- Select the most recently modified `*.inp` in the target directory
- Submit work durably through the queue
- Let a supervised worker execute queued jobs
- Retry conservatively on recognized failures without overwriting the original input
- Record execution status and results alongside the calculation

## 2) Runtime Model

Current intended semantics:

- Public `run-inp` enqueues new work durably
- If an already-completed output is detected, `run-inp` returns completion without relaunching ORCA
- Successful queue submission returns `status: queued`
- Public `run-inp` does not launch ORCA directly for new work
- App-managed background execution has been removed
- The queue worker is a foreground process intended to run under external supervision
- On WSL, the recommended supervisor is `systemd`

Operational consequences:

- Closing the submission terminal after `status: queued` is safe
- If the worker is down, the job remains in `queue.json` until the worker returns
- Worker stop/start is managed by `systemctl` or the terminal that owns the foreground worker

## 3) Directory Structure

```text
~/orca_auto
  config/orca_auto.yaml
  bin/orca_auto
  core/
    cli.py                 # CLI argument parsing and command routing
    commands/
      _helpers.py          # Shared utilities (validation, formatting, config paths)
      run_inp.py           # Public queue submission command
      run_job.py           # Internal worker-owned execution path
      organize.py          # organize command
      queue.py             # queue command handlers
    config.py              # Configuration loading and dataclasses
    config_validation.py   # Configuration validation/normalization
    queue_store.py         # queue.json / queue.lock management
    queue_worker.py        # Foreground queue worker loop
    state_store.py         # State persistence, atomic writes, run lock
    attempt_engine.py      # Retry loop orchestration
    ...                    # Other domain modules
  systemd/
    orca-auto-queue-worker@.service
  scripts/*.sh / *.py
  tests/*.py
```

## 4) Required Environment

- Linux (WSL2 or native Linux)
- Access to an ORCA Linux binary path such as `/opt/orca/orca`
- ORCA runtime dependencies such as OpenMPI and BLAS/LAPACK
- Python 3.10+
- An input root on a Linux filesystem

## 5) Installation and Initial Setup

```bash
cd ~/orca_auto
bash scripts/bootstrap_wsl.sh
```

`bootstrap_wsl.sh`:

- Prepares `.venv`
- Installs Python dependencies
- Seeds `config/orca_auto.yaml` if missing

Within the repository, use `./bin/orca_auto`. An installed `orca_auto` entry point is intended to expose the same public CLI semantics.

## 6) Configuration File

Configuration file: `<project_root>/config/orca_auto.yaml`

Search order:

1. `ORCA_AUTO_CONFIG`
2. `<project_root>/config/orca_auto.yaml`
3. `~/orca_auto/config/orca_auto.yaml`

```yaml
runtime:
  allowed_root: "/path/to/orca_runs"
  organized_root: "/path/to/orca_outputs"
  default_max_retries: 2
  max_concurrent: 4
  admission_root: "/path/to/chem_admission"
  admission_max_concurrent: 4

paths:
  orca_executable: "/path/to/orca/orca"
```

Field descriptions:

- `runtime.allowed_root`: Root directory permitted for execution
- `runtime.organized_root`: Root for organized outputs
- `runtime.default_max_retries`: Maximum retry count after the initial attempt
- `runtime.max_concurrent`: Local worker fill limit for this queue root
- `runtime.admission_root`: Shared admission root for machine-wide slot coordination
- `runtime.admission_max_concurrent`: Shared machine-wide active-run cap for `admission_root`
- `paths.orca_executable`: ORCA executable path

Notes:

- `default_max_retries=2` means `1 initial + 2 retries = 3 total attempts`
- Windows-style paths such as `C:\...` and `/mnt/c/...` are not supported in config

## 7) CLI Usage

### 7.1 `run-inp`

```bash
cd ~/orca_auto
./bin/orca_auto run-inp --reaction-dir '/absolute/path/to/orca_runs/Int1_DMSO'
```

Successful submission example:

```text
status: queued
reaction_dir: /absolute/path/to/orca_runs/Int1_DMSO
queue_id: q_20260403_151220_ab12cd
priority: 10
worker: active
worker_pid: 12345
```

Behavior:

- Validates that `--reaction-dir` is under `allowed_root`
- Rejects duplicate active queue entries for the same directory
- Chooses the latest `*.inp` when execution actually starts
- Writes the queue entry durably before returning
- Does not start a detached ORCA process on behalf of the caller

Public options:

- `--reaction-dir` (required): Reaction directory
- `--force` (optional): Re-run even if a completed output already exists
- `--priority` (optional): Queue priority, lower values run sooner

Legacy notes:

- `--queue-only` is no longer needed because queuing is the default public behavior
- `--require-slot`, public direct execution, and app-managed background launch are removed from the intended workflow

### 7.2 `queue worker`

```bash
./bin/orca_auto queue worker
```

Behavior:

- Runs in the foreground
- Polls `queue.json` for pending jobs
- Uses `runtime.max_concurrent` as the local worker fill limit
- Enforces `runtime.admission_max_concurrent` under `runtime.admission_root`
- Executes jobs through an internal worker-owned execution path
- Updates queue status on completion or failure
- Requeues in-flight jobs during controlled shutdown

Use cases:

- Manual supervised execution in a dedicated terminal
- `systemd`-managed execution on WSL or Linux

There is no supported app-managed `--daemon` mode in the intended workflow. There is also no public `queue stop`; stop the service with `systemctl` or interrupt the foreground worker directly.

### 7.3 `queue cancel`

```bash
./bin/orca_auto queue cancel q_20260403_151220_ab12cd
./bin/orca_auto queue cancel /absolute/path/to/orca_runs/Int1_DMSO
./bin/orca_auto queue cancel all-pending
```

### 7.4 `list`

```bash
./bin/orca_auto list
./bin/orca_auto list --filter pending
./bin/orca_auto list --filter running
./bin/orca_auto list clear
```

`list` presents queue state together with run state and can clear terminal entries.

### 7.5 `organize`

```bash
./bin/orca_auto organize --root '/absolute/path/to/orca_runs'
./bin/orca_auto organize --root '/absolute/path/to/orca_runs' --apply
```

Options:

- `--reaction-dir`: Organize a single reaction directory
- `--root`: Organize from the configured root
- `--apply`: Perform actual moves
- `--rebuild-index`: Rebuild the JSONL index

## 8) WSL systemd Setup

WSL should have `systemd` enabled:

```ini
[boot]
systemd=true
```

If you change `/etc/wsl.conf`, restart WSL from Windows:

```powershell
wsl --shutdown
```

This repository includes a templated unit file:

- [`systemd/orca-auto-queue-worker@.service`](/home/daehyupsohn/orca_auto/systemd/orca-auto-queue-worker@.service)

Recommended install flow:

```bash
cd ~/orca_auto
sudo cp systemd/orca-auto-queue-worker@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now "orca-auto-queue-worker@$(whoami)"
systemctl status "orca-auto-queue-worker@$(whoami)"
journalctl -u "orca-auto-queue-worker@$(whoami)" -f
```

Assumptions of the template:

- Repository path: `/home/<user>/orca_auto`
- Config path: `/home/<user>/orca_auto/config/orca_auto.yaml`

If your paths differ, edit the copied unit before enabling it.

## 9) Completion Determination Rules

The mode is determined from the input route line (`! ...`).

- TS mode: Contains `OptTS` or `NEB-TS`
- Opt mode: Everything else

TS mode completion:

- `****ORCA TERMINATED NORMALLY****` exists
- Exactly 1 imaginary frequency is present
- If the route contains `IRC`, the IRC marker is also required

Opt mode completion:

- `****ORCA TERMINATED NORMALLY****` exists

## 10) Failure Classification and Automatic Recovery

Representative statuses:

- `completed`
- `error_scf`
- `error_scfgrad_abort`
- `error_multiplicity_impossible`
- `error_disk_io`
- `ts_not_found`
- `incomplete`
- `unknown_failure`

Retry modification order:

1. Add `TightSCF SlowConv` plus `%scf MaxIter 300`
2. Add `%geom Calc_Hess true`, `Recalc_Hess 5`, `MaxIter 300`
3. Reuse the last conservative retry recipe if more retries are allowed

Geometry restart rules:

- Prefer the previous attempt's matching `*.xyz`
- Fall back to the most recent `*_trj.xyz` or `.xyz`
- Keep the original geometry block if no restart geometry is available

Principles:

- Original charge and multiplicity are never changed automatically
- Original `.inp` is preserved
- Retry inputs are generated as `<name>.retryNN.inp`

## 11) Output Files

Generated in the reaction directory:

- `<stem>.out`, `<stem>.retryNN.out`
- `run_state.json`
- `run_report.json`
- `run_report.md`
- `organized_ref.json` after organize leaves a stub in the original run directory

Important `run_state.json` fields:

- `job_id`
- `run_id`
- `reaction_dir`
- `selected_inp`
- `max_retries`
- `status`
- `attempts[]`
- `final_result`

Important `attempts[]` fields:

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

Important `run_report.json` fields:

- `job_id`
- `run_id`
- `reaction_dir`
- `selected_inp`
- `status`
- `attempt_count`
- `max_retries`
- `attempts[]`
- `final_result`

## 11.1) Downstream Contract Freeze

The migration baseline assumes the following ORCA-facing compatibility contract
remains readable by downstream tooling such as `chem_workflow_mcp`.

Queue entry fields currently consumed downstream from `queue.json`:

- `queue_id`
- `task_id`
- `run_id`
- `reaction_dir`
- `status`
- `cancel_requested`
- `resource_request`
- `resource_actual`

Tracked job-location fields currently consumed downstream from
`job_locations.json`:

- `job_id`
- `app_name`
- `job_type`
- `status`
- `original_run_dir`
- `molecule_key`
- `selected_input_xyz`
- `organized_output_dir`
- `latest_known_path`
- `resource_request`
- `resource_actual`

Organize stub fields currently consumed downstream from `organized_ref.json`:

- `job_id`
- `run_id`
- `original_run_dir`
- `organized_output_dir`
- `selected_inp`
- `selected_input_xyz`
- `status`
- `job_type`
- `molecule_key`
- `resource_request`
- `resource_actual`

The normalized ORCA contract exposed downstream should continue to provide at
least these fields:

- `run_id`
- `status`
- `reason`
- `state_status`
- `reaction_dir`
- `latest_known_path`
- `organized_output_dir`
- `optimized_xyz_path`
- `queue_id`
- `queue_status`
- `cancel_requested`
- `selected_inp`
- `selected_input_xyz`
- `analyzer_status`
- `completed_at`
- `last_out_path`
- `run_state_path`
- `report_json_path`
- `report_md_path`
- `attempt_count`
- `max_retries`
- `attempts`
- `final_result`
- `resource_request`
- `resource_actual`

## 12) Recommended Workflow

1. Ensure the worker service is active, or start `queue worker` in a dedicated terminal
2. Submit with `run-inp`
3. Confirm `status: queued`
4. Close the submission terminal if desired
5. Monitor with `list` or `journalctl`
6. Review `run_report.md` after completion
7. Use `--force` only when a deliberate rerun is needed

## 13) Frequently Encountered Issues

1. `Reaction directory must be under allowed root`
- Cause: `--reaction-dir` is outside `allowed_root`
- Action: Check `allowed_root` in `config/orca_auto.yaml`

2. `Reaction directory not found`
- Cause: Path string or quoting problem
- Action: Use an absolute path and quote it if needed

3. `State file not found`
- Cause: No job has executed in that directory yet
- Action: Submit with `run-inp` and let the worker pick it up

4. `worker: inactive`
- Cause: The queue submission succeeded, but no worker is running
- Action: Start or restore the worker; the queued job remains durable

5. `error_multiplicity_impossible`
- Cause: Electron count and multiplicity mismatch
- Action: Manually adjust the input, because ORCA Auto does not rewrite charge or multiplicity

## 14) Testing

```bash
cd ~/orca_auto
pytest -q
```
