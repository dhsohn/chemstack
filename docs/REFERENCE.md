# ChemStack ORCA Detailed Reference

ChemStack ORCA is a queue-first executor for ORCA calculations. It conservatively retries failed runs, records state in each job directory, and organizes completed results for later review.

Current developer-facing package rule:

- The canonical implementation lives in `chemstack.orca`
- Shared infrastructure lives in `chemstack.core`
- Supported imports live under `chemstack.*`

## 1) Project Purpose

- Work only within the configured `allowed_root`
- Select the most recently modified `*.inp` in the target directory
- Submit work durably through the queue
- Let a supervised worker execute queued jobs
- Retry conservatively on recognized failures without overwriting the original input
- Record execution status and results alongside the calculation

## 2) Runtime Model

Current intended semantics:

- Public `run-dir` enqueues new work durably
- If an already-completed output is detected, `run-dir` returns completion without relaunching ORCA
- Successful queue submission returns `status: queued`
- Public `run-dir` does not launch ORCA directly for new work
- App-managed background execution has been removed
- The queue worker is a foreground process intended to run under external supervision
- On WSL, the recommended supervisor is `systemd`

Operational consequences:

- Closing the submission terminal after `status: queued` is safe
- If the worker is down, the job remains in `queue.json` until the worker returns
- Worker stop/start is managed by `systemctl` or the terminal that owns the foreground worker

## 3) Directory Structure

```text
<repo_root>
  config/chemstack.yaml
  src/
    chemstack/
      core/               # Shared chemistry-platform infrastructure
      flow/               # Workflow orchestration package
      xtb/                # xTB engine package
      crest/              # CREST engine package
      orca/               # Canonical ORCA implementation
        cli.py
        commands/
        runtime/
        state.py
        tracking.py
        ...
  systemd/
    chemstack-orca-queue-worker@.service
    chemstack-xtb-queue-worker@.service
    chemstack-crest-queue-worker@.service
    chemstack-flow-workflow-worker.service
    chemstack-flow-worker.env.example
  scripts/*.sh / *.py
  tests/
    integration/
    flow/
    ...
```

## 4) Required Environment

- Linux (WSL2 or native Linux)
- Access to an ORCA Linux binary path such as `/opt/orca/orca`
- ORCA runtime dependencies such as OpenMPI and BLAS/LAPACK
- Python 3.10+
- An input root on a Linux filesystem

## 5) Installation and Initial Setup

```bash
cd <repo_root>
bash scripts/bootstrap_wsl.sh
```

`bootstrap_wsl.sh`:

- Prepares `.venv`
- Installs Python dependencies and the repository itself into `.venv`
- Seeds `config/chemstack.yaml` if missing

This reference standardizes on `python -m chemstack.orca.cli ...`.
Activate `.venv` first, or call `.venv/bin/python -m chemstack.orca.cli ...` directly.
By default, config is resolved from `CHEMSTACK_CONFIG`, then `<repo_root>/config/chemstack.yaml`, then `~/chemstack/config/chemstack.yaml`.
Add `--config <path>` only when you want to override default config discovery.

## 6) Configuration File

Configuration file: `<project_root>/config/chemstack.yaml`

Search order:

1. `CHEMSTACK_CONFIG`
2. `<project_root>/config/chemstack.yaml`
3. `~/chemstack/config/chemstack.yaml`

```yaml
resources:
  max_cores_per_task: 8
  max_memory_gb_per_task: 32

behavior:
  auto_organize_on_terminal: false

scheduler:
  max_active_simulations: 4
  admission_root: "/path/to/chem_admission"

telegram:
  bot_token: ""
  chat_id: ""

orca:
  runtime:
    allowed_root: "/path/to/orca_runs"
    organized_root: "/path/to/orca_outputs"
    default_max_retries: 2
  paths:
    orca_executable: "/path/to/orca/orca"

xtb:
  runtime:
    allowed_root: "/path/to/xtb_runs"
    organized_root: "/path/to/xtb_outputs"
  paths:
    xtb_executable: "/path/to/xtb"

crest:
  runtime:
    allowed_root: "/path/to/crest_runs"
    organized_root: "/path/to/crest_outputs"
  paths:
    crest_executable: "/path/to/crest"
```

Field descriptions for the `orca` section:

- `runtime.allowed_root`: Root directory permitted for execution
- `runtime.organized_root`: Root for organized outputs
- `runtime.default_max_retries`: Maximum retry count after the initial attempt
- `scheduler.max_active_simulations`: Shared total active-run cap across ORCA, xTB, and CREST
- `scheduler.admission_root`: Shared admission root for machine-wide slot coordination
- `paths.orca_executable`: ORCA executable path

Notes:

- `default_max_retries=2` means `1 initial + 2 retries = 3 total attempts`
- Windows-style paths such as `C:\...` and `/mnt/c/...` are not supported in config

## 7) CLI Usage

### 7.1 `run-dir`

```bash
cd <repo_root>
python -m chemstack.orca.cli run-dir '/absolute/path/to/orca_runs/Int1_DMSO'
```

Successful submission example:

```text
status: queued
job_dir: /absolute/path/to/orca_runs/Int1_DMSO
queue_id: q_20260403_151220_ab12cd
priority: 10
worker: active
worker_pid: 12345
```

Behavior:

- Validates that the job directory path is under `allowed_root`
- Rejects duplicate active queue entries for the same directory
- Chooses the latest `*.inp` when execution actually starts
- Writes the queue entry durably before returning
- Does not start a detached ORCA process on behalf of the caller

Public options:

- `<path>` (required): Job directory
- `--force` (optional): Re-run even if a completed output already exists
- `--priority` (optional): Queue priority, lower values run sooner

- `--queue-only` is no longer needed because queuing is the default public behavior
- `--require-slot`, public direct execution, and app-managed background launch are removed from the intended workflow

### 7.2 `queue worker`

```bash
python -m chemstack.orca.cli queue worker
```

Behavior:

- Runs in the foreground
- Polls `queue.json` for pending jobs
- Enforces `scheduler.max_active_simulations` under `scheduler.admission_root`
- Executes jobs through an internal worker-owned execution path
- Updates queue status on completion or failure
- Requeues in-flight jobs during controlled shutdown

Use cases:

- Manual supervised execution in a dedicated terminal
- `systemd`-managed execution on WSL or Linux

There is no supported app-managed `--daemon` mode in the intended workflow. There is also no public `queue stop`; stop the service with `systemctl` or interrupt the foreground worker directly.

### 7.3 `queue cancel`

```bash
python -m chemstack.orca.cli queue cancel q_20260403_151220_ab12cd
python -m chemstack.orca.cli queue cancel /absolute/path/to/orca_runs/Int1_DMSO
python -m chemstack.orca.cli queue cancel all-pending
```

### 7.4 `list`

```bash
python -m chemstack.orca.cli list
python -m chemstack.orca.cli list --filter pending
python -m chemstack.orca.cli list --filter running
python -m chemstack.orca.cli list clear
```

`list` presents queue state together with run state and can clear terminal entries.

### 7.5 `organize`

```bash
python -m chemstack.orca.cli organize --root '/absolute/path/to/orca_runs'
python -m chemstack.orca.cli organize --root '/absolute/path/to/orca_runs' --apply
```

Options:

- `--reaction-dir`: Organize a single job directory
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

This repository includes service assets under `systemd/`:

- [`systemd/chemstack-orca-queue-worker@.service`](/home/daehyupsohn/chemstack/systemd/chemstack-orca-queue-worker@.service)
- [`systemd/chemstack-xtb-queue-worker@.service`](/home/daehyupsohn/chemstack/systemd/chemstack-xtb-queue-worker@.service)
- [`systemd/chemstack-crest-queue-worker@.service`](/home/daehyupsohn/chemstack/systemd/chemstack-crest-queue-worker@.service)
- [`systemd/chemstack-flow-workflow-worker.service`](/home/daehyupsohn/chemstack/systemd/chemstack-flow-workflow-worker.service)
- [`systemd/chemstack-flow-worker.env.example`](/home/daehyupsohn/chemstack/systemd/chemstack-flow-worker.env.example)

Recommended engine-worker install flow:

```bash
cd <repo_root>
APP=orca   # or xtb / crest
sudo cp "systemd/chemstack-${APP}-queue-worker@.service" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now "chemstack-${APP}-queue-worker@$(whoami)"
systemctl status "chemstack-${APP}-queue-worker@$(whoami)"
journalctl -u "chemstack-${APP}-queue-worker@$(whoami)" -f
```

Assumptions of the engine templates:

- Repository path: `/home/<user>/chemstack`
- Config path: `/home/<user>/chemstack/config/chemstack.yaml`

If your paths differ, edit the copied unit before enabling it.

ORCA, xTB, and CREST workers can all be enabled together. The shared
`scheduler.max_active_simulations` setting still limits the combined number of
active simulations.

For the workflow worker:

```bash
cd <repo_root>
sudo install -d /etc/chemstack
sudo cp systemd/chemstack-flow-worker.env.example /etc/chemstack/flow-worker.env
sudo cp systemd/chemstack-flow-workflow-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now chemstack-flow-workflow-worker
systemctl status chemstack-flow-workflow-worker
journalctl -u chemstack-flow-workflow-worker -f
```

Edit `/etc/chemstack/flow-worker.env` before enabling the service and set
`CHEM_FLOW_WORKFLOW_ROOT` for your machine.

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

Generated in the job directory:

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
remains readable by downstream tooling such as `chemstack.flow`.

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
2. Submit with `run-dir`
3. Confirm `status: queued`
4. Close the submission terminal if desired
5. Monitor with `list` or `journalctl`
6. Review `run_report.md` after completion
7. Use `--force` only when a deliberate rerun is needed

## 13) Frequently Encountered Issues

1. `Job directory must be under allowed root`
- Cause: the job directory path is outside `allowed_root`
- Action: Check `allowed_root` in `config/chemstack.yaml`

2. `Job directory not found`
- Cause: Path string or quoting problem
- Action: Use an absolute path and quote it if needed

3. `State file not found`
- Cause: No job has executed in that directory yet
- Action: Submit with `run-dir` and let the worker pick it up

4. `worker: inactive`
- Cause: The queue submission succeeded, but no worker is running
- Action: Start or restore the worker; the queued job remains durable

5. `error_multiplicity_impossible`
- Cause: Electron count and multiplicity mismatch
- Action: Manually adjust the input, because ChemStack ORCA does not rewrite charge or multiplicity

## 14) Testing

```bash
cd <repo_root>
pytest -q
```

Focused regression commands used during the monorepo migration:

```bash
pytest tests/flow -q
pytest tests/integration -q
```

For package-layout and import guidance, see [DEVELOPMENT.md](DEVELOPMENT.md).
Historical migration plans are archived under [archive/README.md](archive/README.md).
