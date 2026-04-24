# ChemStack Detailed Reference

ChemStack is a queue-first executor for ORCA and workflow orchestration. xTB and CREST remain part of the runtime, but they are now used internally for workflow stages rather than as standalone public CLI surfaces. This reference standardizes the shared public CLI and keeps the deeper ORCA runtime behavior documented in one place, since ORCA still has the richest retry, reporting, and monitoring surface.

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
- The queue worker runs under external supervision
- On WSL, the recommended supervisor is `systemd`

Operational consequences:

- Closing the submission terminal after `status: queued` is safe
- If the worker is down, the job remains in `queue.json` until the worker returns
- Worker stop/start is managed by `systemctl`

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
    chemstack-queue-worker@.service
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

This reference standardizes on `chemstack ...` for public
commands:

- `queue list`
- `queue cancel`
- `run-dir <path>`
- `init`
- `scaffold <ts_search|conformer_search>`
- `organize orca`
- `summary orca`

Only the ORCA-specific CLI remains public as a compatibility wrapper. ORCA-only
commands that are not yet unified, such as `monitor`, still live
under `python -m chemstack.orca.cli ...`.
Activate `.venv` first, or call `.venv/bin/chemstack ...` directly.
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

workflow:
  root: "/path/to/workflow_root"
  paths:
    xtb_executable: "/path/to/xtb"
    crest_executable: "/path/to/crest"

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
```

Field descriptions for the `orca` section:

- `runtime.allowed_root`: Root directory permitted for execution
- `runtime.organized_root`: Root for organized outputs
- `runtime.default_max_retries`: Maximum retry count after the initial attempt
- `scheduler.max_active_simulations`: Shared total active-run cap across ORCA, internal xTB stages, and internal CREST stages
- `scheduler.admission_root`: Shared admission root for machine-wide slot coordination
- `workflow.root`: Workflow root for workflow creation, activity inspection, and the integrated workflow worker
- `workflow.paths.xtb_executable`: xTB executable path used by workflow-managed internal stages
- `workflow.paths.crest_executable`: CREST executable path used by workflow-managed internal stages
- Internal xTB/CREST runtimes no longer use a shared `workflow.root/internal/<engine>/...` root
- Workflow-managed xTB/CREST job dirs, per-workflow queues/indexes, and organized outputs are stored only under `workflow.root/<workflow_id>/internal/<engine>/{runs,outputs}`
- `paths.orca_executable`: ORCA executable path

Notes:

- `default_max_retries=2` means `1 initial + 2 retries = 3 total attempts`
- Windows-style paths such as `C:\...` and `/mnt/c/...` are not supported in config

## 7) CLI Usage

All public queue, submission, scaffold, organization, and summary commands
should be documented through `chemstack ...`.

Compatibility note:

- `python -m chemstack.orca.cli` remains a thin wrapper for the public ORCA commands below.
- Standalone xTB and CREST CLI commands were removed. xTB and CREST now run as internal workflow/runtime engines.
- `python -m chemstack.orca.cli monitor` remains an engine-specific entrypoint.

### 7.1 `init`

```bash
chemstack init
```

Behavior:

- `init` interactively creates or updates the shared `chemstack.yaml`
- ORCA, internal xTB, internal CREST, and workflow settings are collected in one place

### 7.2 `run-dir`

```bash
cd <repo_root>
chemstack run-dir '/absolute/path/to/orca_runs/Int1_DMSO'
chemstack run-dir '/absolute/path/to/workflow_inputs/reaction_case'
```

Successful ORCA submission example:

```text
status: queued
job_dir: /absolute/path/to/orca_runs/Int1_DMSO
queue_id: q_20260403_151220_ab12cd
priority: 10
worker: active
worker_pid: 12345
```

Shared behavior:

- Inspects the target directory and routes it to ORCA or workflow handling automatically
- Validates the target directory against the detected run type and configured roots
- Rejects duplicate active queue entries for the same directory
- Writes the queue entry durably before returning
- Leaves actual execution to a worker

ORCA-specific notes:

- Chooses the latest `*.inp` when execution actually starts
- `--force` re-runs even if completed output already exists
- `--max-cores` and `--max-memory-gb` override recorded resource limits for that queued run

Workflow notes:

- `run-dir` materializes a workflow when the target directory looks like a workflow input scaffold
- reaction-path and conformer workflows create and submit xTB/CREST stages internally
- `reaction_ts_search` expands all selected reactant x product CREST pairs into xTB child jobs, and as each xTB child finishes it immediately creates and queues the matching ORCA OptTS child job from that `ts_guess`
- `conformer_search` starts with one CREST child job and then hands off up to 20 retained conformers to ORCA child jobs in the next workflow cycle
- Set top-level `workflow.root` in `chemstack.yaml` before using workflow commands
- Public `run-dir` does not expose workflow override flags; workflow settings come from `flow.yaml` and `chemstack.yaml`
- `scaffold ts_search` and `scaffold conformer_search` write `flow.yaml` with `crest_mode: standard` by default; change it to `nci` when needed

There is no public direct-execution mode for new work. `run-dir` is the durable submission path.

### 7.3 `queue cancel`

```bash
chemstack queue cancel q_20260403_151220_ab12cd
chemstack queue cancel /absolute/path/to/orca_runs/Int1_DMSO
```

`queue cancel` accepts workflow ids for whole-workflow cancellation plus queue ids, run ids,
and known path aliases for individual jobs.

### 7.4 `queue list`

```bash
chemstack queue list
chemstack queue list --engine orca
chemstack queue list --status pending
chemstack queue list --engine xtb
```

`queue list` shows workflow and engine activity in one view, but workflow child simulations
are rendered underneath their parent workflow with indentation. Standalone ORCA jobs remain
top-level entries. The `active_simulations` line counts only the currently running
simulations that consume the shared `scheduler.max_active_simulations` slots.

### 7.5 `organize`

```bash
chemstack organize orca --root '/absolute/path/to/orca_runs'
chemstack organize orca --root '/absolute/path/to/orca_runs' --apply
```

Options:

- `organize orca --reaction-dir <dir>`: Organize one ORCA job directory
- `organize orca --root <dir>`: Scan from the configured ORCA root
- `organize orca --rebuild-index`: Rebuild the ORCA JSONL index
- `--apply`: Perform actual moves; otherwise the command is a dry run

### 7.6 `summary`

```bash
chemstack summary orca --no-send
```

Behavior:

- `summary orca` prints or sends the ORCA Telegram digest

### 7.7 Long-Running Services

Long-running worker and Telegram bot processes are managed through `systemd`
only. Public CLI commands do not start those services directly.

Behavior:

- `chemstack-queue-worker@.service` supervises ORCA by default
- If `workflow.root` is set, the same worker service also starts workflow supervision plus the internal CREST and xTB workers
- `chemstack-bot@.service` starts the unified Telegram bot using `telegram.bot_token` and `telegram.chat_id` from `chemstack.yaml`
- `chemstack-runtime@.target` starts both services together

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

- [`systemd/chemstack-runtime@.target`](/home/daehyupsohn/chemstack/systemd/chemstack-runtime@.target)
- [`systemd/chemstack-queue-worker@.service`](/home/daehyupsohn/chemstack/systemd/chemstack-queue-worker@.service)
- [`systemd/chemstack-bot@.service`](/home/daehyupsohn/chemstack/systemd/chemstack-bot@.service)
- [`systemd/chemstack-flow-workflow-worker.service`](/home/daehyupsohn/chemstack/systemd/chemstack-flow-workflow-worker.service)
- [`systemd/chemstack-flow-worker.env.example`](/home/daehyupsohn/chemstack/systemd/chemstack-flow-worker.env.example)

Recommended always-on runtime install flow when Telegram is configured:

```bash
cd <repo_root>
sudo cp systemd/chemstack-bot@.service /etc/systemd/system/
sudo cp systemd/chemstack-queue-worker@.service /etc/systemd/system/
sudo cp systemd/chemstack-runtime@.target /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now "chemstack-runtime@$(whoami).target"
systemctl status "chemstack-runtime@$(whoami).target"
systemctl status "chemstack-queue-worker@$(whoami)"
systemctl status "chemstack-bot@$(whoami)"
journalctl -u "chemstack-queue-worker@$(whoami)" -f
journalctl -u "chemstack-bot@$(whoami)" -f
```

Before enabling the combined runtime target:

- Set `telegram.bot_token` and `telegram.chat_id` in `chemstack.yaml`
- Set `workflow.root` in `chemstack.yaml` if you also want workflow supervision

Assumptions of the unified runtime templates:

- Repository path: `/home/<user>/chemstack`
- Config path: `/home/<user>/chemstack/config/chemstack.yaml`

If your paths differ, edit the copied unit before enabling it.

The unified queue-worker service supervises ORCA by default. When `workflow.root` is
configured, it also starts workflow supervision plus the internal CREST and
xTB workers. The shared `scheduler.max_active_simulations` setting still limits
the combined number of active simulations across ORCA and workflow-managed
internal engine stages.

If you only want unattended execution without the Telegram bot, enable
`chemstack-queue-worker@$(whoami)` directly instead of the combined runtime
target.

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

Set top-level `workflow.root` in `chemstack.yaml` before enabling the workflow service. Edit `/etc/chemstack/flow-worker.env` only when you need to override the Python path or config path used by the service.

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

1. Ensure the worker service is active under `systemd`
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
