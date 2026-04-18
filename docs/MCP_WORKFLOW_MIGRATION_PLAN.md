# ORCA Auto MCP/Platform Alignment Migration Plan

## 1. Goal

Restructure `orca_auto` so it fits the same application shape now used by:

- `crest_auto`
- `xtb_auto`
- `chem_workflow_mcp`
- `chem_core`

The migration should make `orca_auto` easier to integrate as a sibling engine
service in the chemistry platform without weakening its current strengths as a
queue-first ORCA executor.


## 2. What Must Stay Stable

The following behaviors should remain stable during the migration:

- manual CLI submission through `run-inp`
- supervised foreground worker execution
- conservative ORCA-specific retry logic
- existing reaction-directory artifact layout
- existing `run_state.json` / `run_report.json` / `run_report.md` outputs
- existing organized output layout under `organized_root`
- existing compatibility with `chem_workflow_mcp`

Important constraint:

- do not rename the ORCA artifact files in the first migration wave
- do not change the retry ladder semantics while the structural migration is in
  progress


## 3. Main Problems In The Current Shape

`orca_auto` currently differs from the sibling applications in several ways:

- it still ships the package as `core` rather than `orca_auto`
- it keeps its own config dataclasses instead of building on `chem_core.config`
- it keeps its own queue and admission implementations instead of using
  `chem_core.queue` and `chem_core.admission`
- it keeps ORCA result organization in a custom `records.jsonl` index instead
  of exposing a `chem_core.indexing`-style job location facade
- it uses `reaction_dir` and `run_id` as the practical identifiers, but lacks a
  stable submission-time `job_id`/`task_id` model like the sibling apps
- `chem_workflow_mcp` therefore has to read ORCA artifacts more directly and
  more defensively than it does for CREST and xTB


## 4. Final Target Shape

The target architecture should look like this:

```text
orca_auto/
  orca_auto/
    __init__.py
    cli.py
    config.py
    state.py
    job_locations.py
    tracking.py
    notifications.py
    commands/
      __init__.py
      _helpers.py
      init.py
      list_jobs.py
      organize.py
      queue.py
      run_dir.py
      summary.py
    execution/
      __init__.py
      runner.py
      parser.py
      analyzer.py
      attempt_engine.py
      inp_rewriter.py
      completion_rules.py
      state_machine.py
      attempt_resume.py
      attempt_reporting.py
    monitoring/
      __init__.py
      discovery.py
      index.py
      monitor.py
    runtime/
      __init__.py
      run_lock.py
      process_tracking.py
      queue_worker.py
      cancellation.py
  core/
    ... compatibility shims during migration only ...
```

Notes:

- the exact subpackage names may vary slightly, but the important change is the
  boundary: app shell outside, ORCA-specific execution logic inside
- `chem_core` should own generic infrastructure
- `orca_auto` should own only ORCA-specific behavior and ORCA-specific
  operator-facing UX


## 5. Migration Principles

Use these rules throughout the migration:

1. move one boundary at a time
2. preserve user-visible behavior until the end of the migration
3. keep compatibility shims while tests and downstream code still import `core.*`
4. add facades before deleting raw file readers
5. cut over `chem_workflow_mcp` only after `orca_auto` exposes a stable ORCA
   tracking/index facade


## 6. Recommended Delivery Sequence

The migration should be delivered in seven small waves.

### Wave 0. Freeze the current contract

Purpose:

- define exactly what must remain compatible while the structure changes

Changes:

- document the current ORCA artifact contract:
  - `run_state.json`
  - `run_report.json`
  - `run_report.md`
  - queue entry fields currently consumed downstream
  - organized-index record fields currently consumed downstream
- add a focused integration test that asserts `chem_workflow_mcp` can still
  load a completed ORCA result after later internal refactors

Done when:

- the ORCA artifact contract is explicit in tests and docs
- later refactors can be judged against a stable baseline


### Wave 1. Introduce the real `orca_auto` package

Purpose:

- align the package shape with `crest_auto` and `xtb_auto`

Changes:

- create `orca_auto/` as the real Python package
- move `core/cli.py` to `orca_auto/cli.py`
- move `core/commands/` to `orca_auto/commands/`
- update `pyproject.toml` so:
  - the console entry point targets `orca_auto.cli:main`
  - package discovery includes `orca_auto*`
- keep `core/` as import-forwarding shims for one migration window

Suggested compatibility strategy:

- `core/cli.py` imports from `orca_auto.cli`
- `core/commands/run_inp.py` imports from `orca_auto.commands.run_inp`
- tests may continue importing `core.*` until later cleanup

Done when:

- `python -m pip install -e .` exposes `orca_auto`
- `./bin/orca_auto` and the installed console script still work
- no user-visible behavior changes


### Wave 2. Align configuration with `chem_core`

Purpose:

- stop carrying a separate ORCA-only runtime config model for shared fields

Changes:

- replace the current custom runtime/telegram dataclasses with an app config
  built on:
  - `chem_core.config.CommonRuntimeConfig`
  - `chem_core.config.CommonResourceConfig`
  - `chem_core.config.TelegramConfig`
- keep ORCA-specific config only for:
  - `paths.orca_executable`
  - `runtime.default_max_retries`
  - any ORCA-only behavior flags that do not belong in `chem_core`
- rename `runtime.admission_max_concurrent` to `runtime.admission_limit`
  internally, but keep compatibility reading for the old key during migration

Done when:

- `orca_auto.config.AppConfig` looks structurally like the sibling apps'
  configs
- shared code can use `resolved_admission_root` and `resolved_admission_limit`
- old config files still load


### Wave 3. Move queue and admission onto `chem_core`

Purpose:

- eliminate the largest remaining duplication against sibling apps

Changes:

- replace custom queue persistence with `chem_core.queue`
- replace custom admission persistence with `chem_core.admission`
- keep ORCA-specific helpers only where truly needed:
  - duplicate detection by reaction directory
  - orphan reconciliation that consults ORCA run state
  - worker PID tracking if still needed
- convert the ORCA queue metadata to the generic `chem_core.queue.QueueEntry`
  form:
  - `task_id` becomes the stable ORCA submission identifier
  - `metadata` holds `reaction_dir`, selected input, retry settings, and any
    ORCA-specific flags

Important design choice:

- generate a stable submission-time `job_id` or `task_id` at enqueue time
- keep `run_id` as the execution instance identity inside the reaction
  directory state

Done when:

- `run-inp` and `queue worker` use `chem_core.queue` and `chem_core.admission`
- queue and admission tests cover the new path
- the old queue/admission modules are either deleted or reduced to wrappers


### Wave 4. Replace `organize_index` with `job_locations.py` and `tracking.py`

Purpose:

- expose ORCA results to the rest of the platform the same way CREST and xTB do

Changes:

- create `orca_auto/job_locations.py`
- create `orca_auto/tracking.py`
- back ORCA location tracking with `chem_core.indexing`
- define an ORCA job location record that carries:
  - `job_id`
  - `run_id`
  - `status`
  - `original_run_dir`
  - `organized_output_dir`
  - `latest_known_path`
  - `selected_input_xyz`
  - `job_type`
  - `molecule_key`
  - `resource_request`
  - `resource_actual`
- keep writing the legacy organized `records.jsonl` temporarily if
  `chem_workflow_mcp` still depends on it
- add `organized_ref.json` in original run directories after successful moves,
  matching the pattern already used in sibling apps

Important rule:

- add the new tracking facade first
- remove direct downstream reads of `records.jsonl` only after the facade is in
  place and consumed successfully

Done when:

- ORCA has `resolve_latest_job_dir`, `load_job_artifacts`, and `upsert_job_record`
- `chem_workflow_mcp` can load ORCA job locations without reading raw ORCA
  internals first


### Wave 5. Simplify state and reporting boundaries

Purpose:

- make ORCA state look more like the sibling-app pattern without breaking the
  richer ORCA retry model

Changes:

- split the current `state_store.py` responsibilities into clearer modules:
  - `state.py` for JSON read/write and report generation
  - `runtime/run_lock.py` for run-lock ownership
  - optional helper modules for report rendering and persistence
- keep the current file names:
  - `run_state.json`
  - `run_report.json`
  - `run_report.md`
- add helper functions similar in role to sibling apps:
  - `load_state`
  - `load_report_json`
  - `write_state`
  - `write_report_json`
  - `write_report_md`

Do not do yet:

- do not flatten the ORCA retry attempts into the simpler sibling schemas
- do not rename `run_state.json` to `job_state.json` in this wave

Done when:

- ORCA persistence is easier to consume through a thin facade
- the richer ORCA attempt history remains intact


### Wave 6. Simplify execution and worker flow

Purpose:

- reduce the special internal execution path and align worker structure with the
  sibling apps

Changes:

- keep `run-inp` as the public queue submission command
- keep an internal execution function, but stop treating hidden `run-job` as the
  long-term primary boundary
- refactor worker execution toward:
  - dequeue queue entry
  - load ORCA metadata from queue entry
  - execute ORCA run through a Python call path
  - persist state/report/location updates
  - optionally auto-organize
- retain a compatibility `run-job` command temporarily if it makes rollout safer

Rationale:

- `xtb_auto` and `crest_auto` run the engine directly from the worker command
  path
- ORCA can keep an internal helper for lock ownership and signal handling, but
  the architectural boundary should become clearer

Done when:

- the worker no longer depends on `python -m core.cli` as its main long-term
  execution model
- worker logic is readable as submit -> execute -> persist -> organize


### Wave 7. Cut over `chem_workflow_mcp` to the new ORCA facade

Purpose:

- remove ORCA-specific downstream special cases

Changes:

- update `chem_workflow_mcp` so ORCA artifact loading prefers:
  - `orca_auto.job_locations`
  - `chem_core.indexing`
  - ORCA state/report facade helpers
- reduce direct reads of:
  - `queue.json`
  - `organized_root/index/records.jsonl`
  - raw ad hoc path inference from ORCA inputs
- keep defensive fallback reads only while older ORCA outputs still exist on
  disk

Done when:

- ORCA, CREST, and xTB are all discovered through similarly shaped tracking
  facades
- the workflow layer no longer needs one-off ORCA recovery logic for normal
  cases


## 7. Explicit Non-Goals For This Migration

The following items should not be mixed into the structural migration:

- changing ORCA retry recipes
- redesigning completion analysis
- changing chemistry-specific route defaults
- replacing the Telegram bot UX
- introducing a public ORCA MCP server
- renaming all artifacts to match sibling apps immediately


## 8. Concrete File Mapping

Recommended first-pass file mapping:

- `core/cli.py` -> `orca_auto/cli.py`
- `core/config.py` -> `orca_auto/config.py`
- `core/commands/*` -> `orca_auto/commands/*`
- `core/orca_runner.py` -> `orca_auto/execution/runner.py`
- `core/orca_parser.py` -> `orca_auto/execution/parser.py`
- `core/out_analyzer.py` -> `orca_auto/execution/analyzer.py`
- `core/attempt_engine.py` -> `orca_auto/execution/attempt_engine.py`
- `core/inp_rewriter.py` -> `orca_auto/execution/inp_rewriter.py`
- `core/completion_rules.py` -> `orca_auto/execution/completion_rules.py`
- `core/state_machine.py` -> `orca_auto/execution/state_machine.py`
- `core/attempt_resume.py` -> `orca_auto/execution/attempt_resume.py`
- `core/attempt_reporting.py` -> `orca_auto/execution/attempt_reporting.py`
- `core/state_store.py` -> `orca_auto/state.py` plus `orca_auto/runtime/run_lock.py`
- `core/result_organizer.py` -> `orca_auto/commands/organize.py` plus
  `orca_auto/tracking.py`
- `core/organize_index.py` -> `orca_auto/job_locations.py`
- `core/queue_worker.py` -> `orca_auto/runtime/queue_worker.py`
- `core/cancellation.py` -> `orca_auto/runtime/cancellation.py`
- `core/dft_discovery.py` -> `orca_auto/monitoring/discovery.py`
- `core/dft_index.py` -> `orca_auto/monitoring/index.py`
- `core/dft_monitor.py` -> `orca_auto/monitoring/monitor.py`
- `core/telegram_notifier.py` -> `orca_auto/notifications.py`
- `core/telegram_bot.py` -> `orca_auto/telegram_bot.py`


## 9. Testing Strategy By Wave

Each wave should have its own acceptance checks.

### Required checks after Wave 1

- console script still works
- `./bin/orca_auto` still works
- legacy `core.*` imports still resolve

### Required checks after Wave 2

- old config file still loads
- new config object exposes `chem_core`-style runtime fields

### Required checks after Wave 3

- queue submission still rejects duplicate active work
- worker still honors shared admission limits
- cancellation still works for pending and running jobs

### Required checks after Wave 4

- ORCA jobs can be resolved by `job_id`, `run_id`, original path, and organized
  path
- organized runs still remain discoverable after relocation
- `chem_workflow_mcp` can load a completed ORCA artifact through the new facade

### Required checks after Wave 5

- ORCA attempt history is preserved exactly
- report generation still includes final result and attempt table

### Required checks after Wave 6

- worker restart and orphan reconciliation still work
- run lock behavior still prevents duplicate live execution

### Required checks after Wave 7

- one end-to-end workflow in `chem_workflow_mcp` can submit, sync, and complete
  an ORCA stage through the new ORCA tracking path


## 10. Rollout Recommendation

Recommended PR sequence:

1. add `orca_auto/` package and keep `core/` shims
2. move config onto `chem_core` dataclasses
3. move queue and admission onto `chem_core`
4. add `job_locations.py`, `tracking.py`, and `organized_ref.json`
5. split state/report facade from run-lock concerns
6. simplify worker execution flow
7. cut `chem_workflow_mcp` over to the ORCA facade
8. remove `core/` shims and any legacy raw-index dependencies

Why this order:

- it preserves operator behavior early
- it stabilizes shared infrastructure before touching workflow integration
- it avoids breaking `chem_workflow_mcp` while ORCA internals are still moving


## 11. Recommended Immediate Next Step

Start with a small first implementation slice:

1. create the real `orca_auto/` package
2. move `cli.py`, `commands/`, and `config.py`
3. leave `core/` as import-forwarding shims
4. update packaging and tests

This first slice is low-risk, gives the project the right package identity, and
creates the clean boundary needed for the later `chem_core` and tracking
migrations.
