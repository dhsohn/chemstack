# Refactor Plan: Delegation Layers & Dependency-Injection Noise

Status: groundwork written 2026-06-12. Phase 0 done (dead-code sweep, ~1,384 lines
removed, all uncommitted at time of writing). Phases below are ordered by
safety/value; each is independently shippable and must end with
`ruff check src/ tests/` + `mypy src/` + `pytest -q` green.

How to run checks (WSL venv; symlinks not visible from Windows side):

```bash
wsl.exe -d Ubuntu-20.04 -- bash -c "cd ~/orca_auto && .venv/bin/ruff check src/ tests/ && .venv/bin/python -m mypy src/ && .venv/bin/python -m pytest -q"
```

## Two distinct "deps" patterns — do not conflate

1. **`OrchestrationDeps`** (`flow/orchestration/dep_types.py`): a structured,
   typed DI container threaded through orchestration. Legitimate architecture;
   ~20 test files pass `deps=` into it. **Out of scope.**
2. **`_dependency(deps, "string_name", default)`** (`cli_common.py`):
   stringly-typed, per-function override lookup used across CLI modules.
   Mostly noise; only some lookups are exercised by tests. **In scope.**

## Phase 1 — Collapse `orca/commands/organize.py` — **DONE 2026-06-12**

What was done (deviations from the original plan noted):

- `organize.py` (374 lines) is now a ~12-line facade re-exporting
  `cmd_organize` / `organize_reaction_dir` from `organize_service`. The lazy
  importers (`cli_handlers.py`, `queue_worker_runtime.py`) and the
  `tests/test_cli.py` / `tests/test_queue_worker.py` patch targets work
  unchanged through the facade.
- `organize_service.default_apply_dependencies()` constructs
  `OrganizeApplyDependencies` directly. The old extensions wiring was a
  **no-op cycle** (each extension wrapper called the same `organize_apply`
  default with an identically rebuilt deps object), so extensions are now
  left at their all-`None` defaults — behavior identical, recursion gone.
  `OrganizeApplyDependencyGroups` and `build_apply_dependencies_from_groups`
  were deleted (organize.py was their only consumer).
- All `*_fn=` params on `cmd_organize` / `organize_reaction_dir` /
  `cmd_organize_apply` / `resolve_organize_scope` are now optional with
  late-bound defaults (`x_fn or module_global`), so
  `patch("...organize_service.<name>")` works.
- organize.py's notification/output wrappers were dropped without
  replacement — `organize_notifications` / `organize_output` already default
  `escape_html_fn` / `send_message_fn` themselves.
- Tests repointed to real modules: `test_organize_helpers.py`,
  `test_organize_command.py`, `test_organize_command_helpers.py`,
  `test_organize_message.py`, `test_organize_cli.py` (patch
  `organize_service.append_record` now).

## Phase 2 — Remove `del deps` no-op parameters

**DONE 2026-06-12 (partial):** the 3 `run_dir_manifest.py` wrappers
(`_manifest_mapping`, `_resolve_engine_manifest`,
`_resolve_endpoint_pairing_manifest`) were deleted;
`_resolve_run_dir_manifest_sections` now calls the `.manifest` shared
functions directly (no test overrode those `_dependency` names — verified).

Remaining sites — **likely NOT safe to touch blindly**:

- `src/orca_auto/flow/orchestration/stage_runtime/xtb_retry.py` — 3 `*_impl`
  functions with `deps: OrchestrationDeps | None` + `del deps`. The `_impl`
  suffix suggests uniform-signature dispatch (callers may pass `deps=` to all
  impls); map the dispatch table in `flow/orchestration/__init__.py` /
  `stage_runtime/` before changing signatures.
- `src/orca_auto/flow/orchestration/dep_builder_stage_fallbacks.py:190` —
  `del deps_provider`, same caveat.

## Phase 3 — `_dependency(...)` pattern in CLI modules

**`cli_queue.py` DONE 2026-06-12:** all 24 `_dependency` lookups removed.
Only the watch loop had test-exercised deps (`_emit_queue_list_once`,
`sleep`) — now a typed frozen dataclass `QueueCliDeps` threaded through
`cmd_queue_list` → `_watch_queue_list`. Everything else became direct
late-bound module-global calls (monkeypatch seams `list_activities`,
`clear_activities`, `cancel_activity`, `count_global_active_simulations`,
`_queue_table_now` still work). `cmd_queue_cancel` lost its `deps` param
(`cli.py` calls `args.func(args)` — nothing ever passed it).

**`flow/cli_run_dir.py` DONE 2026-06-12:** all 25 lookups removed; no test
ever passed `deps=` here. Bigger win: the workflow-creation registry
apparatus (`_RunDirWorkflowCreationBinding/Registry`,
`_NormalizedRunDirWorkflowCreationSpec`, `_RunDirWorkflowCreationPlan`,
`_normalize_run_dir_workflow_creation_spec`, `_invoke_run_dir_workflow_creation`,
the `_DEFAULT_CREATE_*` sentinels and the `*_name` spec fields) existed only
to detect monkeypatched `create_*_workflow` globals and switch call styles —
but `orchestration.create_*_workflow(**kwargs)` IS
`create_*_workflow_from_request(RequestType(**kwargs))` with identical
defaults (verified against `orchestration/requests.py`), so the module now
always calls the late-bound kwargs creators. The 7 test sites that
monkeypatch `cli_run_dir.create_*_workflow` keep working. 432 → ~230 lines.

**`flow/cli_workflow.py` DONE 2026-06-12:** all 15 lookups removed; no test
ever passed `deps=`. The `_WorkflowWorkerRuntime` dataclass (14 all-`Any`
fields built once per command from `_dependency` defaults) is gone — helpers
now take only `_WorkflowWorkerOptions` and call module globals directly
(tests monkeypatch `file_lock`, `now_utc_iso`, `timestamped_token`,
`advance_workflow_registry_once`, `_emit_worker_payload`, etc. at module
level — late binding preserved). One `cast("dict[str, Any]", ...)` needed in
`_advance_workflow_worker_cycle`: the runtime's `Any` fields had been hiding
that `advance_workflow_registry_once` returns the
`WorkflowRegistryCyclePayload` TypedDict while emitters take `dict[str, Any]`.

Usage counts (2026-06-12): `flow/engines/xtb/terminal.py` 18,
`flow/run_dir_manifest.py` 16, `cli_worker_specs.py` 13, `cli_workers.py` 12,
`flow/run_dir_options.py` 11, `cli_common.py` 11 (defines it), others <10.
Suggested next: `cli_workers.py` / `cli_worker_specs.py` (top-level CLI,
same shape as the three done above), then `flow/run_dir_options.py` +
`flow/run_dir_manifest.py` together (they cross-reference), leaving
`flow/engines/xtb/terminal.py` last (engine layer, widest blast radius).

Tests known to pass `deps=` into this pattern: `tests/test_orca_auto_cli_queue.py:39`
(`cmd_queue_list(args, deps=deps)`), `tests/test_cli_systemd.py`,
`tests/flow/test_activity_cli.py`, `tests/flow/test_cli_helper_edges_more.py`.

Per file, classify each `_dependency(deps, "name", default)` lookup:

- **No test ever overrides "name"** → inline the default, drop the lookup.
- **Tests override it** → keep, or convert to a typed dataclass of deps for
  that module (one container per module, not per function).

Do one module per PR-sized change. Suggested order: `cli_queue.py` (smallest
blast radius, single deps-using test file), then `flow/cli_run_dir.py`, then
`flow/cli_workflow.py`.

## Phase 4 — Re-export facade modules

- `src/orca_auto/orca/result_organizer.py` — pure re-export with
  `# ruff: noqa: F401`, including private names (`_cross_device_move`, ...).
  Repoint importers to the real modules (`result_organizer_planning`,
  `result_organizer_filesystem`, `result_organizer_state`,
  `result_organizer_models`), then delete or minimize the facade.
- `src/orca_auto/core/notifications/telegram.py` — same shape (96-line
  `__all__` re-export). Same treatment.
- Keep `flow/adapters/__init__.py`, `flow/orchestration/__init__.py`,
  `flow/submitters/__init__.py` lazy-import `__getattr__`/`__dir__` machinery —
  intentional API surface.

## Phase 5 (stretch) — Orchestration `dep_builder_*` fragmentation

7 modules implement "dep builders" (`dep_builder_core/_builders/_fallbacks/
_stage_fallbacks/_factories/dep_context/dep_types`). Before merging anything,
map call graph; this is architecture, not cleanup. Only attempt with a full
token budget and after Phases 1–4.

## Deliberately kept (do not "clean up")

- `DFTIndex` query API (`get_stats`, `get_recent`, `get_lowest_energy`,
  `search_by_formula`, `get_for_comparison`, `index_calculations`) — not
  reachable from CLI yet, but a coherent tested feature.
- Test-seam wrappers monkeypatched by tests: `activity_rendering._queue_table_now`,
  `_terminal_max_width`, `cli_queue._queue_table_lines`, `worker_process._pid_file_path`.
- Protocol positional-only params named `__x` in `core/queue/dependencies.py`
  (vulture false positive).
