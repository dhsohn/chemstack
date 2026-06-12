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

## Phase 1 — Collapse `orca/commands/organize.py` (pure delegation file)

Verified import graph (2026-06-12):

- Public surface actually used elsewhere:
  - `cmd_organize` — lazy import in `src/orca_auto/cli_handlers.py:33`;
    patched by `tests/test_cli.py:248` (`orca_auto.orca.commands.organize.cmd_organize`).
  - `organize_reaction_dir` — lazy import in
    `src/orca_auto/orca/queue_worker_runtime.py:65`; used directly by
    `tests/test_organize_helpers.py`.
- Everything else in the file is a private `_x` wrapper that forwards to
  `organize_service` / `organize_apply` / `organize_output` /
  `organize_notifications` / `organize_tracking`, plus `_apply_dependencies()`
  which wires `OrganizeApplyDependencies`.

Plan:

1. Move the `_apply_dependencies()` wiring (and the small `_plan_conflict_result`
   / `_bookkeep_*` / `_rollback_after_apply_failure` / `_apply_one_organize_plan`
   closures it references) into `organize_service.py` as a module-level
   `default_apply_dependencies()` factory. They are already thin calls into
   `organize_apply` with `deps=_apply_dependencies()` — the recursion between
   them and `_apply_dependencies` must be preserved (extensions group
   references the same five functions).
2. Give `organize_service.cmd_organize` / `organize_service.organize_reaction_dir`
   defaults so they can be called without the `*_fn=` forest (keep the kwargs
   for tests, but default them).
3. Shrink `organize.py` to a ~10-line facade: `cmd_organize` and
   `organize_reaction_dir` re-exported (keeps `cli_handlers`, `queue_worker_runtime`,
   and the `tests/test_cli.py` patch target working). Optionally repoint those
   two importers and delete the file entirely.
4. Tests to watch: `tests/test_organize_command.py`, `tests/test_organize_helpers.py`,
   `tests/test_cli.py` (patch target string).

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

Usage counts (2026-06-12): `flow/cli_run_dir.py` 25, `cli_queue.py` 24,
`flow/engines/xtb/terminal.py` 18, `flow/run_dir_manifest.py` 16,
`flow/cli_workflow.py` 15, `cli_worker_specs.py` 13, `cli_workers.py` 12,
`flow/run_dir_options.py` 11, `cli_common.py` 11 (defines it), others <10.

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
