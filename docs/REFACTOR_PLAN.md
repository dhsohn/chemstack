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

## Phase 2 — Remove `del deps` no-op parameters — **CLOSED 2026-06-13**

**DONE 2026-06-12 (partial):** the 3 `run_dir_manifest.py` wrappers
(`_manifest_mapping`, `_resolve_engine_manifest`,
`_resolve_endpoint_pairing_manifest`) were deleted;
`_resolve_run_dir_manifest_sections` now calls the `.manifest` shared
functions directly (no test overrode those `_dependency` names — verified).

**Resolution of the flagged sites (2026-06-13)** — the dispatch-table caution
was warranted; mapping confirmed the remaining `del deps` are load-bearing:

- `stage_runtime/xtb_retry.py` impls: everything registered through
  `_bind_many_with_deps` is wrapped by `_bind_with_deps`, whose wrapper
  ALWAYS injects `kwargs["deps"] = deps_provider.get()` at call time — so
  bound impls must accept a `deps` keyword even when unused. The `del deps`
  in `xtb_attempt_record_impl` / `xtb_current_attempt_number_impl` stays
  (now listed under "Deliberately kept"). One genuinely dead chain WAS
  removed: `xtb_attempt_rows_impl` + the `_xtb_attempt_rows` container field
  (`dep_types.OrchestrationStageRuntimeDeps`) + its registry entry and
  re-exports had no caller anywhere — `o.stages._xtb_attempt_rows` was never
  read (the underlying `WorkflowStageView.xtb_attempt_rows()` method stays;
  stage_view_mutators uses it).
- `dep_builder_stage_fallbacks.py` `*_for_context` adapters (incl. the
  line-190 `del deps_provider`): all five are consumed uniformly as
  `factory(overrides, deps_provider)` by the `_StageDepFallbackSpec`
  registry in `dep_builder_factories.py`; each `del`s the argument it does
  not need. Uniform-dispatch adapters — deliberately kept.

## Phase 3 — `_dependency(...)` pattern in CLI modules — **COMPLETE 2026-06-13**

The stringly-typed `_dependency` lookup is extinct: `cli_common._dependency`
has been deleted along with every call site. (The remaining grep hits for
"dependency" in src are different, deliberate patterns: the
`core/queue/dependencies.py` typed-DI container machinery, and
`flow/engines/xtb/artifacts.py`'s `_required_dependency(explicit, name)`
explicit-or-raise guard — neither is the stringly-typed deps-namespace
lookup.)

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

**`cli_workers.py` + `cli_worker_specs.py` + `cli_worker_conflicts.py` DONE
2026-06-13:** all 30 lookups removed (12 + 13 + 5) and every `deps` parameter
dropped; `cmd_queue_worker(args)` now matches the `args.func(args)` call shape.
The only two `deps=`-passing call sites lived in
`tests/test_orca_auto_cli_workers.py`:
`_terminate_process(..., deps=SimpleNamespace(time=fake))` now monkeypatches
the module-global `time` name instead (same style as the file's other
supervisor tests), and the `_detect_existing_orca_worker_conflict(deps=...)`
overrides were behaviorally identical to the autouse fixture patch of
`worker_conflicts._discover_shared_config_path` plus the real
`_effective_shared_config_text`, so they were simply deleted. Everything else
was already exercised via module-global monkeypatching
(`_build_worker_specs`, `_run_worker_supervisor`, `subprocess.Popen`,
`signal.*`, `time.sleep`, `worker_module_command`,
`_discover_shared_config_path`) — late-bound direct calls serve those seams.

**`flow/run_dir_options.py` + `flow/run_dir_manifest.py` DONE 2026-06-13:**
all 27 lookups removed (11 + 16) and every `deps` parameter dropped. The one
`deps=`-passing test (`test_run_dir_workflow_options_apply_cli_manifest_section_default_precedence`)
now monkeypatches `run_dir_options._resolve_required_workflow_root` instead.
Also deleted: the `_RunDirWorkflowOptionResolvers` indirection (4 all-`Any`
fields rebuilt per call), its `_run_dir_workflow_option_resolvers()` factory,
the trivial `_run_dir_workflow_option_defaults()` constructor wrapper, and
`_workflow_options_to_common_kwargs` (duck-typing relic — the bundle now
calls `options.common_kwargs()` directly). Two seams preserved by
construction: `_resolve_required_workflow_root` calls
`cli_common._discover_workflow_root` via module-attribute access (5 tests
patch `cli_common`, not `run_dir_options`; the old code reached the same
effect with a function-local import), and `_cli_workflow_root_for_args` /
`WORKFLOW_MANIFEST_FILENAMES` stay late-bound module globals
(`test_cli_plans_and_create_more.py` / `test_cli_helper_edges_more.py` patch
them).

**`flow/engines/xtb/terminal.py` DONE 2026-06-13:** this module had its OWN
`_dependency(deps, explicit, name)` (explicit-or-deps-attr-or-raise; no
module-global fallback). Both production callers (`worker_terminal.py`,
`queue_runtime_terminal.py`) always passed every `*_fn` kwarg explicitly and
nothing in src used `deps=`, so the `*_fn` params are now required
keyword-only args; the deps channel, the local `_dependency`, and
`resolve_terminal_dependencies` (a trivial constructor once deps was gone)
were deleted. One test converted from deps-namespace to explicit kwargs;
the missing-dependency guard test now asserts Python's required-kwarg
TypeError.

**`cli_systemd_apply.py` + `cli_systemd_status.py` DONE 2026-06-13:** the
heaviest deps-USING modules — 11 test call sites in `tests/test_cli_systemd.py`
inject system-effect seams (`run`, `which`, `is_root`, default user,
collectors). Converted to typed frozen dataclasses following the
`QueueCliDeps` precedent: `SystemdInstallCliDeps(run, is_root)` and
`ServiceCliDeps(run, which, is_root, default_service_user,
collect_service_status, restart_unit_for_user)`, both exported through the
`cli_systemd` facade. All 11 test sites now construct the typed deps
(underscore field names dropped). Removing the `Any`-typed lookups exposed
that `build_systemd_install_plan` annotated `target_user: str` /
`repo: str | Path` while `cmd_systemd_install` passes
`getattr(args, ..., None)`; the plan builder now accepts `| None` and
validates repo the same way it already validated user (`--repo is required`
ValueError instead of a `Path(None)` TypeError in the impossible path).

**`cli_common.py` DONE 2026-06-13:** with all importers gone, the 10
internal lookups in `_workflow_root_for_args`, `_engine_config_for_command`,
`_shared_orca_auto_config`, `_normalize_workflow_type` became direct calls
and `_dependency` itself was deleted. Exposed one hidden type issue:
`_discover_shared_config_path` returns `str | None`, now flowing naturally
into `shared_workflow_root_from_config(str | Path | None)`.

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

## Phase 4 — Re-export facade modules — **DONE 2026-06-13**

- `orca/result_organizer.py` — DELETED. 8 src importers and 8 test files
  repointed to the real modules (`result_organizer_planning` /
  `_filesystem` / `_state` / `_models`); the organizer.*-style test files
  now import the real modules under `organizer_planning` / `organizer_fs` /
  `organizer_state` aliases. The one facade mock target
  (`test_organize_index_helpers.py` patching `resolve_organize_metadata`)
  now patches `result_organizer_planning`, matching the repointed lazy
  import in `organize_index.py`.
- `core/notifications/telegram.py` — DELETED. Its only non-re-export
  content (a module-level `urlopen_with_ipv4_fallback` wrapper + LOGGER)
  had no callers; `telegram_transport.py` / `telegram_api.py` bind their
  own wrappers. The notifications package `__init__` keeps the same public
  API, now importing from the real modules. No test monkeypatched facade
  attributes.
- Kept as intended: `flow/adapters/__init__.py`,
  `flow/orchestration/__init__.py`, `flow/submitters/__init__.py`
  lazy-import `__getattr__`/`__dir__` machinery — intentional API surface.

## Phase 5 — Orchestration `dep_builder_*` fragmentation — **DONE 2026-06-13**

7 modules implemented "dep builders" (`dep_builder_core/_builders/_fallbacks/
_stage_fallbacks/_factories/dep_context/dep_types`). Call-graph mapping showed
the four inner builder modules were imported only by each other and the
`dep_builders` re-export facade; everything external (src: `deps.py`; tests:
`test_refactor_cleanup.py`, incl. its `_build_contract_deps` monkeypatch via
module attribute) went through the facade. Consolidated 7 → 3 in three
commits:

- `dep_builder_core` + `dep_builder_fallbacks` + `dep_builder_stage_fallbacks`
  + `dep_builder_factories` merged into `dep_builders.py` (facade replaced by
  the real content; pure movement). The orchestration `__init__`
  lazy-submodule map dropped the deleted names
  (`dep_builder_stage_fallbacks` had never been registered).
- The five `*_for_context` adapters folded into the stage fallback factories:
  same uniform `(overrides, deps_provider)` registry signature with
  `del`-unused, one naming layer less.
- `dep_context.py` folded into `deps.py` (`orchestration_context` now sits
  next to `orchestration_deps`; its lazy `import_module` hop is gone). The
  nine importers repointed. `deps.py` also stopped mirroring every
  `dep_types` name — src only ever consumed `orchestration_deps`; the one
  test importing type names now imports `dep_types` directly.

Survivors: `dep_types.py` (typed containers + stage-group registry; the
annotation leaf ~15 modules import), `dep_builders.py` (all builder/fallback
machinery), `deps.py` (public entry: `orchestration_deps` /
`orchestration_context`).

## Phase 7 — Same-package duplicate bodies — **DONE 2026-06-13**

After Phase 6 the facade seam was exhausted, so this round used a byte-identical
function-body scan (AST-shape matches were too noisy — most were deliberate
xtb/crest parallelism). Only two clusters were both byte-identical AND
same-package (so no cross-layer coupling, no new indirection):

- `orca/commands`: `organize_service.workflow_runtime_paths` was a verbatim copy
  of `_helpers._workflow_runtime_paths` (only the annotation differed:
  `str | Path` vs `Path`). Widened `_helpers` to `str | Path`, deleted the
  `organize_service` copy, repointed its one call site, and dropped the
  now-unused `workflow_workspace_internal_engine_paths_from_path` import.
  Neither name was imported elsewhere or monkeypatched.
- `flow/submitters`: `orca_cancellation.cancel_config` and
  `orca_submission.submission_config` had identical bodies (build
  `SiblingSubmitterConfig` from `normalize_text(orca_config)` /
  `normalize_text(orca_repo_root) or None`). Extracted one
  `sibling_submitter_config` into `orca_models.py` (the module that already
  owns the dataclass and other small helpers), repointed both single call
  sites. Each old name had exactly one in-module caller, no test refs.

Net −22 lines. 1791 tests green.

### Surveyed and DECLINED this round (do not re-investigate)

- `flow/adapters/_orca_contract_status.py` `*_impl` (attempt_count / max_retries
  / coerce_attempts / status_from_payloads) vs the concrete twins in
  `orca/_job_location_utils.py`. The adapters versions take `normalize_text_fn`
  / `safe_int_fn` injected and are wired by `flow/adapters/orca.py`'s
  `OrcaContractLoaderDeps` container — a deliberate, consistently-applied DI
  design across the whole `flow/adapters` package (same status as
  `OrchestrationDeps`: legitimate architecture, out of scope). The two live in
  different layers; merging would force a `flow → orca` (or worse) dependency.
- `core/admission/persistence.py` vs `core/queue/persistence.py`
  (load_slots/save_slots ≈ load_entries/save_entries) and the two `store.py`
  `for_root` classmethods. The shared work (`atomic_write_json` /
  `load_json_list_file` / `resolve_root_path`) is already extracted into
  `core/utils/persistence.py`; what remains is a ~6-line typed wrapper over
  different domain types. A generic `Store[T]` / `load_typed_list` would add a
  layer for two callers — net-neutral and against the philosophy.
- `flow/telegram_bot.py` `_dispatch_*` / `_send_*` / `_api_call` vs
  `telegram_dispatch.py`: this is the DI binding seam (bot binds concrete impls,
  `telegram_dispatch` exposes injectable functions, both delegate to
  `telegram_interactive`). Intentional and heavily monkeypatched by
  `test_telegram_bot.py` — not duplication.
- `flow/submitters` `persist_cancellation_workflow` / `persist_submission_workflow`
  (3-line bodies differing only in `deps: CancellationDeps` vs `SubmissionDeps`).
  Merging needs a new structural Protocol — adds an abstraction to dedup six
  lines, net-neutral. Left as-is.
- All xtb↔crest `execution.py` / `queue_runtime.py` / `job_inputs.py` /
  `_submitter_deps` byte-identical helpers: same decline as Phase 6 (parallel
  engine packages; a shared home would re-add the DI indirection).

## Phase 6 — Re-export facade modules, round 2 — **DONE 2026-06-13**

A reverse-import sweep over the remaining import-only modules (imports +
`__all__`, zero defs) found five facades whose consumers could all point at
the real modules directly. No test monkeypatched any of them (every reference
was a plain import), so each was deleted and its importers repointed:

- `flow/orchestration/steps.py` — test-only facade (21 `*_impl` re-exports).
  `test_orchestration_final_push.py` now imports from `lifecycle` /
  `materialization` / `stage_runtime.*` / `support` / `workflow_builders` /
  `deps`. Dropped from the orchestration `__init__` lazy-submodule map.
- `core/notifications/engine_module.py` — `engines.py` / `engine_specs.py` /
  `test_engine_notifications.py` now import from the real `engine_delivery` /
  `engine_jobs` / `engine_notifier` modules.
- `flow/runtime_events.py` — `runtime.py` now imports straight from
  `runtime_results` / `stage_transition_events`. Its 8 event-name mirrors
  (`stage_key` … `handoff_transition_event_payload`) and the
  `ACTIVE_TERMINAL_SYNC_STATUSES` re-export had no consumer anywhere and were
  dropped (same precedent as Phase 5's deps.py mirror removal). The
  advance-helper mirrors pinned by
  `test_refactor_cleanup.py::test_runtime_facade_keeps_advance_helpers_available`
  are untouched. `test_runtime.py` imports `stage_transition_event_payloads`
  from the real module.
- `cli_systemd.py` — 146 lines re-exporting `cli_systemd_apply` /
  `cli_systemd_status` / `systemd_plan`, including ~25 consumer-less
  `_private as _private` lines. `cli_parser_systemd.py` and the two CLI test
  files import the three real modules now; the `args.func is cmd_*` identity
  asserts still hold because the facade re-exported the same objects.
- `flow/submitters/internal_engine.py` — namespace facade over
  `internal_engine_builder/_cancellation/_models/_submission`. `xtb.py` /
  `crest.py` / `orca.py` / `test_submitters_common_engines.py` repointed,
  lazy-map entry dropped. Caution for next time: a multi-line
  `from ...submitters import (internal_engine,)` in that test was invisible
  to a single-line reverse-import grep — the full src+tests mypy run is what
  caught it.

Kept as intended: `orca/job_locations.py` (public face of the
underscore-private `_job_location_*` cluster) and the wide-fan-in aggregators
(`core/queue/engine_execution.py` / `worker.py` / `internal_engine.py`,
`core/notifications/engines.py`, `core/indexing/engines.py`, `flow/state.py`,
`flow/registry.py`) — each consumed as API by 4–10 src modules. Also surveyed
and declined: merging the `flow/engines/xtb` ↔ `crest` parallel packages —
after engine-name normalization the module pairs are only 0.4–0.8 similar
(`terminal.py` 0.16, `execution.py` 0.51), so a shared parameterized core
would add DI indirection, the exact pattern this plan removes.

## Phase 8 — Meaningless fallbacks & redundant indirection — **DONE 2026-06-15**

A fresh pass for *meaningless fallbacks* (defensive code that can never fire
given the static types) and *over-complexity* (redundant computation /
indirection), separate from the earlier delegation/DI theme. Coverage was both
broad (repo-wide pattern greps + two AST scans over all 378 files: redundant
public→private delegation, and dead module-level functions) and deep (full
reads of the non-DI logic files `core/queue/store.py`,
`flow/orchestration/lifecycle.py`, `core/utils/coercion.py`,
`flow/xyz_utils.py`, `orca/out_analyzer.py`).

Removed:

- `flow/runtime_cycle.py` `workflow_lease_expires_at`: the body wrapped a
  `from datetime import …` plus the timestamp arithmetic in a
  `try/except Exception: return ""`. The import is stdlib (cannot fail) and the
  math runs on an already-`<= 0`-guarded `float`, so the except was unreachable.
  Moved the import to module top, dropped the try/except and a redundant
  `float()`. The meaningful empty-lease path (`lease_seconds <= 0`) stays.
  Updated `test_support_module_edges_final.py`, which had injected a fake
  `sys.modules["datetime"]` and a `_BrokenDateTime` solely to cover that dead
  branch — replaced with a `runtime_cycle.datetime` patch plus the real
  `lease_seconds <= 0` assertion.
- `flow/orchestration/support.py` `reaction_ts_guess_error_impl`: the sort key
  computed `int(getattr(item, "rank", 0) or 0)` twice in one lambda; collapsed
  to a single walrus.
- `activity_labels.py` `queue_task_label`: `normalize_text(task_kind) or ""` —
  `normalize_text` always returns `str`, so the `or ""` was a no-op (and the
  same function already uses the bare value at line 86). Dropped.
- `flow/submitters/orca.py` `_queued_payload`: `int(priority)` / `bool(force)`
  in the `extra_fields` dict, where the params are already `priority: int` /
  `force: bool` and the same function emits them bare two lines up
  (`"priority": priority`, `if force:`). Dropped the inconsistent casts.
- `core/queue/store.py`: `entry_from_dict` → `_entry_from_dict` →
  `_queue_persistence.entry_from_dict` was a three-level chain; the private
  middle layer (used only by the public twin, no test ref) added nothing while
  the sibling `entry_to_dict` delegated straight through. Removed the middle
  layer so both are symmetric.
- `orca/summary.py`: `html_to_plain_text` was a pure pass-through to
  `_html_to_plain_text` (identical `(message: str) -> str`, no caller, no test,
  no seam). Merged the impl into the `__all__`-exported public name.

Net −34 lines (incl. test simplification). 1791 tests green.

### Surveyed and confirmed clean / deliberately left (do not re-investigate)

- The AST delegation scan flagged 15 public→`_private` pairs; 13 are real
  API / test-seam splits (external callers and/or `test_*` references) and one
  (`attempt_reporting.finalize_and_emit`) is a kwargs→`FinalizeAndEmitRequest`
  adapter, not pure indirection — only `summary.html_to_plain_text` was genuine.
- The dead-function scan flagged only `DFTIndex.get_recent`, already in the
  "Deliberately kept" list below.
- `int(priority)` / `bool(force)` / `int(returncode)` casts on already-typed
  values at serialization boundaries (`submitters/orca.py` `_submit_request`,
  `internal_engine_submission.py`, `attempt_retry.py`): net-neutral churn, left
  except the single internally-inconsistent function above.
- `xyz_utils._select_energy_ranked_frame`'s
  `item.energy if … is not None else float("-inf")` looks dead (the list is
  pre-filtered) but is required: mypy does not narrow the element type, and `or`
  would mis-handle a legitimate `0.0` energy. Left.
- `run_dir_options.py` `is not None` / `is None` guards before
  `normalize_text(…) != ""` are redundant (normalize_text handles None) but are
  symmetric defensive coercion at a config boundary — net-neutral, left.
- `bool | None` tri-state `is True` / `is False` ladders (`output_status.py`,
  `dft_index_store.py`, `dft_monitor.py`) correctly distinguish `None`; left.

## Deliberately kept (do not "clean up")

- `DFTIndex` query API (`get_stats`, `get_recent`, `get_lowest_energy`,
  `search_by_formula`, `get_for_comparison`, `index_calculations`) — not
  reachable from CLI yet, but a coherent tested feature.
- Test-seam wrappers monkeypatched by tests: `activity_rendering._queue_table_now`,
  `_terminal_max_width`, `cli_queue._queue_table_lines`, `worker_process._pid_file_path`.
- Protocol positional-only params named `__x` in `core/queue/dependencies.py`
  (vulture false positive).
- `del deps` in `stage_runtime/xtb_retry.py` bound impls and
  `del overrides` / `del deps_provider` in the five stage fallback factories
  in `dep_builders.py` — uniform dispatch interfaces (see Phase 2
  resolution; the `*_for_context` adapter layer itself was folded away in
  Phase 5, the convention carries on in the factories).
