# ChemStack Development Notes

This repository now uses a monorepo-style package layout under `src/chemstack`.

## Canonical Import Rules

- ORCA implementation: `chemstack.orca.*`
- Shared infrastructure: `chemstack.core.*`
- Workflow orchestration: `chemstack.flow.*`
- Engine packages: `chemstack.xtb.*`, `chemstack.crest.*`

New code, tests, and docs should import from `chemstack.*`.

## Current Package Layout

```text
<repo_root>/
├── src/
│   └── chemstack/
│       ├── core/
│       ├── flow/
│       ├── xtb/
│       ├── crest/
│       └── orca/
├── tests/
│   ├── core/
│   ├── flow/
│   ├── integration/
│   ├── xtb/
│   └── crest/
└── docs/
```

## Canonical CLI Form

User-facing docs should standardize on these command forms:

- `chemstack queue ...`
- `chemstack run-dir <path>`
- `chemstack init`
- `chemstack scaffold <ts_search|conformer_search> <path>`
- `chemstack organize orca ...`
- `chemstack scan-notify` (alias: `chemstack monitor`)

Long-running services are not part of the public CLI surface. Users should run
them only through the `systemd/` units.

Engine-specific CLI modules are runtime-only worker entrypoints. Do not add new
user-facing commands there.

Flow internals are not public CLI modules. Keep examples on `chemstack ...`
and avoid module-level `python -m` examples for flow internals.

## Practical Import Map

Use these patterns in new code:

```python
from chemstack.cli import main
from chemstack.orca.commands.run_inp import cmd_run_inp
from chemstack.orca.runtime.worker_job import start_background_run_job

from chemstack.core.queue import enqueue
from chemstack.core.admission import reserve_slot
from chemstack.core.indexing import get_job_location
```

Keep imports under `chemstack.*`; avoid top-level aliases or compatibility shims.

## Test Layout

- `tests/flow/`: flow unit and contract tests
- `tests/integration/`: in-repo integration smoke tests
- `tests/core/`, `tests/xtb/`, `tests/crest/`: absorbed package-specific suites
- top-level `tests/test_*.py`: ORCA-focused regression tests

Common commands:

```bash
python -m pip install -c constraints-dev.txt -e .[dev]
pytest tests -q --ignore=tests/core --ignore=tests/xtb --ignore=tests/crest --ignore=tests/flow --ignore=tests/integration
pytest tests/flow -q
pytest tests/integration -q
bash scripts/clean_artifacts.sh
```

## Package Policy

- `chemstack.orca` is the only implementation source of truth
- All supported package imports live under `src/chemstack`
- If a new feature requires code changes in ORCA logic, make them under `src/chemstack/orca`
- Shared queue admission, child-entrypoint, process lifecycle, terminal
  side-effect, and orphan-reconciliation helpers live under `chemstack.core.queue`
- Keep top-level alias packages and alternate runtime readers out of the codebase

## Internal Engine Workers

xTB and CREST are internal workflow engines. ORCA remains the public queue-first
engine, but its queue worker now uses the same internal-engine lifecycle helpers
for queue-child execution, admission metadata, terminal side effects, and
orphan reconciliation.

New internal engines should expose a small engine adapter through
`chemstack.core.queue.engine_execution` rather than owning a full worker
lifecycle. Provide engine-local context, process, artifact, tracking, and
notification callbacks, then execute through `InternalEngineWorkerAdapter` and
`run_internal_engine_worker_entry`.

ORCA-specific state, retry, input selection, reports, auto-organize behavior,
and the downstream `reaction_dir` contract stay in `chemstack.orca`. The
queue-worker child path should use `--queue-root/--queue-id`; the direct
`--reaction-dir` worker-job mode is retained only for compatibility and direct
single-directory execution helpers.

## Related Docs

- [REFERENCE.md](REFERENCE.md): runtime and behavior reference
