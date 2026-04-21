# ChemStack Development Notes

This repository now uses a monorepo-style package layout under `src/chemstack`.

## Canonical Import Rules

- ORCA implementation: `chemstack.orca.*`
- Shared infrastructure: `chemstack.core.*`
- Workflow orchestration: `chemstack.flow.*`
- Engine packages: `chemstack.xtb.*`, `chemstack.crest.*`

Top-level `core.*` and `orca_auto.*` shim packages were removed. New code, tests, and docs should import from `chemstack.*`.

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

- `python -m chemstack.orca.cli ...`
- `python -m chemstack.xtb.cli ...`
- `python -m chemstack.crest.cli ...`
- `python -m chemstack.flow.cli ...`

Console-script aliases such as `orca_auto`, `xtb_auto`, `crest_auto`, `chem_flow`, and `./bin/orca_auto` were removed from the supported CLI surface.

## Practical Import Map

Use these patterns in new code:

```python
from chemstack.orca.cli import main
from chemstack.orca.commands.run_inp import cmd_run_inp
from chemstack.orca.runtime.worker_job import start_background_run_job

from chemstack.core.queue import enqueue
from chemstack.core.admission import reserve_slot
from chemstack.core.indexing import get_job_location
```

Do not use these removed pre-monorepo imports:

```python
from core.commands.run_inp import cmd_run_inp
from orca_auto.runtime.worker_job import start_background_run_job
```

## Test Layout

- `tests/flow/`: flow unit and contract tests
- `tests/integration/`: in-repo integration smoke tests replacing the older cross-repo harness
- `tests/core/`, `tests/xtb/`, `tests/crest/`: absorbed package-specific suites
- top-level `tests/test_*.py`: ORCA-focused regression tests

Common commands:

```bash
pytest tests -q --ignore=tests/core --ignore=tests/xtb --ignore=tests/crest --ignore=tests/flow --ignore=tests/integration
pytest tests/flow -q
pytest tests/integration -q
```

## Package Policy

- `chemstack.orca` is the only implementation source of truth
- All supported package imports live under `src/chemstack`
- If a new feature requires code changes in ORCA logic, make them under `src/chemstack/orca`
- Keep runtime compatibility fallbacks explicit, but do not reintroduce top-level alias packages

## Historical Docs

- [REFERENCE.md](REFERENCE.md): runtime and behavior reference
- [archive/README.md](archive/README.md): archived migration and planning documents
- [archive/MCP_WORKFLOW_MIGRATION_PLAN.md](archive/MCP_WORKFLOW_MIGRATION_PLAN.md): historical migration plan, not the current package map
- [archive/ORCA_AUTO_BASE_MONOREPO_ABSORPTION_PLAN_2026-04-20.md](archive/ORCA_AUTO_BASE_MONOREPO_ABSORPTION_PLAN_2026-04-20.md): absorption planning context
