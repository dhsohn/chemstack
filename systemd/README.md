# systemd assets

This directory is the single home for long-running ChemStack service assets.

## Included units

- `chemstack-queue-worker@.service`
  - recommended unified ORCA+xTB+CREST queue worker template
- `chemstack-orca-queue-worker@.service`
  - ORCA-only compatibility template powered by the unified CLI
- `chemstack-xtb-queue-worker@.service`
  - xTB-only compatibility template powered by the unified CLI
- `chemstack-crest-queue-worker@.service`
  - CREST-only compatibility template powered by the unified CLI
- `chemstack-flow-workflow-worker.service`
  - supervised workflow worker for `chemstack.flow`
- `chemstack-flow-worker.env.example`
  - example environment file for the workflow worker

## Engine queue workers

Use `chemstack-queue-worker@.service` as the default engine-worker service. It starts ORCA, xTB, and CREST together through:

- `python -m chemstack.cli queue worker --app orca --app xtb --app crest`

Common assumptions:

- Repository path is `/home/<user>/chemstack`
- Config path is `/home/<user>/chemstack/config/chemstack.yaml`
- Python path is `/home/<user>/chemstack/.venv/bin/python`
- The unified service runs all three engine workers under one supervisor process

Install the unified engine worker:

```bash
cd <repo_root>
sudo cp systemd/chemstack-queue-worker@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now "chemstack-queue-worker@$(whoami)"
```

Monitor the unified engine worker:

```bash
systemctl status "chemstack-queue-worker@$(whoami)"
journalctl -u "chemstack-queue-worker@$(whoami)" -f
```

Maintain the unified engine worker:

```bash
sudo systemctl restart "chemstack-queue-worker@$(whoami)"
sudo systemctl stop "chemstack-queue-worker@$(whoami)"
```

If you still want split services, the compatibility templates remain available:

- `chemstack-orca-queue-worker@.service` runs `python -m chemstack.cli queue worker --app orca`
- `chemstack-xtb-queue-worker@.service` runs `python -m chemstack.cli queue worker --app xtb`
- `chemstack-crest-queue-worker@.service` runs `python -m chemstack.cli queue worker --app crest`

Install one compatibility worker like this:

```bash
cd <repo_root>
APP=orca   # or xtb / crest
sudo cp "systemd/chemstack-${APP}-queue-worker@.service" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now "chemstack-${APP}-queue-worker@$(whoami)"
```

Whether you use one unified service or three split services, `scheduler.max_active_simulations` in `chemstack.yaml` still caps the combined number of active simulations across ORCA, xTB, and CREST.

## Flow workflow worker

This worker is for the orchestration loop under `chemstack.flow`, not for engine job execution. Engine workers should use `chemstack-queue-worker@.service` or `python -m chemstack.cli queue worker`.

Quick install:

```bash
cd <repo_root>
sudo install -d /etc/chemstack
sudo cp systemd/chemstack-flow-worker.env.example /etc/chemstack/flow-worker.env
sudo cp systemd/chemstack-flow-workflow-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now chemstack-flow-workflow-worker
```

Before enabling the service:

- Set top-level `workflow.root` in `chemstack.yaml`
- `CHEM_FLOW_PYTHON` if you do not use `<repo_root>/.venv/bin/python`
- `CHEM_FLOW_CONFIG` if you do not use the default `chemstack.yaml`

For manual foreground runs without the extra flow-only tuning flags, use:

```bash
python -m chemstack.cli queue worker
python -m chemstack.cli queue worker --app workflow --chemstack-config config/chemstack.yaml
```

The plain `queue worker` form also starts the workflow worker when `workflow.root` is set in `chemstack.yaml`.

Monitoring:

```bash
systemctl status chemstack-flow-workflow-worker
journalctl -u chemstack-flow-workflow-worker -f
```

Maintenance:

```bash
sudo systemctl restart chemstack-flow-workflow-worker
sudo systemctl stop chemstack-flow-workflow-worker
```

The flow service uses `scripts/flow/chem_flow_worker_service.sh`, which now
loads `/etc/chemstack/flow-worker.env` by default.

If your paths differ, edit the copied unit or env file in `/etc` before
enabling it.
