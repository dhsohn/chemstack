# systemd assets

This directory is the single home for long-running ChemStack service assets.

## Included units

- `chemstack-orca-queue-worker@.service`
  - WSL-friendly ORCA queue worker template
- `chemstack-xtb-queue-worker@.service`
  - WSL-friendly xTB queue worker template
- `chemstack-crest-queue-worker@.service`
  - WSL-friendly CREST queue worker template
- `chemstack-flow-workflow-worker.service`
  - supervised workflow worker for `chemstack.flow`
- `chemstack-flow-worker.env.example`
  - example environment file for the workflow worker

## Engine queue workers

ORCA, xTB, and CREST queue workers now share the same `systemd` pattern.

Available templates:

- `chemstack-orca-queue-worker@.service`
- `chemstack-xtb-queue-worker@.service`
- `chemstack-crest-queue-worker@.service`

Common assumptions:

- Repository path is `/home/<user>/chemstack`
- Config path is `/home/<user>/chemstack/config/chemstack.yaml`
- Python path is `/home/<user>/chemstack/.venv/bin/python`
- Each worker runs `python -m chemstack.<app>.cli queue worker`

Install one of the engine workers:

```bash
cd <repo_root>
APP=orca   # or xtb / crest
sudo cp "systemd/chemstack-${APP}-queue-worker@.service" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now "chemstack-${APP}-queue-worker@$(whoami)"
```

Monitor one of the engine workers:

```bash
APP=orca   # or xtb / crest
systemctl status "chemstack-${APP}-queue-worker@$(whoami)"
journalctl -u "chemstack-${APP}-queue-worker@$(whoami)" -f
```

Maintain one of the engine workers:

```bash
APP=orca   # or xtb / crest
sudo systemctl restart "chemstack-${APP}-queue-worker@$(whoami)"
sudo systemctl stop "chemstack-${APP}-queue-worker@$(whoami)"
```

You can keep ORCA, xTB, and CREST workers enabled at the same time. The shared
`scheduler.max_active_simulations` limit in `chemstack.yaml` still caps the
combined number of active simulations across all three engines.

## Flow workflow worker

This worker is for the orchestration loop under `chemstack.flow`, not for
single-engine job submission.

Quick install:

```bash
cd <repo_root>
sudo install -d /etc/chemstack
sudo cp systemd/chemstack-flow-worker.env.example /etc/chemstack/flow-worker.env
sudo cp systemd/chemstack-flow-workflow-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now chemstack-flow-workflow-worker
```

Before enabling the service, edit `/etc/chemstack/flow-worker.env` and set at
least:

- `CHEM_FLOW_WORKFLOW_ROOT`
- `CHEM_FLOW_PYTHON` if you do not use `<repo_root>/.venv/bin/python`
- `CHEM_FLOW_CONFIG` if you do not use the default `chemstack.yaml`

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
