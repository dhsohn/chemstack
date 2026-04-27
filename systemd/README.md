# systemd assets

This directory is the single home for long-running ChemStack service assets.

## Included units

- `chemstack-runtime@.target`
  - recommended combined runtime target for the unified queue worker, Telegram bot, and scheduled summary timer
- `chemstack-queue-worker@.service`
  - recommended unified queue worker template
- `chemstack-bot@.service`
  - unified Telegram bot template
- `chemstack-summary@.service`
  - oneshot sender for the combined ORCA/workflow Telegram summary
- `chemstack-summary@.timer`
  - runs the combined summary every 6 hours
- `chemstack-flow-workflow-worker.service`
  - supervised workflow worker for `chemstack.flow`
- `chemstack-flow-worker.env.example`
  - example environment file for the workflow worker

## Combined runtime target

Use `chemstack-runtime@.target` when you want the unified queue worker and the
unified Telegram bot to start together at boot.

It pulls in:

- `chemstack-queue-worker@.service`
- `chemstack-bot@.service`
- `chemstack-summary@.timer`

Before enabling the combined runtime target:

- Set `telegram.bot_token` and `telegram.chat_id` in `chemstack.yaml`
- Set `workflow.root` in `chemstack.yaml` if you want workflow supervision too

Install the combined runtime target:

```bash
cd <repo_root>
sudo cp systemd/chemstack-queue-worker@.service /etc/systemd/system/
sudo cp systemd/chemstack-bot@.service /etc/systemd/system/
sudo cp systemd/chemstack-summary@.service /etc/systemd/system/
sudo cp systemd/chemstack-summary@.timer /etc/systemd/system/
sudo cp systemd/chemstack-runtime@.target /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now "chemstack-runtime@$(whoami).target"
```

Monitor the combined runtime target:

```bash
systemctl status "chemstack-runtime@$(whoami).target"
systemctl status "chemstack-queue-worker@$(whoami)"
systemctl status "chemstack-bot@$(whoami)"
systemctl status "chemstack-summary@$(whoami).timer"
journalctl -u "chemstack-summary@$(whoami).service" -n 50
journalctl -u "chemstack-queue-worker@$(whoami)" -f
journalctl -u "chemstack-bot@$(whoami)" -f
```

Maintain the combined runtime target:

```bash
sudo systemctl restart "chemstack-runtime@$(whoami).target"
sudo systemctl stop "chemstack-runtime@$(whoami).target"
```

The summary timer runs `chemstack summary` at `00:00`, `06:00`, `12:00`, and `18:00`
local time and sends the combined digest through the shared Telegram settings in
`chemstack.yaml`.

## Engine queue workers

Use `chemstack-queue-worker@.service` as the default worker service. It starts the unified worker supervisor through:

- `python -m chemstack.services.queue_worker`

Common assumptions:

- Repository path is `/home/<user>/chemstack`
- Config path is `/home/<user>/chemstack/config/chemstack.yaml`
- Python path is `/home/<user>/chemstack/.venv/bin/python`
- The unified service runs the ORCA worker by default
- If `workflow.root` is configured, the same service also starts workflow supervision and the internal CREST/xTB workers

Install the unified engine worker:

```bash
cd <repo_root>
sudo cp systemd/chemstack-queue-worker@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now "chemstack-queue-worker@$(whoami)"
```

Use the worker-only service when you do not want the Telegram bot managed by
systemd, or when Telegram is not configured yet.

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

`scheduler.max_active_simulations` in `chemstack.yaml` still caps the combined
number of active simulations across ORCA, internal xTB stages, and internal
CREST stages.

## Flow workflow worker

This worker is for the orchestration loop under `chemstack.flow`, not for engine job execution. Engine workers should use `chemstack-queue-worker@.service`.

If `workflow.root` is set, the unified queue worker already starts workflow supervision together with the internal CREST and xTB workers. Use the dedicated flow service when you specifically want a separate workflow-worker process.

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
- `CHEM_FLOW_WORKFLOW_ROOT` only if you want to override `workflow.root`

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
