# systemd assets

This directory is the single home for long-running ChemStack service assets.

## Included units

- `chemstack-runtime@.target`
  - recommended combined runtime target for the unified queue worker and Telegram bot
- `chemstack-queue-worker@.service`
  - recommended unified queue worker template
- `chemstack-bot@.service`
  - unified Telegram bot template

## Combined runtime target

Use `chemstack-runtime@.target` when you want the unified queue worker and the
unified Telegram bot to start together at boot.

It pulls in:

- `chemstack-queue-worker@.service`
- `chemstack-bot@.service`

Before enabling the combined runtime target:

- Set `telegram.bot_token` and `telegram.chat_id` in `chemstack.yaml`
- Set `workflow.root` in `chemstack.yaml` if you want workflow supervision too
- Restrict local config permissions with `chmod 600 config/chemstack.yaml`

Install the combined runtime target:

```bash
cd <repo_root>
chemstack systemd install --user "$(whoami)" --repo "$(pwd)"
```

The installer renders the unit files with the repository path, writes them to
`/etc/systemd/system`, runs `systemctl daemon-reload`, and enables/starts the
right runtime for the current config. If Telegram is not configured yet, it
enables only the queue worker; run the same command again after setting
`telegram.bot_token` and `telegram.chat_id` to enable the full runtime target.

Monitor the combined runtime target:

```bash
chemstack service status
journalctl -u "chemstack-queue-worker@$(whoami)" -f
journalctl -u "chemstack-bot@$(whoami)" -f
```

Maintain the combined runtime target:

```bash
chemstack service restart
sudo systemctl stop "chemstack-runtime@$(whoami).target"
```

## Engine queue workers

Use `chemstack-queue-worker@.service` as the default worker service. It starts the unified worker supervisor through:

- `python -m chemstack.cli queue worker`

Common assumptions:

- Repository path is `/home/<user>/chemstack`
- Config path is `/home/<user>/chemstack/config/chemstack.yaml`
- Python path is `/home/<user>/chemstack/.venv/bin/python`
- The unified service runs the ORCA worker by default
- If `workflow.root` is configured, the same service also starts workflow supervision and the internal CREST/xTB workers

Install the unified engine worker:

```bash
cd <repo_root>
chemstack systemd install --user "$(whoami)" --repo "$(pwd)"
```

Use the worker-only service when you do not want the Telegram bot managed by
systemd, or when Telegram is not configured yet. The installer chooses that
mode automatically while Telegram settings are empty.

Monitor the unified engine worker:

```bash
chemstack service status
journalctl -u "chemstack-queue-worker@$(whoami)" -f
```

Maintain the unified engine worker:

```bash
chemstack service restart
sudo systemctl stop "chemstack-queue-worker@$(whoami)"
```

`scheduler.max_active_simulations` in `chemstack.yaml` still caps the combined
number of active simulations across ORCA, internal xTB stages, and internal
CREST stages.
