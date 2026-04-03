# systemd assets

`orca-auto-queue-worker@.service` is a WSL-friendly template for running the ORCA Auto queue worker under `systemd`.

Quick install:

```bash
cd ~/orca_auto
sudo cp systemd/orca-auto-queue-worker@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now "orca-auto-queue-worker@$(whoami)"
```

Monitoring:

```bash
systemctl status "orca-auto-queue-worker@$(whoami)"
journalctl -u "orca-auto-queue-worker@$(whoami)" -f
```

Maintenance:

```bash
sudo systemctl restart "orca-auto-queue-worker@$(whoami)"
sudo systemctl stop "orca-auto-queue-worker@$(whoami)"
```

Assumptions:

- Repository path is `/home/<user>/orca_auto`
- Config path is `/home/<user>/orca_auto/config/orca_auto.yaml`

If your paths differ, edit the copied unit in `/etc/systemd/system/` before enabling it.
