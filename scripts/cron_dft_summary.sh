#!/usr/bin/env bash
# Cron script: send ORCA DFT progress summary from allowed_root
# Schedule example: 0 9,21 * * *
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCK_FILE="$ROOT/.cron_dft_summary.lock"
LOG_DIR="$ROOT/logs"
LOG_FILE="$LOG_DIR/cron_dft_summary_$(date +%Y%m%d_%H%M%S).log"
CONFIG_PATH="${ORCA_AUTO_CONFIG:-$ROOT/config/orca_auto.yaml}"

# Load Telegram env vars if available.
[[ -f "$HOME/.orca_auto_telegram_env" ]] && source "$HOME/.orca_auto_telegram_env"
[[ -f "$HOME/.orca_auto_env" ]] && source "$HOME/.orca_auto_env"

# Prevent concurrent execution via flock.
exec 200>"$LOCK_FILE"
flock -n 200 || { echo "[cron_dft_summary] Already running, exiting." >&2; exit 0; }

mkdir -p "$LOG_DIR"

PYTHON_BIN="$ROOT/.venv/bin/python3"
[[ -x "$PYTHON_BIN" ]] || PYTHON_BIN="$(command -v python3)"

echo "[cron_dft_summary] Started at $(date -Iseconds)" | tee -a "$LOG_FILE"
echo "[cron_dft_summary] config=$CONFIG_PATH" | tee -a "$LOG_FILE"

PYTHONPATH="$ROOT:${PYTHONPATH:-}" "$PYTHON_BIN" "$ROOT/scripts/dft_progress_report.py" \
  --config "$CONFIG_PATH" \
  2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}
echo "[cron_dft_summary] Finished at $(date -Iseconds) with exit code $EXIT_CODE" | tee -a "$LOG_FILE"
exit $EXIT_CODE
