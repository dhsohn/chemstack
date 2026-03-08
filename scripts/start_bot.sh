#!/usr/bin/env bash
# Telegram bot management script.
#
# Usage:
#   start_bot.sh            # Start (ignored if already running)
#   start_bot.sh restart    # Stop existing bot and restart
#   start_bot.sh stop       # Stop bot
#
# Add to ~/.bashrc for automatic start on WSL boot:
#   ~/orca_auto/scripts/start_bot.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$ROOT/logs"
PID_FILE="$ROOT/.bot.pid"
CONFIG_PATH="${ORCA_AUTO_CONFIG:-$ROOT/config/orca_auto.yaml}"
ACTION="${1:-start}"

_stop_bot() {
  if [[ -f "$PID_FILE" ]]; then
    OLD_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$OLD_PID" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
      kill "$OLD_PID" 2>/dev/null || true
      # Wait for termination (up to 3 seconds)
      for _ in 1 2 3; do
        kill -0 "$OLD_PID" 2>/dev/null || break
        sleep 1
      done
    fi
    rm -f "$PID_FILE"
  fi
}

_start_bot() {
  # Skip if already running
  if [[ -f "$PID_FILE" ]]; then
    OLD_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$OLD_PID" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
      return 0
    fi
    rm -f "$PID_FILE"
  fi

  PYTHON_BIN="$ROOT/.venv/bin/python3"
  [[ -x "$PYTHON_BIN" ]] || PYTHON_BIN="$(command -v python3)"

  mkdir -p "$LOG_DIR"
  LOG_FILE="$LOG_DIR/bot.log"

  PYTHONPATH="$ROOT:${PYTHONPATH:-}" nohup "$PYTHON_BIN" -m core.cli \
    --config "$CONFIG_PATH" \
    bot \
    >>"$LOG_FILE" 2>&1 &

  echo $! > "$PID_FILE"
}

case "$ACTION" in
  start)
    _start_bot
    ;;
  restart)
    _stop_bot
    _start_bot
    ;;
  stop)
    _stop_bot
    ;;
  *)
    echo "Usage: $0 {start|restart|stop}" >&2
    exit 1
    ;;
esac
