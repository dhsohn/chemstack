#!/usr/bin/env bash
# WSL 시작 시 텔레그램 봇을 백그라운드로 실행한다.
# 이미 실행 중이면 중복 실행하지 않는다.
#
# 사용법:
#   ~/.bashrc 에 추가:  ~/orca_auto/scripts/start_bot.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$ROOT/logs"
PID_FILE="$ROOT/.bot.pid"
CONFIG_PATH="${ORCA_AUTO_CONFIG:-$ROOT/config/orca_auto.yaml}"

# 이미 실행 중인지 확인
if [[ -f "$PID_FILE" ]]; then
  OLD_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$OLD_PID" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
    exit 0
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
