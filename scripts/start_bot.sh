#!/usr/bin/env bash
# 텔레그램 봇 관리 스크립트.
#
# 사용법:
#   start_bot.sh            # 시작 (이미 실행 중이면 무시)
#   start_bot.sh restart    # 기존 봇 종료 후 재시작
#   start_bot.sh stop       # 봇 종료
#
# ~/.bashrc 에 추가하면 WSL 부팅 시 자동 시작:
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
      # 종료 대기 (최대 3초)
      for _ in 1 2 3; do
        kill -0 "$OLD_PID" 2>/dev/null || break
        sleep 1
      done
    fi
    rm -f "$PID_FILE"
  fi
}

_start_bot() {
  # 이미 실행 중이면 무시
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
