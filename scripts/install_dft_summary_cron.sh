#!/usr/bin/env bash
# Installs cron entry for ORCA DFT progress summary at 09:00 / 21:00 daily.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MARKER_START="# ORCA_AUTO_DFT_SUMMARY_CRON_START"
MARKER_END="# ORCA_AUTO_DFT_SUMMARY_CRON_END"

BLOCK="${MARKER_START}
0 9,21 * * * ${ROOT}/scripts/cron_dft_summary.sh
${MARKER_END}"

chmod +x "$ROOT/scripts/cron_dft_summary.sh" "$ROOT/scripts/dft_progress_report.py"

EXISTING="$(crontab -l 2>/dev/null || true)"
CLEANED="$(echo "$EXISTING" | sed "/${MARKER_START}/,/${MARKER_END}/d")"

if [[ -n "${CLEANED//$'\n'/}" ]]; then
  printf '%s\n%s\n' "$CLEANED" "$BLOCK" | crontab -
else
  printf '%s\n' "$BLOCK" | crontab -
fi

echo "[install_dft_summary_cron] Cron entry installed:"
echo "  dft summary: every day at 09:00 and 21:00 (0 9,21 * * *)"
echo ""
echo "Current crontab:"
crontab -l
