#!/usr/bin/env bash
# Installs cron entries for summary digest, event monitor, and organize.
# Uses marker-based block replacement for idempotent operation.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MARKER_START="# ORCA_AUTO_CRON_START"
MARKER_END="# ORCA_AUTO_CRON_END"
LEGACY_SUMMARY_START="# ORCA_AUTO_DFT_SUMMARY_CRON_START"
LEGACY_SUMMARY_END="# ORCA_AUTO_DFT_SUMMARY_CRON_END"

BLOCK="${MARKER_START}
0 9,21 * * * ${ROOT}/scripts/cron_dft_summary.sh
0 0 * * 6 ${ROOT}/scripts/cron_organize.sh
0 * * * * ${ROOT}/scripts/cron_dft_monitor.sh
${MARKER_END}"

# Make scripts executable
chmod +x "$ROOT/scripts/cron_dft_summary.sh"
chmod +x "$ROOT/scripts/cron_organize.sh"
chmod +x "$ROOT/scripts/cron_dft_monitor.sh"

# Remove existing marker blocks and insert new one
EXISTING=$(crontab -l 2>/dev/null || true)
CLEANED=$(echo "$EXISTING" | sed "/${MARKER_START}/,/${MARKER_END}/d" | sed "/${LEGACY_SUMMARY_START}/,/${LEGACY_SUMMARY_END}/d")

printf '%s\n%s\n' "$CLEANED" "$BLOCK" | crontab -

echo "[install_cron] Cron entries installed:"
echo "  dft_summary: Twice daily digest (0 9,21 * * *)"
echo "  organize:    Saturday midnight (0 0 * * 6)"
echo "  dft_monitor: Event alerts every hour (0 * * * *)"
echo ""
echo "Current crontab:"
crontab -l
