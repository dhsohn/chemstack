#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if sudo -n true >/dev/null 2>&1; then
  echo "[bootstrap] Installing base packages..."
  sudo apt-get update -y
  sudo apt-get install -y \
    ca-certificates \
    curl \
    python3 \
    python3-pip \
    python3-venv
else
  echo "[bootstrap] Sudo not available. Skipping apt package installation."
fi

chmod +x "$ROOT/scripts/"*.sh

echo "[bootstrap] Checking ORCA Linux binary..."
ORCA_BIN="${ORCA_BIN:-$HOME/opt/orca/orca}"
if [[ -x "$ORCA_BIN" ]]; then
  echo "[bootstrap] ORCA binary found: $ORCA_BIN"
else
  echo "[bootstrap] WARNING: ORCA binary not found at $ORCA_BIN"
  echo "[bootstrap] Set ORCA_BIN env var or install ORCA to ~/opt/orca/"
  echo "[bootstrap] Required dependencies: OpenMPI, BLAS/LAPACK"
fi

echo "[bootstrap] Preparing Python virtual environment..."
if ! python3 -m venv .venv >/dev/null 2>&1; then
  echo "[bootstrap] python3 -m venv unavailable, falling back to virtualenv..."
  python3 -m pip install --user virtualenv
  python3 -m virtualenv .venv
fi
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .

CONFIG="$ROOT/config/chemstack.yaml"
if [[ ! -f "$CONFIG" ]]; then
  cp "$ROOT/config/chemstack.yaml.example" "$CONFIG"
  echo "[bootstrap] Created config/chemstack.yaml from example template."
  echo "[bootstrap] Edit config/chemstack.yaml and replace /path/to/... placeholders before first run."
fi

echo "[bootstrap] Done."
echo "[bootstrap] Next: source .venv/bin/activate"
echo "[bootstrap] Example: python -m chemstack.orca.cli run-dir '/absolute/path/to/orca_runs/<dir>'"
