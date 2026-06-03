#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

find . \( -path ./.git -o -path ./.venv \) -prune -o \
  \( -type d \( \
    -name '__pycache__' -o \
    -name '.pytest_cache' -o \
    -name '.mypy_cache' -o \
    -name '.ruff_cache' -o \
    -name 'htmlcov' -o \
    -name '*.egg-info' \
  \) -prune -exec rm -rf {} + \)

rm -f .coverage .coverage.*
rm -rf build dist
