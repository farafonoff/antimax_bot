#!/usr/bin/env bash
# Launch the MAX <-> Telegram bridge.
set -euo pipefail

cd "$(dirname "$0")"

# Fresh setup if the virtualenv is missing.
if [ ! -d .venv ]; then
  echo ">> Creating virtualenv and installing dependencies..."
  python3 -m venv .venv
  .venv/bin/python -m pip install --upgrade pip
  .venv/bin/pip install -r requirements.txt
fi

# Ensure runtime dirs exist.
mkdir -p cache data

# Make sure a config exists.
if [ ! -f .env ]; then
  echo ">> No .env found. Copying .env.example — edit it with your secrets."
  cp .env.example .env
  echo ">> Edit .env, then run this script again:  ./run.sh"
  exit 1
fi

exec .venv/bin/python main.py "$@"
