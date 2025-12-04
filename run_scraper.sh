#!/bin/bash
set -euo pipefail

cd /home/pi/scraper_olx || exit 1

# Load .env and export variables (keep comments in .env)
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

# Activate venv if present
if [ -f ./venv/bin/activate ]; then
  # shellcheck disable=SC1091
  . ./venv/bin/activate
fi

# Choose python from venv if available
PYTHON="./venv/bin/python"
if [ ! -x "$PYTHON" ]; then
  PYTHON="$(command -v python3 || command -v python)"
fi

# minimal masked debug to journal (do NOT print full tokens)
echo "ℹ️ Starting scraper; TELEGRAM_BOT_TOKEN present: ${TELEGRAM_BOT_TOKEN:+yes}, TELEGRAM_CHAT_ID present: ${TELEGRAM_CHAT_ID:+yes}"

exec "$PYTHON" main.py