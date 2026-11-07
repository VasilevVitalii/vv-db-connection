#!/usr/bin/env bash
# issue.sh — пайплайн ТЗ: .issue-human.md → prep(доп.инфа от AI) → ai → tech → review.
# Логика — в .ai/scripts/issue.py; настройки моделей — в .ai/.env.
set -euo pipefail
AI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$AI/scripts/ensure_env.sh" || exit 1
ROOT="$(dirname "$AI")"
HUMAN="$AI/.issue-human.md"

if [ ! -f "$HUMAN" ]; then
  echo "не найден $HUMAN" >&2
  exit 1
fi

exec python3 "$AI/scripts/issue.py" --dir "$ROOT" --human "$HUMAN" "$@"
