#!/usr/bin/env bash
# fix.sh — DeepSeek исправляет замечания последнего scan-файла → .ai/data/fix-<тот же хвост>.md
# При успешном фиксе (есть изменения, тесты зелёные) версия в package.json инкрементится и делается git-коммит.
set -euo pipefail

AI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$AI/scripts/ensure_env.sh" || exit 1
ROOT="$(dirname "$AI")"
cd "$ROOT"
mkdir -p "$AI/data"

LAST="$(ls -1 "$AI"/data/scan-*.md 2>/dev/null | sort | tail -n1 || true)"
if [ -z "$LAST" ]; then
  echo "не найден файл scan-YYYYMMDD-XXXX.md"
  exit 0
fi

TAIL="${LAST##*scan-}"          # YYYYMMDD-XXXX.md
OUT="$AI/data/fix-$TAIL"
if [ -f "$OUT" ]; then
  echo "последний scan ($(basename "$LAST")) уже обработан ($(basename "$OUT"))"
  exit 0
fi

python3 "$AI/scripts/fix.py" \
  --dir "$ROOT" --scan "$LAST" --out "$OUT" \
  --agreed "$AI/.agreed.md" --debt "$AI/.debt.md"
