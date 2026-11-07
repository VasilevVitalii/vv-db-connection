#!/usr/bin/env bash
# scan.sh — Opus-ревью проекта: собрать дамп + решения/долг + прошлый scan → .ai/data/scan-<дата>-<номер>.md
set -euo pipefail

AI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$AI/scripts/ensure_env.sh" || exit 1
ROOT="$(dirname "$AI")"
cd "$ROOT"
mkdir -p "$AI/data"

STAMP="$(date +%Y%m%d)"
seq=0
for f in "$AI"/data/scan-"$STAMP"-*.md; do
  [ -e "$f" ] || continue
  n="${f##*scan-$STAMP-}"; n="${n%.md}"
  n=$((10#$n))
  [ "$n" -gt "$seq" ] && seq="$n"
done
SEQ="$(printf '%04d' $((seq + 1)))"
OUT="$AI/data/scan-$STAMP-$SEQ.md"

# последний scan (для проверки, что его пункты закрыты)
LAST="$(ls -1 "$AI"/data/scan-*.md 2>/dev/null | sort | tail -n1 || true)"

ARGS=(--dir "$ROOT" --out "$OUT" --agreed "$AI/.agreed.md" --debt "$AI/.debt.md")
[ -n "$LAST" ] && ARGS+=(--findings "$LAST")

python3 "$AI/scripts/scan.py" "${ARGS[@]}"
