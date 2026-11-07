#!/usr/bin/env bash
# ensure_env.sh — проверяет наличие .ai/.env; при отсутствии создаёт дефолтный (без ключей)
# и выходит с кодом 1, не запуская пайплайн: поправьте .env и запустите снова.
# Подключается в начале issue.sh / scan.sh / fix.sh.
set -euo pipefail

AI="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV="$AI/.env"

[ -f "$ENV" ] && exit 0

cat > "$ENV" <<'EOF'
# Настройки пайплайна ./.ai — модели и URL. Ключи API НЕ здесь (они в ~/.hermes/.env).
# ПОПРАВЬТЕ под проект: TEST_CMD / BUILD_CMD (и модели при необходимости).

# --- scan.sh (Opus-ревью) ---
SCAN_API_URL=https://routerai.ru/api/v1/chat/completions
SCAN_MODEL=~anthropic/claude-opus-latest
SCAN_EFFORT=low
SCAN_MAX_TOKENS=24000

# --- fix.sh (DeepSeek-фикс) ---
FIX_API_URL=https://api.deepseek.com/v1/chat/completions
FIX_MODEL=deepseek-v4-pro
FIX_MAX_TOKENS=24000
FIX_MAX_ITERS=2
TEST_CMD=npm test
BUILD_CMD=npm run build

# --- issue.sh (постановка → ТЗ) ---
ISSUE_PREP_MODEL=deepseek-v4-pro
ISSUE_PREP_API_URL=https://api.deepseek.com/v1/chat/completions
ISSUE_AI_MODEL=deepseek-v4-pro
ISSUE_AI_API_URL=https://api.deepseek.com/v1/chat/completions
ISSUE_TECH_MODEL=~anthropic/claude-opus-latest
ISSUE_TECH_API_URL=https://routerai.ru/api/v1/chat/completions
ISSUE_TECH_EFFORT=low
ISSUE_REVIEW_MODEL=openai/gpt-6-astra
ISSUE_REVIEW_API_URL=https://routerai.ru/api/v1/chat/completions
ISSUE_MAX_TOKENS=24000
ISSUE_DEP_DIRS=~/work
EOF

echo "создан дефолтный $ENV — поправьте его (TEST_CMD, BUILD_CMD, модели) и запустите скрипт снова" >&2
exit 1
