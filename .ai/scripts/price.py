#!/usr/bin/env python3
"""price.py — учёт расхода моделей в .ai/price.csv.

Формат строки CSV: "X1;X2;X3;X4;X5;X6"
  X1 — дата "YYYY-MM-DD HH:MI";
  X2 — тип: "F" (fix) / "S" (scan) / "I" (issue);
  X3 — цена с ведущими нулями слева и справа, напр. "000123.0000";
  X4 — валюта: "USD" (DeepSeek) / "RUR" (RouterAI);
  X5 — провайдер: "DEEPSEEK" / "ROUTERAI";
  X6 — название модели.

Провайдер определяется по api_url: api.deepseek.com → DeepSeek (цену считаем из токенов, USD),
routerai.ru → RouterAI (берём готовый usage.cost в ₽).
"""
import os
from datetime import datetime, timezone

AI_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PRICE_CSV = os.path.join(AI_DIR, "price.csv")

# Цены DeepSeek (официальный API), USD за 1M токенов, кортежи (off-peak, peak).
# Источник: api-docs.deepseek.com — deepseek-prices.md.
DEEPSEEK_PRICES = {
    "deepseek-v4-pro": {"miss": (0.66, 1.32), "hit": (0.022, 0.044), "out": (1.98, 3.96)},
    "deepseek-flash": {"miss": (0.15, 0.30), "hit": (0.003, 0.006), "out": (0.60, 1.20)},
}


def append_price(cost, currency, provider, model, kind):
    """Дописать строку расхода. cost — число в валюте currency."""
    date = datetime.now().strftime("%Y-%m-%d %H:%M")
    line = f"{date};{kind};{cost:011.4f};{currency};{provider};{model}\n"
    with open(PRICE_CSV, "a", encoding="utf-8") as f:
        f.write(line)


def _deepseek_price(model):
    if "flash" in (model or ""):
        return DEEPSEEK_PRICES["deepseek-flash"]
    return DEEPSEEK_PRICES["deepseek-v4-pro"]


def _is_peak(dt):
    """Пик DeepSeek: будни 01:00–04:00 и 06:00–10:00 UTC (×2; китайские праздники не учитываем)."""
    if dt.weekday() >= 5:  # суббота/воскресенье — off-peak
        return False
    h = dt.hour + dt.minute / 60
    return (1.0 <= h < 4.0) or (6.0 <= h < 10.0)


def deepseek_cost_usd(usage, model):
    """Расход DeepSeek в USD: токены × прайс (cache hit/miss, off-peak/peak)."""
    usage = usage or {}
    p = _deepseek_price(model)
    miss = usage.get("prompt_cache_miss_tokens")
    if miss is None:
        miss = usage.get("prompt_tokens", 0) - usage.get("prompt_cache_hit_tokens", 0)
    hit = usage.get("prompt_cache_hit_tokens", 0)
    out = usage.get("completion_tokens", 0)
    i = 1 if _is_peak(datetime.now(timezone.utc)) else 0
    return (miss * p["miss"][i] + hit * p["hit"][i] + out * p["out"][i]) / 1_000_000


def record_call(usage, model, api_url, kind):
    """Определить провайдера по api_url и дописать строку расхода в price.csv."""
    if "deepseek.com" in (api_url or ""):
        append_price(deepseek_cost_usd(usage, model), "USD", "DEEPSEEK", model or "", kind)
    else:
        cost = (usage or {}).get("cost")
        if cost is None:
            return
        append_price(float(cost), "RUR", "ROUTERAI", model or "", kind)
