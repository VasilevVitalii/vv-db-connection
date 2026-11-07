#!/usr/bin/env python3
"""scan.py — Opus-ревью TypeScript/Node-проекта через RouterAI (архитектор цикла scan/fix).

Настройки из .ai/.env: SCAN_API_URL, SCAN_MODEL, SCAN_EFFORT, SCAN_MAX_TOKENS.
Ключ API — из ~/.hermes/.env (HERMES_CUSTOM_ROUTERAI_RU_API_KEY).

Ключевой параметр — reasoning_effort: "low": без него opus-5.5 уходит в runaway-thinking
и сжигает весь max_tokens без единой строки ответа.
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request

import price

AI_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_FILE = os.path.join(AI_DIR, ".env")

SYSTEM = (
    "Ты — старший инженер-ревьюер. Разбери код ниже и верни ТОЛЬКО замечания — реальные проблемы, "
    "которые нужно исправить. Больше ничего.\n"
    "Формат каждого замечания — заголовок «## N. ТИП» и абзац текста под ним:\n"
    "- N — сквозной номер по порядку (1, 2, 3…), в порядке, как нашлись при чтении кода (не сортируй).\n"
    "- ТИП — один из: «Критический баг», «Баг», «Отсутствие теста», «Некорректный README», "
    "«Риск», «Прочее».\n"
    "- В тексте: файл:строка, в чём проблема, и (если не очевидно) как исправить.\n"
    "Правила:\n"
    "- «Баг» включает неверное поведение, неверные/слабые типы и утечки внутреннего API.\n"
    "- Мелочь, которую можно поправить, — тоже замечание (тип «Прочее»).\n"
    "- Если у исправления несколько вариантов — перечисли их и укажи рекомендуемый.\n"
    "- НЕ пиши «что сделано хорошо», «уже исправлено», «проверено и корректно», «код соответствует докам»; "
    "разделов с пустым итогом не создавай.\n"
    "- Если замечаний нет — ответь одной строкой «Замечаний нет».\n"
    "- «ПРИНЯТЫЕ РЕШЕНИЯ ВЛАДЕЛЬЦА» — сознательные решения, не выставляй их замечаниями.\n"
    "- «ПРИНЯТЫЙ ДОЛГ» — известные отложенные ограничения, не выставляй их новыми замечаниями; "
    "упомяни только если что-то из долга стало критичным.\n"
    "- «ПРОШЛОЕ РЕВЬЮ» — исправленные пункты НЕ упоминай; неисправленные/частичные внеси как замечание "
    "с пометкой «не исправлено с прошлого ревью».\n"
    "- Про .agreed.md/.debt.md не предлагай: только если проблему нельзя закрыть кодом, "
    "пометь замечание как кандидата в долг."
)

EXTRA_FILES = ["package.json", "tsconfig.json", "README.md"]


def load_env(path):
    env = {}
    try:
        f = open(path)
    except OSError:
        return env
    for line in f:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def load_key(var):
    env = os.path.expanduser("~/.hermes/.env")
    for line in open(env):
        line = line.strip()
        if line.startswith(var + "="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit(f"не найден {var} в ~/.hermes/.env")


def read_if(path):
    return open(path).read() if path and os.path.isfile(path) else ""


def dump_ts(project):
    parts = []
    for root in ("src", "test"):
        base = os.path.join(project, root)
        if not os.path.isdir(base):
            continue
        for dirpath, _, files in sorted(os.walk(base)):
            for fn in sorted(files):
                if fn.endswith(".ts"):
                    rel = os.path.relpath(os.path.join(dirpath, fn), project)
                    parts.append(f"\n===== FILE: {rel} =====\n")
                    with open(os.path.join(dirpath, fn)) as f:
                        for i, line in enumerate(f, 1):
                            parts.append(f"{i:4}| {line}".rstrip("\n") + "\n")
    for fn in EXTRA_FILES:
        p = os.path.join(project, fn)
        if os.path.isfile(p):
            parts.append(f"\n===== FILE: {fn} =====\n")
            with open(p) as f:
                for i, line in enumerate(f, 1):
                    parts.append(f"{i:4}| {line}".rstrip("\n") + "\n")
    return "".join(parts)


def main():
    env = load_env(ENV_FILE)
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", default=os.getcwd(), help="каталог проекта (по умолчанию cwd)")
    ap.add_argument("--out", required=True, help="куда писать ревью")
    ap.add_argument("--findings", default=None,
                    help="файл с пунктами прошлого ревью для проверки (опционально)")
    ap.add_argument("--agreed", default=os.path.join(AI_DIR, ".agreed.md"),
                    help="принятые решения владельца — не переоткрывать")
    ap.add_argument("--debt", default=os.path.join(AI_DIR, ".debt.md"),
                    help="принятый долг — не выставлять как новые находки")
    ap.add_argument("--model", default=env.get("SCAN_MODEL", "~anthropic/claude-opus-latest"))
    ap.add_argument("--endpoint",
                    default=env.get("SCAN_API_URL", "https://routerai.ru/api/v1/chat/completions"))
    ap.add_argument("--effort", default=env.get("SCAN_EFFORT", "low"))
    ap.add_argument("--max-tokens", type=int, default=int(env.get("SCAN_MAX_TOKENS", "24000")))
    args = ap.parse_args()

    project = os.path.abspath(args.dir)
    out = args.out if os.path.isabs(args.out) else os.path.join(project, args.out)

    user = "## КОД ПАКЕТА\n" + dump_ts(project)
    agreed = read_if(args.agreed)
    debt = read_if(args.debt)
    if agreed:
        user += "\n\n## ПРИНЯТЫЕ РЕШЕНИЯ ВЛАДЕЛЬЦА (сознательные, НЕ переоткрывать)\n" + agreed
    if debt:
        user += "\n\n## ПРИНЯТЫЙ ДОЛГ (известные ограничения, не новые находки)\n" + debt
    if args.findings:
        user += "\n\n## ПРОШЛОЕ РЕВЬЮ (проверить)\n" + open(args.findings).read()

    body = {
        "model": args.model,
        "max_tokens": args.max_tokens,
        "temperature": 0.2,
        "reasoning_effort": args.effort,
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": user},
        ],
    }
    req = urllib.request.Request(
        args.endpoint,
        data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {load_key('HERMES_CUSTOM_ROUTERAI_RU_API_KEY')}",
                 "Content-Type": "application/json"},
        method="POST",
    )
    print("sending review request ...", file=sys.stderr)
    try:
        with urllib.request.urlopen(req, timeout=600) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        print("HTTP", e.code, e.read().decode()[:2000], file=sys.stderr)
        raise SystemExit(1)

    msg = data["choices"][0]["message"]
    content = msg.get("content") or ""
    usage = data.get("usage", {})
    price.record_call(usage, data.get("model") or args.model, args.endpoint, "S")

    with open(out, "w") as f:
        f.write(content)
    print(f"model:   {data.get('model')}")
    print(f"finish:  {data['choices'][0].get('finish_reason')}")
    print(f"usage:   {json.dumps(usage, ensure_ascii=False)}")
    print(f"cost:    {usage.get('cost')} ₽")
    print(f"chars:   {len(content)}")
    print(f"SAVED:   {out}")


if __name__ == "__main__":
    main()
