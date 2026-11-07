#!/usr/bin/env python3
"""scan.py — Opus-ревью TypeScript/Node-проекта через RouterAI (архитектор цикла scan/fix).

Настройки из ai/.env: SCAN_API_URL, SCAN_MODEL, SCAN_EFFORT, SCAN_MAX_TOKENS.
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

AI_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_FILE = os.path.join(AI_DIR, ".env")

SYSTEM = (
    "Ты — старший инженер-ревьюер. Разбери код ниже. Выдай структурированное ревью НА РУССКОМ: "
    "(1) критические баги, (2) проблемы корректности, (3) проблемы API и типов, "
    "(4) риски по компонентам/модулям, (5) расхождения кода с README/доками, (6) что сделано хорошо. "
    "По каждому пункту — файл:строка и конкретика, без воды. Код не переписывай, только ревью. "
    "Если передан раздел «ПРИНЯТЫЕ РЕШЕНИЯ ВЛАДЕЛЬЦА» — это сознательные решения владельца, а не баги: "
    "НЕ повторяй их как ошибки и НЕ требуй их пересмотра. "
    "Если передан раздел «ПРИНЯТЫЙ ДОЛГ» — это известные ограничения, которые владелец сознательно "
    "отложил: не выставляй их как новые находки, но можешь упомянуть, если что-то из долга стало критичным. "
    "Если переданы пункты прошлого ревью — отдельным разделом проверь каждый по формату: "
    "ИСПРАВЛЕНО / ЧАСТИЧНО / НЕ ИСПРАВЛЕНО + номер и строка в текущем коде + кратко почему."
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
