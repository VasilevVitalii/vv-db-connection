#!/usr/bin/env python3
"""fix.py — DeepSeek исправляет замечания последнего scan-файла (программист цикла scan/fix).

Настройки из ai/.env: FIX_API_URL, FIX_MODEL, FIX_MAX_TOKENS, FIX_MAX_ITERS, TEST_CMD, BUILD_CMD.
Ключ API — из ~/.hermes/.env (DEEPSEEK_API_KEY).

Модель возвращает строгий JSON:
  {"files":[{"path","content"}], "debt_add":[...], "agreed_add":[...], "summary":"..."}
После применения прогоняется TEST_CMD; при провале вывод теста возвращается модели
(до FIX_MAX_ITERS полных циклов). При исчерпании итераций изменения откатываются.
"""
import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request

AI_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_FILE = os.path.join(AI_DIR, ".env")

SYSTEM = (
    "Ты — старший программист. Исправь замечания ревью в коде проекта. "
    "Верни СТРОГО ОДИН JSON-объект (без markdown-обёртки и текста вне JSON):\n"
    '{"files":[{"path":"относительный путь","content":"ПОЛНОЕ новое содержимое файла"}],\n'
    ' "debt_add":["новый пункт долга"],\n'
    ' "agreed_add":["новое принятое решение"],\n'
    ' "summary":"супер-краткий дайджест (1-2 предложения)"}\n'
    "Правила: в files только реально меняемые файлы с полным содержимым; "
    "не выдумывай неизменённые части; не трогай node_modules/dist/сгенерированное; "
    "замечание «нет unit-теста на ветку» — исправление, а не долг: напиши тест в test/ и закрой через files; "
    "в debt_add клади только то, что тестом закрыть нельзя (внешние СУБД, тайминги/гонки, приватный API драйверов); "
    "пункт, который не баг или уже решён владельцем, — не меняй, причину клади в agreed_add; "
    "пункт из «ПРИНЯТЫЙ ДОЛГ» — не меняй. Пиши только корректный TypeScript."
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


def call(messages, env, key):
    body = {
        "model": env.get("FIX_MODEL", "deepseek-v4-pro"),
        "max_tokens": int(env.get("FIX_MAX_TOKENS", "24000")),
        "temperature": 0.2,
        "messages": messages,
    }
    req = urllib.request.Request(
        env.get("FIX_API_URL", "https://api.deepseek.com/v1/chat/completions"),
        data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=600) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        print("HTTP", e.code, e.read().decode()[:2000], file=sys.stderr)
        raise SystemExit(1)
    return data["choices"][0]["message"].get("content") or ""


def parse_json(text):
    a, b = text.find("{"), text.rfind("}")
    if a == -1 or b <= a:
        raise ValueError("нет JSON-объекта в ответе модели")
    return json.loads(text[a:b + 1])


def safe_path(project, rel):
    p = os.path.abspath(os.path.join(project, rel))
    if not p.startswith(os.path.abspath(project) + os.sep):
        return None
    parts = os.path.normpath(rel).split(os.sep)
    if "node_modules" in parts or parts[0] in ("dist", "distjs"):
        return None
    return p


def run_gate(cmd, project):
    if not cmd:
        return 0, "(TEST_CMD не задан)"
    try:
        r = subprocess.run(cmd, shell=True, cwd=project, capture_output=True, text=True, timeout=600)
    except subprocess.TimeoutExpired:
        return 1, f"TIMEOUT: {cmd}"
    out = (r.stdout or "") + (r.stderr or "")
    return r.returncode, out[-6000:]


def append_lines(path, items):
    if not items:
        return
    with open(path, "a") as f:
        f.write("\n")
        for it in items:
            f.write(f"- {str(it).strip()}\n")


def rollback(project, changed):
    for path, existed in changed:
        if existed:
            subprocess.run(["git", "checkout", "--", path], cwd=project, check=False)
        else:
            try:
                os.remove(path)
            except OSError:
                pass


def main():
    env = load_env(ENV_FILE)
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", default=os.getcwd(), help="каталог проекта (по умолчанию cwd)")
    ap.add_argument("--scan", required=True, help="scan-файл с замечаниями")
    ap.add_argument("--out", required=True, help="куда писать fix-дайджест")
    ap.add_argument("--agreed", default=os.path.join(AI_DIR, ".agreed.md"))
    ap.add_argument("--debt", default=os.path.join(AI_DIR, ".debt.md"))
    ap.add_argument("--max-iters", type=int, default=int(env.get("FIX_MAX_ITERS", "2")))
    args = ap.parse_args()

    project = os.path.abspath(args.dir)
    out = args.out if os.path.isabs(args.out) else os.path.join(project, args.out)
    key = load_key("DEEPSEEK_API_KEY")

    user_base = (
        "## КОД ПРОЕКТА\n" + dump_ts(project)
        + "\n\n## РЕВЬЮ (замечания, которые надо исправить)\n" + open(args.scan).read()
    )
    agreed = read_if(args.agreed)
    debt = read_if(args.debt)
    if agreed:
        user_base += "\n\n## ПРИНЯТЫЕ РЕШЕНИЯ ВЛАДЕЛЬЦА (НЕ переоткрывать)\n" + agreed
    if debt:
        user_base += "\n\n## ПРИНЯТЫЙ ДОЛГ (не чинить сейчас)\n" + debt

    messages = [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user_base}]

    applied_all = []
    debt_add, agreed_add, summary = [], [], ""
    test_ok = False

    for it in range(1, args.max_iters + 1):
        content = call(messages, env, key)
        try:
            data = parse_json(content)
        except ValueError as e:
            print(f"итерация {it}: {e}", file=sys.stderr)
            data = {"files": [], "debt_add": [], "agreed_add": [],
                    "summary": f"ошибка парсинга ответа: {e}"}

        files = data.get("files") or []
        debt_add = data.get("debt_add") or []
        agreed_add = data.get("agreed_add") or []
        summary = data.get("summary") or summary

        changed = []
        for f in files:
            rel = f.get("path", "")
            p = safe_path(project, rel)
            if not p:
                print(f"пропущен путь вне проекта: {rel!r}", file=sys.stderr)
                continue
            os.makedirs(os.path.dirname(p), exist_ok=True)
            existed = os.path.exists(p)
            with open(p, "w") as fh:
                fh.write(f.get("content", ""))
            changed.append((p, existed))
        applied_all.extend(changed)

        if not changed:
            test_ok = True
            break

        code, tail = run_gate(env.get("TEST_CMD"), project)
        if code == 0:
            test_ok = True
            break
        print(f"итерация {it}: тесты упали (exit {code})", file=sys.stderr)
        messages.append({"role": "assistant", "content": content})
        messages.append({"role": "user",
                         "content": f"Тесты упали (exit {code}). Вывод:\n{tail}\n"
                                    "Исправь и верни тот же JSON."})

    if not test_ok:
        rollback(project, applied_all)
        result = f"**fix не применён** (откат): {summary or 'тесты остались красными'}"
    else:
        result = summary or "без изменений"

    append_lines(args.debt, debt_add)
    append_lines(args.agreed, agreed_add)

    tail = os.path.basename(args.scan)
    if tail.startswith("scan-"):
        tail = tail[len("scan-"):]
    with open(out, "w") as f:
        f.write(f"# fix-{tail}\n\n")
        f.write(f"Вход: {os.path.basename(args.scan)}\n\n")
        f.write(result + "\n\n")
        if applied_all:
            f.write("Изменённые файлы:\n")
            for p, _ in applied_all:
                f.write(f"- {os.path.relpath(p, project)}\n")
        if debt_add:
            f.write("\nДолг (добавлено):\n")
            for x in debt_add:
                f.write(f"- {x}\n")
        if agreed_add:
            f.write("\nРешения (добавлено):\n")
            for x in agreed_add:
                f.write(f"- {x}\n")
        f.write(f"\nТесты: {'OK' if test_ok else 'FAILED (откат)'}\n")

    print(f"SAVED: {out}")


if __name__ == "__main__":
    main()
