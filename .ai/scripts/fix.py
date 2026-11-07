#!/usr/bin/env python3
"""fix.py — DeepSeek исправляет замечания последнего scan-файла (программист цикла scan/fix).

Настройки из .ai/.env: FIX_API_URL, FIX_MODEL, FIX_MAX_TOKENS, FIX_MAX_ITERS, TEST_CMD, BUILD_CMD.
Ключ API — из ~/.hermes/.env (DEEPSEEK_API_KEY).

Модель возвращает строгий JSON:
  {"files":[{"path","content"}], "debt_add":[...], "agreed_add":[...],
   "results":[{"id","status","note"}], "summary":"..."}
После применения прогоняется TEST_CMD; при провале вывод теста возвращается модели
(до FIX_MAX_ITERS полных циклов). При исчерпании итераций изменения откатываются.
При успешном применении (тесты зелёные и есть изменённые файлы) инкрементится версия
в package.json (+1 к третьей триаде; если она стала 10 — +1 ко второй, третья = 0)
и делается git-коммит: изменённые файлы + package.json + fix-отчёт + .agreed/.debt.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request

import price

AI_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_FILE = os.path.join(AI_DIR, ".env")

SYSTEM = (
    "Ты — старший программист. Исправь замечания ревью в коде проекта. "
    "Верни СТРОГО ОДИН JSON-объект (без markdown-обёртки и текста вне JSON):\n"
    '{"files":[{"path":"относительный путь","content":"ПОЛНОЕ новое содержимое файла"}],\n'
    ' "debt_add":["текст нового пункта долга"],\n'
    ' "agreed_add":["текст нового принятого решения"],\n'
    ' "results":[{"id":номер,"status":"статус","note":"краткая причина"}],\n'
    ' "summary":"супер-краткий дайджест (1-2 предложения)"}\n'
    "results — по КАЖДОМУ замечанию из раздела «РЕВЬЮ» (id — номер из заголовка «## N. ТИП»). "
    "status — один из: «исправлено полностью», «исправлено частично», «перенесено в .debt.md», "
    "«перенесено в .agreed.md», «не подтверждено», «не исправлено». "
    "note — только там, где без неё непонятно (не подтверждено / частично / не исправлено / долг / решение); "
    "для «исправлено полностью» note не нужен.\n"
    "Правила: в files только реально меняемые файлы с полным содержимым; "
    "не выдумывай неизменённые части; не трогай node_modules/dist/сгенерированное; "
    "замечание «нет unit-теста на ветку» — исправление, а не долг: напиши тест в test/ и закрой через files; "
    "в debt_add клади только то, что тестом закрыть нельзя (внешние СУБД, тайминги/гонки, приватный API драйверов); "
    "пункт, который не баг или уже решён владельцем, — не меняй, причину клади в agreed_add; "
    "пункт из «ПРИНЯТЫЙ ДОЛГ» — не меняй. "
    "В debt_add/agreed_add давай только текст пункта (без номера и имени файла — их допишет скрипт как «N. scan-файл.md.»). "
    "Пиши только корректный TypeScript."
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
    model = env.get("FIX_MODEL", "deepseek-flash")
    api_url = env.get("FIX_API_URL", "https://api.deepseek.com/v1/chat/completions")
    body = {
        "model": model,
        "max_tokens": int(env.get("FIX_MAX_TOKENS", "100000")),
        "temperature": 0.2,
        "messages": messages,
    }
    req = urllib.request.Request(
        api_url,
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
    price.record_call(data.get("usage", {}), data.get("model") or model, api_url, "F")
    return data["choices"][0]["message"].get("content") or ""


def parse_json(text):
    a, b = text.find("{"), text.rfind("}")
    if a == -1 or b <= a:
        raise ValueError("нет JSON-объекта в ответе модели")
    return json.loads(text[a:b + 1])


VALID_STATUSES = {
    "исправлено полностью", "исправлено частично", "перенесено в .debt.md",
    "перенесено в .agreed.md", "не подтверждено", "не исправлено",
}


def scan_ids(scan_path):
    """Номера замечаний из scan-файла (заголовки «## N. ТИП»), в порядке появления."""
    ids = []
    for line in open(scan_path, encoding="utf-8"):
        m = re.match(r"^## (\d+)\.\s", line)
        if m:
            ids.append(int(m.group(1)))
    return ids


def format_results(ids, results_map, rolled_back):
    """Строки «N. статус [— note]» по номерам из scan; пропущенные — «не обработано»."""
    lines = []
    for n in ids:
        r = results_map.get(n)
        if r is None:
            lines.append(f"{n}. не обработано")
            continue
        status = str(r.get("status") or "не обработано").strip()
        if status not in VALID_STATUSES:
            status = "не обработано"
        if rolled_back and status in ("исправлено полностью", "исправлено частично"):
            status = "не исправлено"
        note = (r.get("note") or "").strip()
        line = f"{n}. {status}"
        if note:
            line += f" — {note}"
        lines.append(line)
    return lines


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


def next_number(path):
    """Следующий свободный номер пункта в файле (макс. «N.» в начале строки + 1)."""
    try:
        text = open(path, encoding="utf-8").read()
    except OSError:
        return 1
    nums = re.findall(r"^(\d+)\. scan-", text, re.MULTILINE)
    return max(int(x) for x in nums) + 1 if nums else 1


def append_numbered(path, items, scan_name):
    """Дописать пункты в формате «N. scan-файл.md. <текст>» со сквозной нумерацией.

    scan_name — имя scan-файла (basename), откуда пункты возникли. Модель отдаёт
    только текст пункта; номер и префикс «scan-файл.md.» дописывает этот код.
    """
    if not items:
        return
    n = next_number(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write("\n")
        for it in items:
            f.write(f"{n}. {scan_name}. {str(it).strip()}\n\n")
            n += 1


def rollback(project, changed):
    for path, existed in changed:
        if existed:
            subprocess.run(["git", "checkout", "--", path], cwd=project, check=False)
        else:
            try:
                os.remove(path)
            except OSError:
                pass


def git(project, *args):
    """git-команда в каталоге проекта; при ошибке печатает stderr, но не падает."""
    r = subprocess.run(["git", *args], cwd=project, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"git {' '.join(args)}: {(r.stderr or '').strip()}", file=sys.stderr)
    return r.returncode


def bump_version(project):
    """Инкремент версии в package.json: +1 к третьей триаде; если она стала 10 — +1 ко второй, третья = 0.

    Возвращает новую версию строкой или None, если package.json нет / версию не распарсить.
    """
    pkg = os.path.join(project, "package.json")
    if not os.path.isfile(pkg):
        return None
    text = open(pkg, encoding="utf-8").read()
    m = re.search(r'"version"\s*:\s*"([^"]+)"', text)
    if not m:
        return None
    try:
        nums = [int(x) for x in m.group(1).split(".")]
    except ValueError:
        return None
    while len(nums) < 3:
        nums.append(0)
    major, minor, patch = nums[0], nums[1], nums[2]
    patch += 1
    if patch == 10:
        patch = 0
        minor += 1
    new_ver = f"{major}.{minor}.{patch}"
    text = text[:m.start(1)] + new_ver + text[m.end(1):]
    with open(pkg, "w", encoding="utf-8") as f:
        f.write(text)
    return new_ver


def commit_fix(project, applied_all, out, args, debt_add, agreed_add, tail, summary, new_ver):
    """Коммит успешного фикса: изменённые файлы + package.json (версия) + fix-отчёт + .agreed/.debt (если менялись)."""
    paths = [os.path.relpath(p, project) for p, _ in applied_all]
    if new_ver:
        paths.append("package.json")
    paths.append(os.path.relpath(out, project))
    if debt_add:
        paths.append(os.path.relpath(args.debt, project))
    if agreed_add:
        paths.append(os.path.relpath(args.agreed, project))
    seen, uniq = set(), []
    for p in paths:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    git(project, "add", "--", *uniq)
    short = " ".join((summary or "исправлены замечания ревью").split())[:100]
    msg = f"fix: {short} [scan-{tail}]"
    if new_ver:
        msg += f" (v{new_ver})"
    git(project, "commit", "-m", msg)


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
    debt_add, agreed_add, summary, results = [], [], "", []
    test_ok = False

    for it in range(1, args.max_iters + 1):
        content = call(messages, env, key)
        try:
            data = parse_json(content)
        except ValueError as e:
            print(f"итерация {it}: {e}", file=sys.stderr)
            data = {"files": [], "debt_add": [], "agreed_add": [], "results": [],
                    "summary": f"ошибка парсинга ответа: {e}"}

        files = data.get("files") or []
        debt_add = data.get("debt_add") or []
        agreed_add = data.get("agreed_add") or []
        results = data.get("results") or []
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

    scan_name = os.path.basename(args.scan)
    append_numbered(args.debt, debt_add, scan_name)
    append_numbered(args.agreed, agreed_add, scan_name)

    ids = scan_ids(args.scan)
    results_map = {}
    for r in results:
        if isinstance(r, dict) and r.get("id") is not None:
            try:
                results_map[int(r["id"])] = r
            except (TypeError, ValueError):
                pass

    tail = scan_name
    if tail.startswith("scan-"):
        tail = tail[len("scan-"):]
    with open(out, "w") as f:
        f.write(f"# fix-{tail}\n\n")
        f.write(f"Вход: {os.path.basename(args.scan)}\n\n")
        if ids:
            f.write("Результаты по замечаниям:\n")
            for line in format_results(ids, results_map, not test_ok):
                f.write(line + "\n")
            f.write("\n")
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

    if test_ok and applied_all:
        new_ver = bump_version(project)
        commit_fix(project, applied_all, out, args, debt_add, agreed_add, tail, summary, new_ver)
        if new_ver:
            print(f"VERSION: {new_ver}")


if __name__ == "__main__":
    main()
