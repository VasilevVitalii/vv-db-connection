#!/usr/bin/env python3
"""issue.py — пайплайн подготовки ТЗ из постановки .issue-human.md.

Шаги (по порядку, каждый — отдельный вызов модели):
  0 prep   (DeepSeek): извлечь внешние зависимости из постановки → найти README
           каждого пакета (локально node_modules / каталоги проектов → npm/pypi registry) →
           дописать раздел «Дополнительная информация от AI» в конец .issue-human.md
           (только новая информация: разбор API пакетов, без пересказа постановки).
  1 ai     (DeepSeek): .issue-human.md → .issue-ai.md (нетехническое ТЗ).
  2 tech   (Opus):     .issue-human.md + .issue-ai.md → .issue-ai-tech.md (техТЗ).
  3 review (GPT):      .issue-human.md + .issue-ai.md + .issue-ai-tech.md → .issue-ai-review.md.

Настройки из .ai/.env (ISSUE_*). Ключи из ~/.hermes/.env:
  DeepSeek — DEEPSEEK_API_KEY; Opus/GPT — HERMES_CUSTOM_ROUTERAI_RU_API_KEY.
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

import price

AI_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_FILE = os.path.join(AI_DIR, ".env")

README_NAMES = ("README.md", "readme.md", "README", "readme", "README.rst", "README.txt", "Readme.md")
README_CAP = 60000  # символов README на пакет — защита от раздувания промпта
AI_SECTION_HEADER = "## Дополнительная информация от AI"  # маркер доп. раздела в .issue-human.md

PREP_SYSTEM = (
    "Ты — анализатор технической постановки. Извлеки из постановки ниже список ВНЕШНИХ зависимостей: "
    "npm-пакеты, pip-пакеты, внешние библиотеки или сервисы, которые упомянуты или явно подразумеваются. "
    "Верни СТРОГО ОДИН JSON-объект (без markdown-обёртки и текста вне JSON):\n"
    '{"packages":[{"name":"...","type":"npm|pip|other"}]}\n'
    'Если внешних зависимостей нет — верни {"packages":[]}.'
)

SECTION_SYSTEM = (
    "Ты — технический аналитик. Тебе даны постановка задачи и документация (README) используемых пакетов. "
    "Составь раздел «Дополнительная информация от AI» — ТОЛЬКО НОВАЯ информация, которой НЕТ в постановке. "
    "Главное — разбор API каждого пакета: ключевые классы/функции и их сигнатуры, как ими пользоваться, "
    "важные ограничения, предостережения, неочевидные детали. "
    "НЕ пересказывай постановку и не повторяй то, что в ней уже сказано. "
    "Пиши на русском, компактно, подзаголовками по пакетам. Если документация пакета не найдена — укажи это."
)

AI_SYSTEM = (
    "Ты — системный аналитик. Разверни постановку ниже в подробное НЕТЕХНИЧЕСКОЕ техническое задание: "
    "назначение, потребители, возможности, сценарии использования. Без деталей реализации "
    "(язык, библиотеки, схемы данных, API). Раздел «Дополнительная информация от AI» в постановке — "
    "справочный контекст: вплетай факты оттуда по необходимости, не пересказывай его дословно. Пиши на русском."
)

TECH_SYSTEM = (
    "Ты — технический архитектор. На основе постановки и нетехнического ТЗ составь ДЕТАЛЬНОЕ техническое задание: "
    "стек, структура проекта, конфигурация, схемы и типы, алгоритмы, ограничения, открытые вопросы. "
    "API внешних пакетов описывай СТРОГО по разделу «Дополнительная информация от AI» постановки — "
    "не выдумывай сигнатуры. Места, где данных не хватает, выноси в раздел открытых вопросов, а не додумывай. "
    "Пиши на русском."
)

REVIEW_SYSTEM = (
    "Ты — независимый ревьюер. Сравни нетехническое ТЗ и техническое ТЗ, сверь оба с исходной постановкой. "
    "Выдай структурированное ревью НА РУССКОМ: (1) противоречия между документами, "
    "(2) внутренние нестыковки и логические ошибки, (3) неоднозначности, (4) блокеры — что мешает начать реализацию. "
    "По каждому пункту — конкретная ссылка (файл:строка или номер раздела). Отдельным разделом — что сделано хорошо."
)


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
    path = os.path.expanduser("~/.hermes/.env")
    for line in open(path):
        line = line.strip()
        if line.startswith(var + "="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit(f"не найден {var} в ~/.hermes/.env")


def read_if(path):
    return open(path).read() if path and os.path.isfile(path) else ""


def call(model, api_url, key, messages, max_tokens, effort=None):
    body = {"model": model, "max_tokens": max_tokens, "temperature": 0.2, "messages": messages}
    if effort:
        body["reasoning_effort"] = effort
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
    content = data["choices"][0]["message"].get("content") or ""
    price.record_call(data.get("usage", {}), data.get("model") or model, api_url, "I")
    return content, data.get("usage", {})


def parse_json(text):
    a, b = text.find("{"), text.rfind("}")
    if a == -1 or b <= a:
        raise ValueError("нет JSON-объекта в ответе модели")
    return json.loads(text[a:b + 1])


def split_human(text, header):
    idx = text.find(header)
    if idx == -1:
        return text, False
    return text[:idx].rstrip("\n") + "\n", True


def find_readme_local(dirpath):
    for n in README_NAMES:
        p = os.path.join(dirpath, n)
        if os.path.isfile(p):
            return p
    return None


def resolve_readme(name, pkg_type, project, dep_dirs):
    # 1) локально: node_modules/<имя> (учитывая scoped @scope/name)
    if name.startswith("@") and "/" in name:
        scope, pkg = name.split("/", 1)
        node_path = os.path.join(project, "node_modules", scope, pkg)
    else:
        node_path = os.path.join(project, "node_modules", name)
    p = find_readme_local(node_path)
    if p:
        return open(p).read(), f"локально: node_modules/{name}"
    # 2) локально: каталоги проектов-зависимостей (по умолчанию ~/work)
    for d in dep_dirs:
        p = find_readme_local(os.path.join(os.path.expanduser(d), name))
        if p:
            return open(p).read(), f"локально: {d}/{name}"
    # 3) registry
    if pkg_type == "npm":
        url = "https://registry.npmjs.org/" + urllib.parse.quote(name, safe="")
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                d = json.loads(r.read())
        except Exception:
            return None, "registry недоступен"
        readme = d.get("readme") or ""
        return (readme, f"npm registry: {name}") if readme else (None, "readme в registry отсутствует")
    if pkg_type == "pip":
        url = "https://pypi.org/pypi/" + urllib.parse.quote(name, safe="") + "/json"
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                d = json.loads(r.read())
        except Exception:
            return None, "pypi недоступен"
        desc = (d.get("info") or {}).get("description") or ""
        return (desc, f"pypi: {name}") if desc else (None, "описание в pypi отсутствует")
    return None, "тип пакета не поддерживается"


def step_prep(env, project, human_path, header, max_tokens):
    key = load_key("DEEPSEEK_API_KEY")
    model = env.get("ISSUE_PREP_MODEL", "deepseek-v4-pro")
    api = env.get("ISSUE_PREP_API_URL", "https://api.deepseek.com/v1/chat/completions")
    dep_dirs = [p for p in env.get("ISSUE_DEP_DIRS", "~/work").split(":") if p]

    human = open(human_path).read()
    core, _ = split_human(human, header)

    content, _ = call(model, api, key,
                      [{"role": "system", "content": PREP_SYSTEM},
                       {"role": "user", "content": core}], max_tokens)
    try:
        packages = parse_json(content).get("packages", [])
    except ValueError as e:
        print(f"prep: {e}", file=sys.stderr)
        raise SystemExit(1)

    print(f"prep: пакеты — {[p.get('name') for p in packages] or 'нет'}", file=sys.stderr)
    if not packages:
        print("зависимостей не найдено — раздел не добавляется")
        return

    docs = []
    for pkg in packages:
        name = (pkg.get("name") or "").strip()
        ptype = (pkg.get("type") or "npm").strip().lower()
        if not name:
            continue
        text, src = resolve_readme(name, ptype, project, dep_dirs)
        docs.append((name, src, text))

    user = "## ПОСТАНОВКА\n" + core + "\n\n## ДОКУМЕНТАЦИЯ ПАКЕТОВ\n"
    for name, src, text in docs:
        if text:
            user += f"\n### {name} (источник: {src})\n{text[:README_CAP]}\n"
        else:
            user += f"\n### {name}: документация НЕ найдена ({src})\n"

    section, usage = call(model, api, key,
                          [{"role": "system", "content": SECTION_SYSTEM},
                           {"role": "user", "content": user}], max_tokens)
    new_human = core.rstrip("\n") + "\n\n" + header + "\n\n" + section.strip() + "\n"
    with open(human_path, "w") as f:
        f.write(new_human)
    print(f"prep: раздел «{header}» записан в {human_path}")
    print(f"prep: cost {usage.get('cost')} ₽")


def step_ai(env, project, human_path, max_tokens):
    key = load_key("DEEPSEEK_API_KEY")
    model = env.get("ISSUE_AI_MODEL", "deepseek-v4-pro")
    api = env.get("ISSUE_AI_API_URL", "https://api.deepseek.com/v1/chat/completions")
    out = os.path.join(AI_DIR, ".issue-ai.md")
    human = open(human_path).read()
    content, usage = call(model, api, key,
                          [{"role": "system", "content": AI_SYSTEM},
                           {"role": "user", "content": "## ПОСТАНОВКА\n" + human}], max_tokens)
    with open(out, "w") as f:
        f.write(content)
    print(f"ai: {out}  cost {usage.get('cost')} ₽")


def step_tech(env, project, human_path, max_tokens):
    key = load_key("HERMES_CUSTOM_ROUTERAI_RU_API_KEY")
    model = env.get("ISSUE_TECH_MODEL", "~anthropic/claude-opus-latest")
    api = env.get("ISSUE_TECH_API_URL", "https://routerai.ru/api/v1/chat/completions")
    effort = env.get("ISSUE_TECH_EFFORT", "low")
    out = os.path.join(AI_DIR, ".issue-ai-tech.md")
    human = open(human_path).read()
    ai = read_if(os.path.join(AI_DIR, ".issue-ai.md"))
    user = "## ПОСТАНОВКА (с доп. информацией от AI)\n" + human
    if ai:
        user += "\n\n## НЕТЕХНИЧЕСКОЕ ТЗ\n" + ai
    content, usage = call(model, api, key,
                          [{"role": "system", "content": TECH_SYSTEM},
                           {"role": "user", "content": user}], max_tokens, effort=effort)
    with open(out, "w") as f:
        f.write(content)
    print(f"tech: {out}  cost {usage.get('cost')} ₽")


def step_review(env, project, human_path, max_tokens):
    key = load_key("HERMES_CUSTOM_ROUTERAI_RU_API_KEY")
    model = env.get("ISSUE_REVIEW_MODEL", "openai/gpt-6-astra")
    api = env.get("ISSUE_REVIEW_API_URL", "https://routerai.ru/api/v1/chat/completions")
    out = os.path.join(AI_DIR, ".issue-ai-review.md")
    human = open(human_path).read()
    ai = read_if(os.path.join(AI_DIR, ".issue-ai.md"))
    tech = read_if(os.path.join(AI_DIR, ".issue-ai-tech.md"))
    user = "## ПОСТАНОВКА\n" + human
    if ai:
        user += "\n\n## НЕТЕХНИЧЕСКОЕ ТЗ\n" + ai
    if tech:
        user += "\n\n## ТЕХНИЧЕСКОЕ ТЗ\n" + tech
    content, usage = call(model, api, key,
                          [{"role": "system", "content": REVIEW_SYSTEM},
                           {"role": "user", "content": user}], max_tokens)
    with open(out, "w") as f:
        f.write(content)
    print(f"review: {out}  cost {usage.get('cost')} ₽")


def main():
    env = load_env(ENV_FILE)
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", default=os.getcwd(), help="каталог проекта (по умолчанию cwd)")
    ap.add_argument("--human", default=os.path.join(AI_DIR, ".issue-human.md"),
                    help="постановка .issue-human.md")
    ap.add_argument("--only", choices=["prep", "ai", "tech", "review"], default=None,
                    help="выполнить только один шаг")
    args = ap.parse_args()

    project = os.path.abspath(args.dir)
    human_path = args.human if os.path.isabs(args.human) else os.path.join(project, args.human)
    if not os.path.isfile(human_path):
        raise SystemExit(f"не найден {human_path}")

    header = AI_SECTION_HEADER
    max_tokens = int(env.get("ISSUE_MAX_TOKENS", "24000"))

    def run(step):
        return args.only in (None, step)

    if run("prep"):
        step_prep(env, project, human_path, header, max_tokens)
    if run("ai"):
        step_ai(env, project, human_path, max_tokens)
    if run("tech"):
        step_tech(env, project, human_path, max_tokens)
    if run("review"):
        step_review(env, project, human_path, max_tokens)


if __name__ == "__main__":
    main()
