"""
Fetch complete operator list from Brain API and update wq_operators_cleaned.json.

Key mapping:
  API "definition"   → file "operator_syntax"
  API "level"        → file "level" (lowercased)
  API "description"  → file "summary"
  GET /operators/{name} detail → file "detailed_explanation"
"""
import sys, os, json, time, re
from pathlib import Path

# Ensure project root is on the path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
from agent.wq_tools import WQTools, API_BASE

tools = WQTools()
tools.login()
print("[OK] Logged in\n")

# 1. Fetch operator list
resp = tools._session_manager.get(f"{API_BASE}/operators", timeout=30)
all_ops: list[dict] = resp.json()
print(f"Total operators from API: {len(all_ops)}\n")

# 2. Fetch detailed explanation for each
updated: list[dict] = []
for i, op in enumerate(all_ops):
    name = op["name"]
    detail_url = op.get("documentation") or f"/operators/{name}"
    if not detail_url.startswith("http"):
        detail_url = f"{API_BASE}{detail_url}"

    try:
        detail_resp = tools._session_manager.get(detail_url, timeout=15)
        detail_data = detail_resp.json() if detail_resp.status_code == 200 else {}
    except Exception:
        detail_data = {}

    explanation = detail_data.get("description", op.get("description", ""))

    # Build entry matching existing file format
    entry = {
        "operator_syntax": op["definition"],
        "level": op.get("level", "ALL").lower() if op.get("level") else "base",
        "summary": (op.get("description") or "")[:200],
        "detailed_explanation": explanation,
    }
    updated.append(entry)

    if (i + 1) % 10 == 0:
        print(f"  ... fetched {i + 1}/{len(all_ops)}")
    time.sleep(0.5)

# 3. Sort by operator_syntax
updated.sort(key=lambda e: e["operator_syntax"])

# 4. Backup old file and write new
old_path = _PROJECT_ROOT / "wq_operators_cleaned.json"
backup_path = _PROJECT_ROOT / "wq_operators_cleaned.json.bak"
if old_path.exists():
    import shutil
    shutil.copy2(old_path, backup_path)
    print(f"\n[BACKUP] Saved old file as {backup_path.name}")

old_ops = json.loads(old_path.read_text(encoding="utf-8")) if old_path.exists() else []
old_names: set[str] = set()
for op in old_ops:
    m = re.match(r"([A-Za-z_][A-Za-z0-9_]*)", op["operator_syntax"])
    if m:
        old_names.add(m.group(1))

old_path.write_text(json.dumps(updated, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"[OK] Wrote {len(updated)} operators to {old_path.name}")

# 5. Compare
new_names: set[str] = set()
for op in updated:
    m = re.match(r"([A-Za-z_][A-Za-z0-9_]*)", op["operator_syntax"])
    if m:
        new_names.add(m.group(1))

added = new_names - old_names
removed = old_names - new_names

print(f"\n=== Summary ===")
print(f"Old: {len(old_names)} operators  →  New: {len(new_names)} operators")

if added:
    print(f"\nAdded ({len(added)}):")
    for n in sorted(added):
        print(f"  + {n}")

if removed:
    print(f"\nRemoved ({len(removed)}):")
    for n in sorted(removed):
        print(f"  - {n}")

print("\nOperators by level:")
levels: dict[str, int] = {}
for op in updated:
    lvl = op["level"]
    levels[lvl] = levels.get(lvl, 0) + 1
for lvl, count in sorted(levels.items()):
    print(f"  {lvl}: {count}")
