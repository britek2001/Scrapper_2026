import json
import os

def load_matches(config_path=None):
    if not config_path:
        config_path = os.path.join(os.path.dirname(__file__), "config", "matches.json")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("matches", [])
    except Exception:
        return []

def load_search_terms(config_path=None):
    matches = load_matches(config_path)
    terms = []
    for m in matches:
        terms.extend(m.get("terms", []))
    seen = set()
    out = []
    for t in terms:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out
