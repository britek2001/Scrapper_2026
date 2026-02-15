import json
import os

def load_nitter_config(path=None):
    if not path:
        path = os.path.join(os.path.dirname(__file__), "config", "nitter_config.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def load_matches(path=None):
    if not path:
        path = os.path.join(os.path.dirname(__file__), "config", "matches.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("matches", [])
    except Exception:
        return []

def load_search_terms(match_name: str = None, path: str = None):
    if not path:
        path = os.path.join(os.path.dirname(__file__), "config", "matches.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        matches = data.get("matches", [])
        if match_name:
            for m in matches:
                if m.get("name") == match_name:
                    return m.get("queries", []) or []
            return [] 
        queries = []
        for m in matches:
            qs = m.get("queries", [])
            if qs:
                queries.extend(qs)
        seen = set()
        result = []
        for q in queries:
            if q not in seen:
                seen.add(q)
                result.append(q)
        return result
    except Exception:
        return []
