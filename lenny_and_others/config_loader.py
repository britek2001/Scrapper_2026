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

def get_match_names(matches):
    return [m.get("name", "Unnamed") for m in matches]

def get_terms_for_match(matches, idx):
    try:
        return matches[idx].get("terms", [])
    except Exception:
        return []
