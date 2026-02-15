import json
import os

def load_config(path=None):
    if not path:
        path = os.path.join(os.path.dirname(__file__), "config", "urls.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}
