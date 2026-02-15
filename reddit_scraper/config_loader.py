import json
import os

def load_event_config(config_path=None):
    if not config_path:
        config_path = os.path.join(os.path.dirname(__file__), "config", "event_config.json")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except Exception:
        return {}
