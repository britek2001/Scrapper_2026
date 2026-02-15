import json
import os

def load_tasks(config_path=None):
    if not config_path:
        config_path = os.path.join(os.path.dirname(__file__), "config", "tasks.json")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("tasks", [])
    except Exception:
        return []
