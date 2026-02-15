import json
import os

def load_player_urls(config_path=None):
    if not config_path:
        config_path = os.path.join(os.path.dirname(__file__), "config", "players.json")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        urls = data.get("player_urls", [])
        # dédupliquer en conservant l'ordre
        seen = set()
        out = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out
    except Exception:
        return []
