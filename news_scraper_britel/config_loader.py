import json
import os

def load_queries(config_path=None):
    if not config_path:
        config_path = os.path.join(os.path.dirname(__file__), "config", "queries.json")
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Supporte soit {"queries": [...]} soit une liste au top-level
        raw = data.get("queries", []) if isinstance(data, dict) else data
        queries = []
        for item in raw:
            if isinstance(item, (list, tuple)) and len(item) >= 2 and isinstance(item[0], str):
                queries.append((item[0], item[1]))
            elif isinstance(item, dict) and 'query' in item and 'tag' in item:
                queries.append((item['query'], item['tag']))
        return queries
    except Exception:
        return []
