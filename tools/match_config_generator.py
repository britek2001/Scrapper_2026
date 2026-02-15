import json
import argparse
import shutil
from pathlib import Path
from datetime import datetime
from typing import Any

def read_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def write_json(path: Path, data, *, make_backup=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and make_backup:
        backup_file(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def backup_file(path: Path):
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    # place backups in a central .backups directory at project root (tools/..)
    project_root = Path(__file__).resolve().parents[1]
    try:
        rel = path.relative_to(project_root)
    except Exception:
        # fallback: use the file name only if outside project
        rel = Path(path.name)
    bak = project_root / ".backups" / rel.with_name(rel.name + f".bak.{ts}")
    bak.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, bak)
    print(f"Backup created: {bak}")

def uniq_preserve(items):
    seen = set()
    out = []
    for it in items:
        # prefer using the item directly if hashable
        try:
            hash(it)
            key = (0, it)
        except Exception:
            # fallback: use stable json representation for unhashable items
            try:
                key = (1, json.dumps(it, sort_keys=True, ensure_ascii=False))
            except Exception:
                # last resort: use repr
                key = (2, repr(it))
        if key not in seen:
            seen.add(key)
            out.append(it)
    return out

def prompt_list(prompt):
    s = input(f"{prompt}: ").strip()
    if not s:
        return []
    return [p.strip() for p in s.split(";") if p.strip()]

def is_provided(provided_keys, rel_path):
    # check common variants: exact, with/without 'config/' segment
    if rel_path in provided_keys:
        return True
    alt = rel_path.replace("/config/", "/")
    if alt in provided_keys:
        return True
    alt2 = rel_path.replace("/", "/config/", 1) if "/config/" not in rel_path else rel_path
    if alt2 in provided_keys:
        return True
    return False


def deep_merge(a: Any, b: Any):
    if isinstance(a, dict) and isinstance(b, dict):
        out = dict(a)
        for k, v in b.items():
            if k in out:
                out[k] = deep_merge(out[k], v)
            else:
                out[k] = v
        return out
    if isinstance(a, list) and isinstance(b, list):
        combined = list(a) + list(b)
        return uniq_preserve(combined)
    return b


def is_protected_path(rel_path: str) -> bool:
    """Return True for paths we must not modify or backup inside the repo."""
    rel = rel_path.lstrip("/")
    protected = {  }
    if rel in protected:
        return True
    return False

def main():
    p = argparse.ArgumentParser(description="Générateur de configuration de match pour tous les scrapers")
    p.add_argument("--central-config", help="Fichier JSON central (optionnel) qui fournit certaines configs")
    p.add_argument("--match-config", help="Fichier JSON de configuration du match (applique directement les 'configs')")
    p.add_argument("--non-interactive", action="store_true", help="Mode non interactif (limité)")
    p.add_argument("--name", help="Nom du match / identifiant")
    p.add_argument("--queries", help="Mots-clés séparés par ;")
    args = p.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    provided_configs = {}
    if args.central_config:
        central = read_json(args.central_config, {})
        provided_configs = central.get("configs", {}) if isinstance(central, dict) else {}
        central_match = central if isinstance(central, dict) else {}
        print(f"Central config loaded, {len(provided_configs)} provided configs.")

        for rel, cfg in provided_configs.items():
            rel = rel.lstrip("/")
            if is_protected_path(rel):
                print(f"Protected path, skip central apply: {rel}")
                continue
            
        
            target = repo_root / rel
            if "/config/" not in rel:
                parts = rel.split("/", 1)
                if len(parts) == 2:
                    alt_rel = parts[0] + "/config/" + parts[1]
                    alt_target = repo_root / alt_rel
                    if alt_target.exists() or not target.exists():
                        target = alt_target
            target.parent.mkdir(parents=True, exist_ok=True)
            merged = cfg
            write_json(target, merged, make_backup=True)
            print(f"Applied central config: {target}")

    match_input = {}
    if args.match_config:
        match_input = read_json(args.match_config, {})
        if isinstance(match_input, dict):
            print(f"Match config loaded: {args.match_config}")
        else:
            match_input = {}

  
    if args.non_interactive:
        if not args.name:
            print("En mode non-interactif, --name requis.")
            return
        name = args.name
        terms = [q.strip() for q in (args.queries or "").split(";") if q.strip()]
    else:
        print("\n=== Match Config Generator ===\n")
        name = input("Nom du match / identifiant: ").strip()
        if not name:
            print("Nom requis. Abandon.")
            return
        print("\n-- Équipes --")
        team_a_key = input("Clé équipe A: ").strip() or "team_a"
        team_a_name = input("Nom affiché équipe A: ").strip() or team_a_key
        team_a_players = prompt_list("Joueurs équipe A (séparer par `;`)")
        team_a_hashtags = prompt_list("Hashtags équipe A (séparer par `;`)")
        team_b_key = input("Clé équipe B: ").strip() or "team_b"
        team_b_name = input("Nom affiché équipe B: ").strip() or team_b_key
        team_b_players = prompt_list("Joueurs équipe B (séparer par `;`)")
        team_b_hashtags = prompt_list("Hashtags équipe B (séparer par `;`)")

        print("\n-- Recherches / mots-clés --")
        terms = prompt_list("Mots / queries principales (séparer par `;`)")
        player_urls = prompt_list("URLs des pages joueurs (séparer par `;`) (optionnel)")
        scr_input_csv = input("Nom CSV d'entrée pour scrapper_by_url (laisser vide pour défaut): ").strip() or None

        event_date = input("Date événement (dd/mm/yyyy) (laisser vide pour ignorer Reddit): ").strip()
        if event_date:
            days_before = int(input("Jours avant (ex: 3): ").strip() or "0")
            days_after = int(input("Jours après (ex: 2): ").strip() or "0")
            subreddit = input("Subreddit (sans r/): ").strip()
            event_keywords = prompt_list("Mots-clés événement Reddit (séparer par `;`)") or terms[:10]
        else:
            days_before = days_after = 0
            subreddit = ""
            event_keywords = []
        tw_entry = {
            "name": name,
            "queries": terms,
            "teams": {
                team_a_key: {"name": team_a_name, "players": team_a_players, "hashtags": team_a_hashtags},
                team_b_key: {"name": team_b_name, "players": team_b_players, "hashtags": team_b_hashtags},
            }
        }

    if 'tw_entry' not in locals():
        tw_entry = {"name": name, "queries": terms, "teams": {}}

    if not match_input and 'central_match' in locals() and isinstance(central_match, dict):
        name = central_match.get("name") or name
        terms = central_match.get("terms") or central_match.get("queries") or terms
        team_a = central_match.get("team_a")
        team_b = central_match.get("team_b")
        if team_a or team_b:
            ta_key = (team_a.get("key") if isinstance(team_a, dict) else "team_a") if team_a else "team_a"
            tb_key = (team_b.get("key") if isinstance(team_b, dict) else "team_b") if team_b else "team_b"
            tw_entry = {
                "name": name,
                "queries": central_match.get("queries", terms),
                "teams": {
                    ta_key: team_a or {},
                    tb_key: team_b or {}
                }
            }
        else:
            tw_entry = {"name": name, "queries": central_match.get("queries", terms), "teams": {}}

    if match_input:
        name = match_input.get("name") or name
        terms = match_input.get("terms") or match_input.get("queries") or terms
        teams = match_input.get("teams")
        if teams:
            tw_entry = {"name": name, "queries": match_input.get("queries", terms), "teams": teams}
        else:
            team_a = match_input.get("team_a")
            team_b = match_input.get("team_b")
            if team_a or team_b:
                ta_key = (team_a.get("key") if isinstance(team_a, dict) else "team_a") if team_a else "team_a"
                tb_key = (team_b.get("key") if isinstance(team_b, dict) else "team_b") if team_b else "team_b"
                tw_entry = {
                    "name": name,
                    "queries": match_input.get("queries", terms),
                    "teams": {
                        ta_key: team_a or {},
                        tb_key: team_b or {}
                    }
                }

    if match_input and isinstance(match_input.get("configs"), dict):
        for rel, cfg in match_input.get("configs", {}).items():
            rel = rel.lstrip("/")

            if rel == "players_scraper/players.json":
                rel = "players_scraper/config/players.json"
            elif rel == "reddit_scraper/event_config.json":
                rel = "reddit_scraper/config/event_config.json"
            elif rel == "news_scraper_taiwen/tasks.json":
                rel = "news_scraper_taiwen/config/tasks.json"
            elif rel == "news_scraper_britel/queries.json":
                rel = "news_scraper_britel/config/queries.json"
            elif rel == "scrapper_by_url/urls.json":
                rel = "scrapper_by_url/config/urls.json"
            
            target = repo_root / rel
            if is_provided(provided_configs, rel):
                if target.exists():
                    backup_file(target)
                print(f"Skipped update (provided in central): {rel}")
                continue
            # ensure parent dir exists
            target.parent.mkdir(parents=True, exist_ok=True)
            
            # Replace completely instead of merging
            merged = cfg
            write_json(target, merged, make_backup=True)
            print(f"Updated: {target}")

    # Exemple : mise à jour twitter_scraper/config/matches.json
    tw_cfg_rel = "twitter_scraper/config/matches.json"
    tw_cfg_path = repo_root / tw_cfg_rel
    if is_provided(provided_configs, tw_cfg_rel):
        # sauvegarde locale avant d'ignorer (central fourni)
        if tw_cfg_path.exists():
            backup_file(tw_cfg_path)
        print(f"Skipped update (provided in central): {tw_cfg_rel}")
    else:
        # Si match_input contient une config twitter explicite, l'utiliser directement
        if match_input and "twitter_scraper/config/matches.json" in match_input.get("configs", {}):
            # Déjà traité dans la boucle des configs explicites ci-dessus
            pass
        else:
            # Sinon, faire le merge classique pour les entrées générées dynamiquement
            tw_matches = read_json(tw_cfg_path, {"matches": []}).get("matches", [])
            # merge simple: append if absent
            if not any(m.get("name") == name for m in tw_matches):
                tw_matches.append(tw_entry)
            else:
                for m in tw_matches:
                    if m.get("name") == name:
                        m["queries"] = uniq_preserve(m.get("queries", []) + tw_entry.get("queries", []))
                        m.setdefault("teams", {})
                        for k, v in tw_entry.get("teams", {}).items():
                            if k not in m["teams"]:
                                m["teams"][k] = v
                            else:
                                m["teams"][k]["players"] = uniq_preserve(m["teams"][k].get("players", []) + v.get("players", []))
                                m["teams"][k]["hashtags"] = uniq_preserve(m["teams"][k].get("hashtags", []) + v.get("hashtags", []))
            write_json(tw_cfg_path, {"matches": tw_matches}, make_backup=True)
            print(f"Updated: {tw_cfg_path}")

    # Exemple : lenny_and_others/config/matches.json (terms)
    lenny_cfg_rel = "lenny_and_others/config/matches.json"
    lenny_cfg_path = repo_root / lenny_cfg_rel
    if is_provided(provided_configs, lenny_cfg_rel):
        if lenny_cfg_path.exists():
            backup_file(lenny_cfg_path)
        print(f"Skipped update (provided in central): {lenny_cfg_rel}")
    else:
        lenny_data = read_json(lenny_cfg_path, {"matches": []})
        lenny_matches = lenny_data.get("matches", [])
        new_fb_match = {"name": name, "terms": terms}
        if not any(m.get("name") == name for m in lenny_matches):
            lenny_matches.append(new_fb_match)
        else:
            for m in lenny_matches:
                if m.get("name") == name:
                    m["terms"] = uniq_preserve(m.get("terms", []) + terms)
        lenny_data["matches"] = lenny_matches
        write_json(lenny_cfg_path, lenny_data, make_backup=True)
        print(f"Updated: {lenny_cfg_path}")


    players_rel = "players_scraper/config/players.json"
    players_path = repo_root / players_rel
    if not is_provided(provided_configs, players_rel) and not is_protected_path(players_rel):
        players_cfg = read_json(players_path, {"player_urls": []})
        new_urls = match_input.get("player_urls") or []
        if new_urls:
            players_cfg["player_urls"] = uniq_preserve(players_cfg.get("player_urls", []) + new_urls)
            write_json(players_path, players_cfg, make_backup=True)
            print(f"Updated: {players_path}")

    reddit_rel = "reddit_scraper/config/event_config.json"
    reddit_path = repo_root / reddit_rel
    if not is_provided(provided_configs, reddit_rel) and not is_protected_path(reddit_rel):
        reddit_cfg = read_json(reddit_path, {})
        if match_input.get("event_date"):
            reddit_cfg.update({
                "event_date": match_input.get("event_date"),
                "days_before": match_input.get("days_before", reddit_cfg.get("days_before", 0)),
                "days_after": match_input.get("days_after", reddit_cfg.get("days_after", 0)),
                "subreddit": match_input.get("subreddit", reddit_cfg.get("subreddit", "")),
                "keywords": match_input.get("event_keywords", reddit_cfg.get("keywords", []))
            })
            write_json(reddit_path, reddit_cfg, make_backup=True)
            print(f"Updated: {reddit_path}")

    sb_rel = "scrapper_by_url/config/urls.json"
    sb_path = repo_root / sb_rel
    if not is_provided(provided_configs, sb_rel) and not is_protected_path(sb_rel):
        sb_cfg = read_json(sb_path, {})
        if match_input.get("input_csv"):
            sb_cfg["input_csv"] = match_input.get("input_csv")
        if match_input.get("queries"):
            sb_cfg["queries"] = match_input.get("queries")
        if match_input.get("urls"):
            sb_cfg["urls"] = match_input.get("urls")
        if sb_cfg:
            write_json(sb_path, sb_cfg, make_backup=True)
            print(f"Updated: {sb_path}")

    print("\nAll configs processed. Vérifiez les fichiers dans chaque dossier `config/`.\n")

if __name__ == "__main__":
    main()
