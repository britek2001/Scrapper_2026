import os
import re
import json
import requests
import shutil
import argparse
from bs4 import BeautifulSoup
from datetime import datetime
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except Exception:
    PLAYWRIGHT_AVAILABLE = False

URL = "https://www.transfermarkt.fr/achraf-hakimi/verletzungen/spieler/398073"
# dossier absolu pour stocker HTML/JSON générés
DATA_DIR = "/Users/mac/Downloads/web-scraping-master/data"
# nom par défaut de HTML local (sauvegardé dans DATA_DIR)
LOCAL_HTML = os.path.join(DATA_DIR, "hakimi_injuries.html")

# s'assurer que le dossier existe
os.makedirs(DATA_DIR, exist_ok=True)

# sous-dossiers pour trier les résultats
INJURED_DIR = os.path.join(DATA_DIR, "injured_players")
NOT_INJURED_DIR = os.path.join(DATA_DIR, "not_injured_players")
NO_DATA_DIR = os.path.join(DATA_DIR, "no_data_players")
RAW_HTML_DIR = os.path.join(DATA_DIR, "raw_html")
for d in (INJURED_DIR, NOT_INJURED_DIR, NO_DATA_DIR, RAW_HTML_DIR):
    os.makedirs(d, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "fr-FR,fr;q=0.9"
}

# Liste des URLs à scraper (Maroc + Sénégal)
PLAYER_URLS = [
    # --- Morocco ---
    "https://www.transfermarkt.fr/bono/profil/spieler/207834",
    "https://www.transfermarkt.fr/achraf-hakimi/profil/spieler/398073",
    "https://www.transfermarkt.fr/noussair-mazraoui/profil/spieler/340456",
    "https://www.transfermarkt.fr/sofyan-amrabat/profil/spieler/287579",
    "https://www.transfermarkt.fr/azzedine-ounahi/profil/spieler/589433",
    "https://www.transfermarkt.fr/hakim-ziyech/profil/spieler/214113",
    "https://www.transfermarkt.fr/soufiane-boufal/profil/spieler/226428",
    "https://www.transfermarkt.fr/youssef-en-nesyri/profil/spieler/325601",
    "https://www.transfermarkt.fr/romain-saiss/profil/spieler/163478",
    "https://www.transfermarkt.fr/nayef-aguerd/profil/spieler/432369",
    "https://www.transfermarkt.fr/selim-amallah/profil/spieler/513289",
    "https://www.transfermarkt.fr/bilal-el-khannouss/profil/spieler/654982",
    "https://www.transfermarkt.fr/abdelhamid-sabiri/profil/spieler/340394",
    "https://www.transfermarkt.fr/zakaria-aboukhlal/profil/spieler/393238",
    "https://www.transfermarkt.fr/abde-ezzalzouli/profil/spieler/724520",
    "https://www.transfermarkt.fr/amine-harit/profil/spieler/372711",
    "https://www.transfermarkt.fr/samy-mmaee/profil/spieler/288621",
    "https://www.transfermarkt.fr/yahya-attiyat-allah/profil/spieler/578547",
    "https://www.transfermarkt.fr/amine-adli/profil/spieler/639265",

    # --- Senegal ---
    "https://www.transfermarkt.fr/edouard-mendy/profil/spieler/206390",
    "https://www.transfermarkt.fr/kalidou-koulibaly/profil/spieler/192565",
    "https://www.transfermarkt.fr/abdou-diallo/profil/spieler/258299",
    "https://www.transfermarkt.fr/youssouf-sabaly/profil/spieler/199864",
    "https://www.transfermarkt.fr/pape-gueye/profil/spieler/543892",
    "https://www.transfermarkt.fr/nampalys-mendy/profil/spieler/111051",
    "https://www.transfermarkt.fr/idrissa-gana-gueye/profil/spieler/126665",
    "https://www.transfermarkt.fr/ismaila-sarr/profil/spieler/398077",
    "https://www.transfermarkt.fr/sadio-mane/profil/spieler/200512",
    "https://www.transfermarkt.fr/nicolas-jackson/profil/spieler/621671",
    "https://www.transfermarkt.fr/habib-diallo/profil/spieler/372777",
    "https://www.transfermarkt.fr/formose-mendy/profil/spieler/649023",
    "https://www.transfermarkt.fr/moustapha-name/profil/spieler/586262",
    "https://www.transfermarkt.fr/lamine-camara/profil/spieler/918969",
    "https://www.transfermarkt.fr/pathe-ciss/profil/spieler/525578",
    "https://www.transfermarkt.fr/cheikhou-kouyate/profil/spieler/66934",
    "https://www.transfermarkt.fr/moussa-niakhate/profil/spieler/291200",
    "https://www.transfermarkt.fr/fode-ballo-toure/profil/spieler/296422",
    "https://www.transfermarkt.fr/moussa-sy/profil/spieler/1060786",
    "https://www.transfermarkt.fr/pape-matar-sarr/profil/spieler/568693"
]

def load_html(local_path: str = LOCAL_HTML, remote_url: str = URL) -> str:
    """Charge le HTML depuis fichier local si présent, sinon télécharge la page."""
    # si chemin relatif donné, le préfixer avec DATA_DIR
    if not os.path.isabs(local_path):
        local_path = os.path.join(DATA_DIR, local_path)
    if os.path.exists(local_path):
        with open(local_path, 'r', encoding='utf-8') as f:
            return f.read()
    resp = requests.get(remote_url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    html = resp.text
    # sauvegarde locale pour debug
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, 'w', encoding='utf-8') as f:
        f.write(html)
    return html

def fetch_with_playwright(remote_url, local_path):
    with sync_playwright() as p:
        # Use a real-world browser fingerprint
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        # Navigate and wait for the actual table, not just the page load
        page.goto(remote_url, wait_until="networkidle")
        
        # KEY: Sometimes you need to click a consent button or just wait for the table
        try:
            page.wait_for_selector("table.items", timeout=10000)
        except:
            print(f"⚠️ Table not found at {remote_url}. The site might be blocking us.")
            
        html = page.content()
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(html)
        browser.close()
        return html

def _int_from_text(txt: str) -> int:
    if not txt:
        return 0
    m = re.search(r'(\d+)', str(txt))
    return int(m.group(1)) if m else 0

def parse_injuries(html: str) -> dict:
    soup = BeautifulSoup(html, 'html.parser')

    # Player name (header h1) et player_id depuis canonical / og:url
    player_name = None
    h1 = soup.find('h1')
    if h1:
        player_name = h1.get_text(" ", strip=True)
    if not player_name:
        meta_title = soup.find('meta', property='og:title')
        if meta_title and meta_title.get('content'):
            player_name = meta_title['content'].split('-')[0].strip()

    player_id = None
    can = soup.find('link', rel='canonical')
    url_src = (can.get('href') if can else (soup.find('meta', property='og:url') or {}).get('content', '')) or ''
    m = re.search(r'/spieler/(\d+)', url_src) or re.search(r'/spieler/(\d+)', html)
    if m:
        player_id = m.group(1)

    # Trouver tous les tableaux class="items"
    tables = soup.find_all('table', class_='items')

    injuries = []
    summary = []

    # Tableau principal des blessures (chercher le tableau qui contient les headers Saison / Blessure)
    main_table = None
    for t in tables:
        headers = [th.get_text(" ", strip=True).lower() for th in t.select('thead th')]
        if any('saison' in h for h in headers) and any('blessure' in h or 'verletzung' in h for h in headers):
            main_table = t
            break

    if main_table:
        for row in main_table.select('tbody tr'):
            cols = [td.get_text(" ", strip=True) for td in row.find_all('td')]
            if not cols:
                continue
            # Colonnes attendues: saison, blessure, de, jusqu'à, jours, matchs manqués
            season = cols[0] if len(cols) > 0 else None
            injury_type = cols[1] if len(cols) > 1 else None
            from_date = cols[2] if len(cols) > 2 else None
            until_date = cols[3] if len(cols) > 3 else None
            days_text = cols[4] if len(cols) > 4 else ''
            games_text = cols[5] if len(cols) > 5 else ''
            days = _int_from_text(days_text)
            games_missed = _int_from_text(games_text)
            injuries.append({
                "season": season,
                "injury_type": injury_type,
                "from_date": from_date,
                "until_date": until_date,
                "days_out": days,
                "games_missed": games_missed
            })

    # Tableau "Total" (souvent second tableau items) -> résumé par saison
    total_table = None
    # heuristique: second table with header containing "jours" and "Blessures"
    for t in tables:
        headers = [th.get_text(" ", strip=True).lower() for th in t.select('thead th')]
        if any('jours' in h for h in headers) and any('blessures' in h or 'verletzungen' in h for h in headers):
            total_table = t
            break
    # fallback: if not found by header, take second table if exists
    if not total_table and len(tables) > 1:
        total_table = tables[1]

    if total_table:
        for row in total_table.select('tbody tr'):
            cols = [td.get_text(" ", strip=True) for td in row.find_all('td')]
            if not cols:
                continue
            # Colonnes attendues: saison, jours, blessures, matchs manqués
            season = cols[0] if len(cols) > 0 else None
            total_days = _int_from_text(cols[1]) if len(cols) > 1 else 0
            injuries_count = _int_from_text(cols[2]) if len(cols) > 2 else 0
            games_missed = _int_from_text(cols[3]) if len(cols) > 3 else 0
            summary.append({
                "season": season,
                "total_days": total_days,
                "injuries_count": injuries_count,
                "games_missed": games_missed
            })

    # Totaux globaux calculés si nécessaire
    totals = {
        "injuries_count": sum(s.get('injuries_count', 0) for s in summary) if summary else len(injuries),
        "total_days": sum(s.get('total_days', 0) for s in summary) if summary else sum(i.get('days_out', 0) for i in injuries),
        "total_games_missed": sum(s.get('games_missed', 0) for s in summary) if summary else sum(i.get('games_missed', 0) for i in injuries)
    }

    result = {
        "player": {
            "name": player_name or "Unknown",
            "player_id": player_id or None,
            "profile_url": url_src or URL
        },
        "injuries": injuries,
        "summary": summary,
        "totals": totals,
        "scraped_at": datetime.utcnow().isoformat() + 'Z'
    }
    return result

def save_json(data: dict, outpath: str = None) -> str:
    if not outpath:
        name = (data['player'].get('name') or 'player').lower().replace(' ', '_')
        pid = data['player'].get('player_id') or ''
        safe = re.sub(r'[^\w\-_.]', '_', f"{name}_{pid}")
        outpath = os.path.join(DATA_DIR, f"{safe}_injuries.json")
    # s'assurer que le dossier existe (défensive)
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    with open(outpath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return outpath

def _slug_and_id_from_url(url: str) -> tuple[str, str]:
    """Retourne (slug, id) à partir d'une URL Transfermarkt."""
    url = url.rstrip('/')  # enlever slash final
    # retirer un point final si présent (quelques entrées pouvaient l'avoir)
    url = url.rstrip('.')
    m = re.search(r'/([^/]+)/profil/spieler/(\d+)', url)
    if m:
        slug = m.group(1)
        pid = m.group(2)
        return slug, pid
    # fallback: extraire simplement l'id
    m2 = re.search(r'/spieler/(\d+)', url)
    pid = m2.group(1) if m2 else 'unknown'
    slug = url.split('/')[3] if len(url.split('/')) > 3 else f'player_{pid}'
    return slug, pid

def injuries_url_from_profile(url: str) -> str:
    """Convertit une URL profil Transfermarkt en URL 'verletzungen' (blessures)."""
    # remplace /profil/spieler/... par /verletzungen/spieler/...
    u = re.sub(r'/profil/spieler/', '/verletzungen/spieler/', url)
    # si conversion n'a rien changé et contient /spieler/{id}, construire manuellement
    if u == url:
        m = re.search(r'/spieler/(\d+)', url)
        if m:
            pid = m.group(1)
            # garde même domaine et slug si possible
            domain = re.match(r'https?://[^/]+', url).group(0)
            u = f"{domain}/verletzungen/spieler/{pid}"
    # s'assure qu'il n'y a pas de double slash à la fin
    return u.rstrip('/')

def cleanup_data_dir(keep_files):
    """Supprime tous les fichiers dans DATA_DIR sauf ceux listés (chemins relatifs ou absolus)."""
    # normaliser noms de base
    keep_basenames = {os.path.basename(p) for p in keep_files}
    for entry in os.listdir(DATA_DIR):
        path = os.path.join(DATA_DIR, entry)
        if os.path.isdir(path):
            # ne pas toucher aux dossiers de tri
            continue
        if entry in keep_basenames:
            continue
        try:
            os.remove(path)
            print(f"Removed: {path}")
        except Exception as e:
            print(f"Failed to remove {path}: {e}")

def main():
    # Itérer sur tous les joueurs et générer un JSON par joueur (utilise page /verletzungen/)
    # support optionnel de purge via variable d'environnement ou flag CLI
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--clean", nargs="*", help="Liste de fichiers (basenames) à conserver dans DATA_DIR; si absent, aucun nettoyage.")
    args, _ = parser.parse_known_args()

    for url in PLAYER_URLS:
        try:
            slug, pid = _slug_and_id_from_url(url)
            # chemin local dans DATA_DIR
            local_html = os.path.join(DATA_DIR, f"{slug}_{pid}_injuries.html")

            inj_url = injuries_url_from_profile(url)
            print(f"\n🔍 Processing {slug} (ID: {pid}) -> {inj_url}")
            try:
                html = load_html(local_path=local_html, remote_url=inj_url)
            except Exception as e:
                print(f"  ❌ Failed to load injuries page for {slug}: {e}")
                # fallback: essayer construire manuellement une autre forme d'URL
                try:
                    fallback = re.sub(r'/profil/spieler/(\d+)', r'/spieler/\1', url)
                    fallback = injuries_url_from_profile(fallback)
                    print(f"  🔁 Retrying with fallback URL: {fallback}")
                    html = load_html(local_path=local_html, remote_url=fallback)
                except Exception as e2:
                    print(f"  ❌ Fallback also failed for {slug}: {e2}")
                    continue

            data = parse_injuries(html)
            # Si aucun résultat, tenter Playwright pour récupérer le HTML rendu (JS)
            if not data['injuries'] and not data['summary']:
                if PLAYWRIGHT_AVAILABLE:
                    try:
                        print("  ⏳ No injuries found via requests — trying Playwright render...")
                        html = fetch_with_playwright(inj_url, local_html)
                        data = parse_injuries(html)
                        print(f"  🔎 After Playwright: Injuries {len(data['injuries'])}, Summary rows {len(data['summary'])}")
                    except Exception as pe:
                        print(f"  ❌ Playwright fetch failed for {slug}: {pe}")
                else:
                    print("  ⚠️ Playwright not available — install it to render JS pages.")

            # for clarity ensure player_id is set
            if not data['player'].get('player_id'):
                data['player']['player_id'] = pid

            outname = os.path.join(DATA_DIR, f"{slug}_{pid}_injuries.json")
            out = save_json(data, outpath=outname)

            # sauvegarde brute HTML dans raw_html (copie)
            if os.path.exists(local_html):
                try:
                    shutil.copy2(local_html, os.path.join(RAW_HTML_DIR, os.path.basename(local_html)))
                except Exception:
                    pass

            # Trier et déplacer JSON + HTML dans les dossiers appropriés
            if data.get('injuries') or data.get('totals', {}).get('injuries_count', 0) > 0:
                dest_dir = INJURED_DIR
            elif data.get('player', {}).get('name'):
                dest_dir = NOT_INJURED_DIR
            else:
                dest_dir = NO_DATA_DIR

            dest_json = os.path.join(dest_dir, os.path.basename(out))
            try:
                os.replace(out, dest_json)
                out = dest_json
            except Exception:
                # si échec, garder out (déjà écrit)
                pass

            # déplacer le HTML source aussi
            if os.path.exists(local_html):
                dest_html = os.path.join(dest_dir, os.path.basename(local_html))
                try:
                    os.replace(local_html, dest_html)
                except Exception:
                    pass

            print(f"  ✅ JSON saved to: {out}")
            print(f"   • Player: {data['player']['name']} (ID: {data['player']['player_id']})")
            print(f"   • Injuries: {len(data['injuries'])} | Summary rows: {len(data['summary'])}")

        except Exception as e:
            print(f"  ❌ Error processing {url}: {e}")
            continue

    # Si appelé avec --clean, effectuer la purge (passer basenames ou chemins)
    if args.clean is not None:
        keep = args.clean
        print(f"\nCleaning DATA_DIR, keeping: {keep}")
        cleanup_data_dir(keep)

if __name__ == "__main__":
    main()
