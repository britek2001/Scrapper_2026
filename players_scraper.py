import os
import re
import json
import requests
import shutil
import argparse
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from bs4 import BeautifulSoup
import logging
from urllib.parse import urlparse

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.getcwd(), 'scraper.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Dossier de base pour stocker les données
BASE_DIR = os.path.join(os.getcwd(), "transfermarkt_data")
DATA_DIR = os.path.join(BASE_DIR, "data")
PLAYER_DATA_DIR = os.path.join(DATA_DIR, "players")
PLAYER_HTML_DIR = os.path.join(DATA_DIR, "html")

# Création des dossiers nécessaires
for folder in [BASE_DIR, DATA_DIR, PLAYER_DATA_DIR, PLAYER_HTML_DIR]:
    os.makedirs(folder, exist_ok=True)

# Sous-dossiers par type de page
PAGE_TYPES = {
    'profil': 'profiles',
    'verletzungen': 'injuries',
    'leistungsdaten': 'performance',
    'leistungsdatendetails': 'performance_details',
    'detaillierteleistungsdaten': 'detailed_performance',
    'leistungsdatenverein': 'club_performance',
    'leistungsdatentrainer': 'coach_performance',
    'bilanz': 'balance',
    'elfmetertore': 'penalty_goals',
    'marktwertverlauf': 'market_value',
    'transfers': 'transfers',
    'nationalmannschaft': 'national_team',
    'news': 'news',
    'erfolge': 'achievements',
    'debuets': 'debuts',
    'siege': 'wins',
    'niederlagen': 'losses',
    'meistetore': 'top_goals',
    'meistetorbeteiligungen': 'goal_involvements',
    'rueckennummern': 'kit_numbers'
}

# Création des sous-dossiers pour chaque type
for subfolder in PAGE_TYPES.values():
    os.makedirs(os.path.join(PLAYER_HTML_DIR, subfolder), exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

class TransfermarktScraper:
    def __init__(self, use_playwright: bool = True, delay: float = 1.0):
        self.use_playwright = use_playwright and PLAYWRIGHT_AVAILABLE
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
    def extract_player_id_from_url(self, url: str) -> Optional[str]:
        """Extrait l'ID du joueur depuis l'URL."""
        match = re.search(r'/spieler/(\d+)', url)
        return match.group(1) if match else None
    
    def extract_slug_from_url(self, url: str) -> str:
        """Extrait le slug du joueur depuis l'URL (premier segment du path)."""
        try:
            parsed = urlparse(url)
            segments = [s for s in parsed.path.split('/') if s]
            if segments:
                return segments[0]
        except Exception:
            pass
        return "unknown"
    
    def build_all_page_urls(self, base_url: str) -> Dict[str, str]:
        """Construit les URLs pour toutes les pages associées à un joueur."""
        player_id = self.extract_player_id_from_url(base_url)
        if not player_id:
            logger.error(f"Impossible d'extraire l'ID du joueur depuis: {base_url}")
            return {}
        
        # Extraire le domaine et le slug
        domain_match = re.match(r'(https?://[^/]+)', base_url)
        if not domain_match:
            logger.error(f"URL invalide: {base_url}")
            return {}
        
        domain = domain_match.group(1)
        slug = self.extract_slug_from_url(base_url)
        
        # Construire toutes les URLs
        urls = {}
        base_path = f"{slug}/spieler/{player_id}"
        
        for page_type in PAGE_TYPES.keys():
            # Éviter les doublons
            if page_type in ['leistungsdatentrainer']:  # C'est un doublon dans votre liste
                continue
                
            url = f"{domain}/{base_path}"
            if page_type != 'profil':
                url = f"{domain}/{slug}/{page_type}/spieler/{player_id}"
            urls[page_type] = url
        
        return urls
    
    def fetch_page(self, url: str, page_type: str, player_id: str, slug: str) -> Optional[str]:
        """Récupère le contenu HTML d'une page."""
        # Chemin local pour sauvegarde
        html_filename = f"{slug}_{player_id}_{page_type}.html"
        html_path = os.path.join(PLAYER_HTML_DIR, PAGE_TYPES[page_type], html_filename)
        
        # Vérifier si le fichier existe déjà
        if os.path.exists(html_path):
            logger.info(f"Chargement depuis le cache: {html_path}")
            try:
                with open(html_path, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception as e:
                logger.warning(f"Erreur lors de la lecture du cache: {e}")
        
        # Attendre pour éviter d'être bloqué
        time.sleep(self.delay)
        
        html_content = None
        
        # Essayer avec requests d'abord
        try:
            logger.info(f"Téléchargement de {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Vérifier si la page est valide (pas une page d'erreur)
            if response.status_code == 200:
                html_content = response.text
                
                # Vérifier si c'est une page valide (pas une redirection ou erreur)
                if any(indicator in html_content.lower() for indicator in 
                       ['player not found', 'error', '404', 'seite nicht gefunden']):
                    logger.warning(f"Page invalide ou non trouvée: {url}")
                    html_content = None
        except Exception as e:
            logger.warning(f"Erreur avec requests pour {url}: {e}")
            html_content = None
        
        # Si requests échoue et que playwright est disponible, l'utiliser
        if not html_content and self.use_playwright and page_type not in ['news']:  # Éviter les pages dynamiques trop lourdes
            try:
                html_content = self.fetch_with_playwright(url)
            except Exception as e:
                logger.error(f"Erreur avec playwright pour {url}: {e}")
        
        # Sauvegarder le HTML si récupéré
        if html_content:
            try:
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                logger.info(f"HTML sauvegardé: {html_path}")
            except Exception as e:
                logger.error(f"Erreur lors de la sauvegarde du HTML: {e}")
        
        return html_content
    
    def fetch_with_playwright(self, url: str) -> Optional[str]:
        """Utilise Playwright pour récupérer des pages nécessitant JavaScript."""
        if not PLAYWRIGHT_AVAILABLE:
            return None
            
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                    ]
                )
                
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={'width': 1920, 'height': 1080},
                    locale='fr-FR'
                )
                
                # Ajouter des headers supplémentaires
                context.set_extra_http_headers(HEADERS)
                
                page = context.new_page()
                
                # Naviguer vers la page
                page.goto(url, wait_until="networkidle", timeout=30000)
                
                # Attendre un peu pour le chargement complet
                page.wait_for_timeout(2000)
                
                # Récupérer le HTML
                html_content = page.content()
                
                # Fermer le browser
                browser.close()
                
                return html_content
                
        except PlaywrightTimeoutError:
            logger.error(f"Timeout avec Playwright pour: {url}")
            return None
        except Exception as e:
            logger.error(f"Erreur Playwright: {e}")
            return None
    
    def parse_profile_page(self, html: str) -> Dict[str, Any]:
        """Parse la page de profil du joueur."""
        soup = BeautifulSoup(html, 'html.parser')
        data = {}

        try:
            # Nom du joueur (nettoyage du numéro de maillot)
            name_elem = soup.find('h1', class_='data-header__headline-wrapper')
            if name_elem:
                raw = name_elem.get_text(" ", strip=True)
                # retirer un éventuel "#<num>" au début
                raw = re.sub(r'^#\d+\s*', '', raw)
                data['name'] = raw.strip()
            else:
                title = soup.find('title')
                if title:
                    data['name'] = title.get_text(strip=True).split('-')[0].strip()

            # Club actuel (format: <span class="data-header__club"> <a>Club</a> ...)
            current_club = soup.find('span', class_='data-header__club')
            if current_club:
                a = current_club.find('a')
                if a:
                    data['current_club'] = a.get_text(strip=True)
                else:
                    data['current_club'] = current_club.get_text(strip=True)

            # Numéro de maillot
            shirt_number = soup.find('span', class_='data-header__shirt-number')
            if shirt_number:
                data['shirt_number'] = shirt_number.get_text(strip=True)

            # Récupération des détails sous .data-header__items (âge, nationalité, taille, position, agents...)
            info = {}
            for ul in soup.find_all('ul', class_='data-header__items'):
                for li in ul.find_all('li'):
                    # tenter d'extraire label et valeur
                    label = li.get_text(":", strip=True)
                    # si le li contient un label explicite
                    # on utilise heuristique : split par ":" -> key/value
                    if ':' in label:
                        k, v = [part.strip() for part in label.split(':', 1)]
                        info[k] = v
            if info:
                data['basic_info'] = info

            # Valeur de marché (fallback sur data-header__market-value-wrapper)
            mv = soup.find('a', class_='data-header__market-value-wrapper')
            if mv:
                data['current_market_value'] = mv.get_text(" ", strip=True)

            # position / pied préféré (recherche textuelle)
            for li in soup.find_all('li', class_='data-header__label'):
                txt = li.get_text(" ", strip=True)
                if 'Position' in txt:
                    data['position'] = txt.split(':', 1)[-1].strip()
                if 'Pied' in txt or 'Foot' in txt:
                    data['preferred_foot'] = txt.split(':', 1)[-1].strip()

        except Exception as e:
            logger.error(f"Erreur lors du parsing du profil: {e}")

        return data
    
    def parse_injuries_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse la page des blessures."""
        soup = BeautifulSoup(html, 'html.parser')
        injuries = []
        
        try:
            table = soup.find('table', class_='items')
            if table:
                for row in table.find('tbody').find_all('tr'):
                    cols = row.find_all('td')
                    if len(cols) >= 5:
                        injury = {
                            'season': cols[0].get_text(strip=True),
                            'injury_type': cols[1].get_text(strip=True),
                            'from_date': cols[2].get_text(strip=True),
                            'until_date': cols[3].get_text(strip=True),
                            'days_out': cols[4].get_text(strip=True),
                            'games_missed': cols[5].get_text(strip=True) if len(cols) > 5 else '0'
                        }
                        injuries.append(injury)
            
            # Total des blessures
            total_table = soup.find_all('table', class_='items')
            if len(total_table) > 1:
                totals = {}
                for row in total_table[1].find('tbody').find_all('tr'):
                    cols = row.find_all('td')
                    if len(cols) >= 4:
                        season = cols[0].get_text(strip=True)
                        totals[season] = {
                            'total_days': cols[1].get_text(strip=True),
                            'injuries_count': cols[2].get_text(strip=True),
                            'games_missed': cols[3].get_text(strip=True)
                        }
                
                if totals:
                    return {
                        'injuries_list': injuries,
                        'season_totals': totals
                    }
                    
        except Exception as e:
            logger.error(f"Erreur lors du parsing des blessures: {e}")
        
        return injuries if isinstance(injuries, list) else []
    
    def parse_performance_page(self, html: str, page_type: str) -> Dict[str, Any]:
        """Parse les pages de performance (fallback amélioré pour tables)."""
        soup = BeautifulSoup(html, 'html.parser')
        data = {}

        try:
            # Si page_type est 'bilanz', déléguer au parseur dédié
            if page_type == 'bilanz':
                return {'balance': self.parse_balance_page(html)}

            tables = soup.find_all('table', class_='items')
            if not tables:
                # fallback: chercher n'importe quel <table>
                tables = soup.find_all('table')

            for i, table in enumerate(tables):
                headers = []
                thead = table.find('thead')
                if thead:
                    for th in thead.find_all('th'):
                        text = th.get_text(" ", strip=True)
                        if text == '':
                            # parfois les icônes n'ont pas de text; on ajoute placeholder
                            text = f"col_{len(headers)+1}"
                        headers.append(text)
                # si pas d'en-têtes, on essaie de construire depuis la première ligne du tbody
                tbody = table.find('tbody')
                rows = []
                if tbody:
                    first_row = tbody.find('tr')
                    if first_row and not headers:
                        # construire headers génériques en fonction du nombre de td
                        cols = first_row.find_all(['td', 'th'])
                        headers = [f"col_{j+1}" for j in range(len(cols))]

                    for row in tbody.find_all('tr'):
                        cols = row.find_all('td')
                        if not cols:
                            continue
                        row_data = {}
                        for j, td in enumerate(cols):
                            key = headers[j] if j < len(headers) else f"col_{j+1}"
                            row_data[key] = td.get_text(" ", strip=True)
                        if row_data:
                            rows.append(row_data)

                if rows:
                    data[f'table_{i+1}'] = {'headers': headers, 'rows': rows}

        except Exception as e:
            logger.error(f"Erreur lors du parsing de {page_type}: {e}")

        return data
    
    def parse_balance_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse la page 'bilanz' (bilan) — table listant les clubs et stats contre eux."""
        soup = BeautifulSoup(html, 'html.parser')
        results = []

        try:
            table = soup.find('table', class_='items')
            if not table:
                return results

            tbody = table.find('tbody')
            if not tbody:
                return results

            for row in tbody.find_all('tr'):
                cols = row.find_all('td')
                if not cols:
                    continue

                # structure observée (approximative) :
                # 0: crest/link, 1: club name (short), 2: appearances (maybe link), 3: wins, 4: draws, 5: losses,
                # 6: goals conceded, 7: clean sheets, 8: yellow, 9: yellow-red, 10: red
                entry = {}
                try:
                    # club
                    if len(cols) > 1:
                        club_a = cols[1].find('a')
                        entry['club'] = club_a.get_text(strip=True) if club_a else cols[1].get_text(strip=True)
                    # appearances
                    if len(cols) > 2:
                        entry['appearances'] = cols[2].get_text(strip=True)
                    # results
                    if len(cols) > 3:
                        entry['wins'] = cols[3].get_text(strip=True)
                    if len(cols) > 4:
                        entry['draws'] = cols[4].get_text(strip=True)
                    if len(cols) > 5:
                        entry['losses'] = cols[5].get_text(strip=True)
                    if len(cols) > 6:
                        entry['goals_conceded'] = cols[6].get_text(strip=True)
                    if len(cols) > 7:
                        entry['clean_sheets'] = cols[7].get_text(strip=True)
                    if len(cols) > 8:
                        entry['yellow_cards'] = cols[8].get_text(strip=True)
                    if len(cols) > 9:
                        entry['yellow_red'] = cols[9].get_text(strip=True)
                    if len(cols) > 10:
                        entry['red_cards'] = cols[10].get_text(strip=True)
                except Exception:
                    pass

                if entry:
                    results.append(entry)

        except Exception as e:
            logger.error(f"Erreur lors du parsing de la page bilanz: {e}")

        return results
    
    def parse_achievements_page(self, html: str) -> Dict[str, Any]:
        """Parse la page 'erfolge' (titres & victoires)."""
        soup = BeautifulSoup(html, 'html.parser')
        achievements: Dict[str, Any] = {}

        try:
            # Les blocs principaux sont des .box contenant un h2 (titre) et une table .auflistung
            boxes = soup.find_all('div', class_='box')
            for box in boxes:
                # Titre du bloc (ex: "2x Gardien de la saison", "1x Spanischer Meister", ...)
                h2 = box.find('h2', class_='content-box-headline') or box.find('div', class_='content-box-headline') or box.find('h2')
                title = h2.get_text(strip=True) if h2 else None
                if not title:
                    # certains boxes sont des listes de tous les titres (sidebar), utiliser le premier th si disponible
                    th = box.find('th')
                    title = th.get_text(strip=True) if th else None
                if not title:
                    continue

                items: List[Dict[str, Optional[str]]] = []

                # Chercher d'abord une table avec la classe 'auflistung' sinon prendre la première table
                tbl = box.find('table', class_='auflistung') or box.find('table')
                if not tbl:
                    achievements[title] = items
                    continue

                # Parcourir les lignes de la table en ignorant les en-têtes/groupes
                for tr in tbl.find_all('tr'):
                    tds = tr.find_all('td')
                    if not tds:
                        continue

                    # Extraction robuste : saison dans tds[0], club souvent dans tds[2] (ou 1)
                    season = tds[0].get_text(strip=True) if len(tds) > 0 else ''
                    club = ''
                    club_id: Optional[str] = None

                    # Priorité colonne 2 (index 2), fallback sur index 1
                    target_idx = 2 if len(tds) > 2 else 1 if len(tds) > 1 else None
                    if target_idx is not None:
                        target_td = tds[target_idx]
                        a = target_td.find('a')
                        if a:
                            club = a.get_text(strip=True)
                            href = a.get('href', '')
                            m = re.search(r'/startseite/verein/(\d+)', href)
                            if m:
                                club_id = m.group(1)
                        else:
                            club = target_td.get_text(strip=True)

                    # Nettoyage: éviter lignes de titre répétitives
                    if season == '' and club == '':
                        continue

                    items.append({
                        'season': season,
                        'club': club,
                        'club_id': club_id
                    })

                achievements[title] = items

        except Exception as e:
            logger.error(f"Erreur lors du parsing de la page achievements: {e}")

        return achievements

    def parse_market_value_page(self, html: str) -> List[Dict[str, str]]:
        """Parse l'historique de la valeur de marché."""
        soup = BeautifulSoup(html, 'html.parser')
        values = []
        try:
            table = soup.find('table', class_='items')
            if table and table.find('tbody'):
                for row in table.find('tbody').find_all('tr'):
                    cols = row.find_all('td')
                    # colonnes typiques: date, market_value, club
                    if len(cols) >= 2:
                        date = cols[0].get_text(strip=True)
                        mv = cols[1].get_text(" ", strip=True)
                        club = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                        values.append({'date': date, 'market_value': mv, 'club': club})
        except Exception as e:
            logger.error(f"Erreur lors du parsing des valeurs de marché: {e}")
        return values

    def parse_transfers_page(self, html: str) -> List[Dict[str, str]]:
        """Parse l'historique des transferts."""
        soup = BeautifulSoup(html, 'html.parser')
        transfers = []
        try:
            table = soup.find('table', class_='items')
            if table and table.find('tbody'):
                for row in table.find('tbody').find_all('tr'):
                    cols = row.find_all('td')
                    # structures variables -> extraire les valeurs visibles
                    entry = {}
                    if len(cols) >= 1:
                        entry['season'] = cols[0].get_text(strip=True)
                    if len(cols) >= 2:
                        entry['date'] = cols[1].get_text(strip=True)
                    if len(cols) >= 3:
                        entry['from_club'] = cols[2].get_text(" ", strip=True)
                    if len(cols) >= 4:
                        entry['to_club'] = cols[3].get_text(" ", strip=True)
                    if len(cols) >= 5:
                        entry['market_value'] = cols[4].get_text(strip=True)
                    if len(cols) >= 6:
                        entry['fee'] = cols[5].get_text(strip=True)
                    if entry:
                        transfers.append(entry)
        except Exception as e:
            logger.error(f"Erreur lors du parsing des transferts: {e}")
        return transfers

    def scrape_player(self, base_url: str) -> Dict[str, Any]:
        """Scrape toutes les informations d'un joueur."""
        player_id = self.extract_player_id_from_url(base_url)
        slug = self.extract_slug_from_url(base_url)

        if not player_id:
            logger.error(f"URL invalide, impossible d'extraire l'ID: {base_url}")
            return {}

        logger.info(f"Scraping du joueur: {slug} (ID: {player_id})")
        urls = self.build_all_page_urls(base_url)
        if not urls:
            logger.error(f"Impossible de construire les URLs pour: {base_url}")
            return {}

        player_data = {
            'player_id': player_id,
            'slug': slug,
            'base_url': base_url,
            'scraped_at': datetime.utcnow().isoformat() + 'Z',
            'pages': {}
        }

        for page_type, url in urls.items():
            logger.info(f"  Scraping {page_type}: {url}")
            try:
                html = self.fetch_page(url, page_type, player_id, slug)
                if not html:
                    player_data['pages'][page_type] = {'error': 'Failed to fetch page'}
                    time.sleep(self.delay)
                    continue

                # Sélection du parser
                if page_type == 'profil':
                    page_data = self.parse_profile_page(html)
                elif page_type == 'verletzungen':
                    page_data = self.parse_injuries_page(html)
                elif page_type == 'marktwertverlauf':
                    page_data = self.parse_market_value_page(html)
                elif page_type == 'transfers':
                    page_data = self.parse_transfers_page(html)
                elif page_type == 'bilanz':
                    page_data = self.parse_balance_page(html)
                elif page_type == 'erfolge':
                    page_data = self.parse_achievements_page(html)
                elif page_type in [
                    'leistungsdaten', 'leistungsdatendetails', 'detaillierteleistungsdaten',
                    'leistungsdatenverein', 'leistungsdatentrainer', 'elfmetertore',
                    'meistetore', 'meistetorbeteiligungen', 'rueckennummern',
                    'nationalmannschaft', 'news', 'debuets', 'siege', 'niederlagen'
                ]:
                    page_data = self.parse_performance_page(html, page_type)
                else:
                    # Pour les pages non encore prises en charge, on conserve le HTML sauvegardé
                    page_data = {'html_saved': True, 'url': url}

                player_data['pages'][page_type] = page_data if page_data else {'error': 'No data extracted'}

            except Exception as e:
                logger.error(f"Erreur lors du scraping de {page_type}: {e}")
                player_data['pages'][page_type] = {'error': str(e)}

            time.sleep(self.delay)

        # sauvegarde JSON finale
        self.save_player_json(player_data, slug, player_id)
        return player_data

    def save_player_json(self, data: Dict[str, Any], slug: str, player_id: str):
        """Sauvegarde les données du joueur en JSON."""
        json_filename = f"{slug}_{player_id}_complete.json"
        json_path = os.path.join(PLAYER_DATA_DIR, json_filename)
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Données sauvegardées: {json_path}")
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde JSON: {e}")

    def scrape_all_players(self, player_urls: List[str]):
        """Scrape tous les joueurs de la liste."""
        results = []
        for i, url in enumerate(player_urls, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"Joueur {i}/{len(player_urls)}: {url}")
            logger.info('='*60)
            try:
                player_data = self.scrape_player(url)
                success = bool(player_data.get('pages'))
                results.append({
                    'url': url,
                    'success': success,
                    'player_id': player_data.get('player_id'),
                    'slug': player_data.get('slug')
                })
                self.generate_progress_report(results, i, len(player_urls))
            except Exception as e:
                logger.error(f"Erreur critique pour {url}: {e}")
                results.append({'url': url, 'success': False, 'error': str(e)})
            time.sleep(self.delay * 2)

        self.generate_final_report(results)

    def generate_progress_report(self, results: List[Dict], current: int, total: int):
        """Génère un rapport de progression."""
        success_count = sum(1 for r in results if r.get('success', False))
        logger.info(f"\n📊 Progression: {current}/{total} ({current/total*100:.1f}%)")
        logger.info(f"✅ Succès: {success_count}/{current} ({(success_count/current*100) if current else 0:.1f}%)")

    def generate_final_report(self, results: List[Dict]):
        """Génère un rapport final du scraping."""
        success_count = sum(1 for r in results if r.get('success', False))
        total = len(results)
        success_rate = (success_count / total) * 100 if total > 0 else 0
        logger.info(f"\n{'='*60}")
        logger.info("RAPPORT FINAL DU SCRAPING")
        logger.info('='*60)
        logger.info(f"Total de joueurs: {total}")
        logger.info(f"Scraping réussi: {success_count} ({success_rate:.1f}%)")
        logger.info(f"Échecs: {total - success_count}")

        report_path = os.path.join(BASE_DIR, f"scraping_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'timestamp': datetime.utcnow().isoformat() + 'Z',
                    'total_players': total,
                    'successful_scrapes': success_count,
                    'success_rate': success_rate,
                    'details': results
                }, f, ensure_ascii=False, indent=2)
            logger.info(f"Rapport sauvegardé: {report_path}")
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du rapport: {e}")

        failures = [r for r in results if not r.get('success', False)]
        if failures:
            logger.info("\n❌ Échecs:")
            for fail in failures:
                logger.info(f"  - {fail.get('url', 'URL inconnue')}: {fail.get('error', 'Erreur inconnue')}")


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


def main():
    parser = argparse.ArgumentParser(description="Scraper Transfermarkt complet")
    parser.add_argument("--delay", type=float, default=1.0, help="Délai entre les requêtes en secondes")
    parser.add_argument("--no-playwright", action="store_true", help="Ne pas utiliser Playwright")
    parser.add_argument("--single", type=str, help="Scraper un seul joueur (URL)")
    parser.add_argument("--resume", action="store_true", help="Reprendre depuis le dernier joueur")
    args = parser.parse_args()

    scraper = TransfermarktScraper(use_playwright=not args.no_playwright, delay=args.delay)

    if args.single:
        logger.info(f"Scraping d'un seul joueur: {args.single}")
        scraper.scrape_player(args.single)
        logger.info("Scraping terminé pour le joueur unique")
    else:
        urls = PLAYER_URLS
        if args.resume:
            logger.info("Mode reprise non implémenté, scraping complet")
        logger.info(f"Début du scraping de {len(urls)} joueurs")
        scraper.scrape_all_players(urls)
        logger.info("Scraping terminé!")

if __name__ == "__main__":
    main()

