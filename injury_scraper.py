import requests
import time
import re
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
from bs4 import BeautifulSoup
import cloudscraper  # Alternative to requests that bypasses Cloudflare
from urllib.parse import urlparse, parse_qs

class AdvancedTransfermarktScraper:
    """Advanced Transfermarkt scraper with better parsing logic and error handling"""
    
    def __init__(self, use_cloudscraper=True):
        self.setup_logging()
        self.use_cloudscraper = use_cloudscraper
        
        if use_cloudscraper:
            self.session = cloudscraper.create_scraper(
                browser={
                    'browser': 'chrome',
                    'platform': 'windows',
                    'mobile': False
                }
            )
        else:
            self.session = requests.Session()
        
        # Enhanced headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
            'TE': 'trailers'
        }
        
        self.session.headers.update(self.headers)
        
        # Rate limiting
        self.request_delay = 1.5
        self.last_request_time = 0
        
        # Base URLs with different endpoints
        self.base_url = "https://www.transfermarkt.com"
        self.endpoints = {
            'profile': "/spieler/{id}",
            'injury': "/verletzungen/spieler/{id}",
            'performance': "/leistungsdaten/spieler/{id}",
            'transfer_history': "/transfers/spieler/{id}",
            'timeline': "/profil/spieler/{id}"
        }
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('transfermarkt_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _rate_limit(self):
        """Implement rate limiting between requests"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.request_delay:
            time.sleep(self.request_delay - time_since_last)
        
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, max_retries: int = 3) -> Optional[BeautifulSoup]:
        """Make HTTP request with retry logic and error handling"""
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                
                self.logger.debug(f"Requesting {url} (attempt {attempt + 1}/{max_retries})")
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                
                # Check if we got a valid HTML response
                if not response.text or len(response.text) < 1000:
                    self.logger.warning(f"Short response received from {url}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Check for common error indicators
                error_indicators = [
                    'Access denied',
                    'Cloudflare',
                    'captcha',
                    '403 Forbidden',
                    '404 Not Found'
                ]
                
                page_text = soup.text.lower()
                for indicator in error_indicators:
                    if indicator.lower() in page_text:
                        self.logger.warning(f"Error indicator found: {indicator}")
                        if attempt < max_retries - 1:
                            time.sleep(2 ** attempt)
                            continue
                
                return soup
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
    
    def scrape_player(self, player_id: str) -> Dict:
        """Main method to scrape complete player data"""
        
        self.logger.info(f"Starting scrape for player ID: {player_id}")
        
        player_data = {
            'player_id': player_id,
            'source_urls': {},
            'scraped_at': datetime.now().isoformat(),
            'personal_info': {},
            'club_info': {},
            'market_value': {},
            'injury_history': [],
            'career_stats': [],
            'titles': [],
            'transfers': [],
            'performance_data': []
        }
        
        try:
            # Get all necessary pages
            profile_soup = self._scrape_profile_page(player_id)
            if profile_soup:
                player_data['personal_info'] = self._extract_personal_info(profile_soup)
                player_data['club_info'] = self._extract_club_info(profile_soup)
                player_data['market_value'] = self._extract_market_value(profile_soup)
                player_data['titles'] = self._extract_titles(profile_soup)
            
            # Get performance data page (contains detailed stats)
            performance_soup = self._scrape_performance_page(player_id)
            if performance_soup:
                player_data['career_stats'] = self._extract_detailed_stats(performance_soup)
                player_data['transfers'] = self._extract_transfer_history(performance_soup)
            
            # Get injury data
            injury_soup = self._scrape_injury_page(player_id)
            if injury_soup:
                player_data['injury_history'] = self._extract_injury_history(injury_soup)
            
            self.logger.info(f"Successfully scraped data for player {player_id}")
            
        except Exception as e:
            self.logger.error(f"Error during scrape: {e}")
            # Save partial data if available
            if player_data['personal_info'].get('name') == 'Unknown':
                self.logger.error("Failed to extract any data")
        
        return player_data
    
    def _scrape_profile_page(self, player_id: str) -> Optional[BeautifulSoup]:
        """Scrape the main profile page"""
        url = f"{self.base_url}{self.endpoints['profile'].format(id=player_id)}"
        self.logger.info(f"Scraping profile page: {url}")
        
        soup = self._make_request(url)
        if soup:
            self._save_html_for_debug(soup, f"profile_{player_id}")
            return soup
        return None
    
    def _scrape_performance_page(self, player_id: str) -> Optional[BeautifulSoup]:
        """Scrape the performance data page"""
        url = f"{self.base_url}{self.endpoints['performance'].format(id=player_id)}"
        self.logger.info(f"Scraping performance page: {url}")
        
        soup = self._make_request(url)
        if soup:
            self._save_html_for_debug(soup, f"performance_{player_id}")
            return soup
        return None
    
    def _scrape_injury_page(self, player_id: str) -> Optional[BeautifulSoup]:
        """Scrape the injury history page"""
        url = f"{self.base_url}{self.endpoints['injury'].format(id=player_id)}"
        self.logger.info(f"Scraping injury page: {url}")
        
        soup = self._make_request(url)
        if soup:
            self._save_html_for_debug(soup, f"injury_{player_id}")
            return soup
        return None
    
    def _save_html_for_debug(self, soup: BeautifulSoup, filename: str):
        """Save HTML for debugging purposes"""
        try:
            with open(f'debug_{filename}.html', 'w', encoding='utf-8') as f:
                f.write(soup.prettify())
        except Exception as e:
            self.logger.warning(f"Could not save debug HTML: {e}")
    
    def _extract_personal_info(self, soup: BeautifulSoup) -> Dict:
        """Extract personal information with multiple fallback strategies"""
        info = {
            'name': 'Unknown',
            'full_name': 'Unknown',
            'date_of_birth': None,
            'age': None,
            'place_of_birth': None,
            'nationality': [],
            'height': None,
            'position': [],
            'preferred_foot': None,
            'agent': None,
            'international_caps': 0,
            'international_goals': 0,
            'other_names': []
        }
        
        try:
            # Strategy 1: Try to get name from header
            header = soup.find('h1', class_='data-header__headline-wrapper')
            if header:
                name_text = header.text.strip()
                # Remove shirt number if present
                name_text = re.sub(r'#\d+', '', name_text).strip()
                info['name'] = name_text
            
            # Strategy 2: Fallback to meta tags
            if info['name'] == 'Unknown':
                meta_name = soup.find('meta', property='og:title')
                if meta_name and 'content' in meta_name.attrs:
                    title = meta_name['content']
                    # Extract name from "Name - Profile | Transfermarkt"
                    if '-' in title:
                        info['name'] = title.split('-')[0].strip()
            
            # Extract data from info table
            info_table = soup.find('div', class_='info-table')
            if not info_table:
                info_table = soup.find('table', class_='auflistung')
            
            if info_table:
                rows = info_table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        label = cells[0].text.strip().lower()
                        value = cells[1].text.strip()
                        
                        if 'date of birth' in label or 'geburtstag' in label:
                            info['date_of_birth'], info['age'] = self._parse_date_of_birth(value)
                        
                        elif 'place of birth' in label or 'geburtsort' in label:
                            info['place_of_birth'] = value
                        
                        elif 'citizenship' in label or 'staatsangehörigkeit' in label:
                            # Extract nationalities from flags or text
                            flags = cells[1].find_all('img', class_='flaggenrahmen')
                            if flags:
                                info['nationality'] = [self._extract_country_from_flag(img) for img in flags]
                            else:
                                info['nationality'] = [v.strip() for v in value.split(',')]
                        
                        elif 'height' in label or 'größe' in label:
                            info['height'] = self._parse_height(value)
                        
                        elif 'position' in label:
                            positions = value.split(',')
                            info['position'] = [pos.strip() for pos in positions]
                        
                        elif 'foot' in label or 'fuß' in label:
                            info['preferred_foot'] = value
                        
                        elif 'agent' in label or 'spielerberater' in label:
                            info['agent'] = value
                        
                        elif 'caps/goals' in label or 'länderspiele/tore' in label:
                            caps_goals = self._parse_caps_goals(value)
                            if caps_goals:
                                info['international_caps'], info['international_goals'] = caps_goals
            
            # Additional extraction for full name
            full_name_elem = soup.find('span', string=re.compile('Full name'))
            if full_name_elem:
                full_name_row = full_name_elem.find_parent('tr')
                if full_name_row:
                    full_name_cell = full_name_row.find_all('td')[1]
                    if full_name_cell:
                        info['full_name'] = full_name_cell.text.strip()
            
            # If full name still unknown, use name
            if info['full_name'] == 'Unknown':
                info['full_name'] = info['name']
            
        except Exception as e:
            self.logger.error(f"Error extracting personal info: {e}")
        
        return info
    
    def _parse_date_of_birth(self, value: str) -> Tuple[Optional[str], Optional[int]]:
        """Parse date of birth and age from string"""
        try:
            # Format: "Nov 4, 1998 (25)"
            date_match = re.search(r'([A-Za-z]+\s*\d{1,2},\s*\d{4})', value)
            age_match = re.search(r'\((\d+)\)', value)
            
            date_str = None
            age = None
            
            if date_match:
                date_str = date_match.group(1)
            
            if age_match:
                age = int(age_match.group(1))
            
            return date_str, age
        except:
            return None, None
    
    def _extract_country_from_flag(self, img_element) -> str:
        """Extract country name from flag image"""
        try:
            alt_text = img_element.get('alt', '')
            title_text = img_element.get('title', '')
            
            if alt_text:
                return alt_text
            elif title_text:
                return title_text
            else:
                # Try to extract from src
                src = img_element.get('src', '')
                if 'flagge' in src.lower():
                    match = re.search(r'flagge/([^/]+)', src)
                    if match:
                        return match.group(1).replace('_', ' ').title()
        except:
            pass
        return "Unknown"
    
    def _parse_height(self, value: str) -> str:
        """Parse height from string"""
        # Format: "1,81 m" or "5'11""
        clean_value = value.strip()
        return clean_value
    
    def _parse_caps_goals(self, value: str) -> Optional[Tuple[int, int]]:
        """Parse caps and goals from string"""
        # Format: "24/5" or "24 / 5"
        match = re.search(r'(\d+)\s*/\s*(\d+)', value)
        if match:
            return int(match.group(1)), int(match.group(2))
        return None
    
    def _extract_club_info(self, soup: BeautifulSoup) -> Dict:
        """Extract current club information"""
        club_info = {
            'current_club': 'Unknown',
            'club_nationality': 'Unknown',
            'league': 'Unknown',
            'division': 'Unknown',
            'joined': None,
            'contract_until': None,
            'shirt_number': None
        }
        
        try:
            # Find current club info
            club_box = soup.find('div', class_='data-header__club-info')
            if club_box:
                club_link = club_box.find('a')
                if club_link:
                    club_info['current_club'] = club_link.text.strip()
            
            # Find contract info
            contract_info = soup.find('span', class_='data-header__label', string=re.compile('Contract'))
            if contract_info:
                contract_value = contract_info.find_next('span', class_='data-header__content')
                if contract_value:
                    contract_text = contract_value.text.strip()
                    # Parse "Jun 30, 2029" format
                    club_info['contract_until'] = contract_text
            
            # Find shirt number
            shirt_number = soup.find('div', class_='data-header__shirt-number')
            if shirt_number:
                number_text = shirt_number.text.strip()
                number_match = re.search(r'#\s*(\d+)', number_text)
                if number_match:
                    club_info['shirt_number'] = number_match.group(1)
            
            # Joined date might be in transfer history
            joined_elem = soup.find('span', string=re.compile('Joined'))
            if joined_elem:
                joined_value = joined_elem.find_next('span')
                if joined_value:
                    club_info['joined'] = joined_value.text.strip()
        
        except Exception as e:
            self.logger.error(f"Error extracting club info: {e}")
        
        return club_info
    
    def _extract_market_value(self, soup: BeautifulSoup) -> Dict:
        """Extract market value information"""
        market_value = {
            'current_value': 'Unknown',
            'value_currency': '€',
            'value_history': [],
            'peak_value': 'Unknown',
            'peak_date': 'Unknown'
        }
        
        try:
            # Find current market value
            value_elem = soup.find('div', class_='tm-player-market-value-development__current-value')
            if value_elem:
                value_text = value_elem.text.strip()
                match = re.search(r'([\d.,]+)\s*(\w*)', value_text)
                if match:
                    market_value['current_value'] = match.group(1)
                    if match.group(2):
                        market_value['value_currency'] = match.group(2)
            
            # Try alternative selector
            if market_value['current_value'] == 'Unknown':
                market_elem = soup.find('a', class_='data-header__market-value-wrapper')
                if market_elem:
                    value_text = market_elem.text.strip()
                    match = re.search(r'([\d.,]+)\s*(\w*)', value_text)
                    if match:
                        market_value['current_value'] = match.group(1)
        
        except Exception as e:
            self.logger.error(f"Error extracting market value: {e}")
        
        return market_value
    
    def _extract_detailed_stats(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract detailed career statistics"""
        stats = []
        
        try:
            # Find the main stats table
            table = soup.find('table', class_='items')
            if table:
                rows = table.find_all('tr')[1:]  # Skip header row
                
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) > 5:
                        season_stats = {
                            'season': cells[0].text.strip() if cells[0].text.strip() else None,
                            'club': cells[1].text.strip() if cells[1].text.strip() else None,
                            'competition': cells[2].text.strip() if cells[2].text.strip() else None,
                            'appearances': cells[3].text.strip() if cells[3].text.strip() else '0',
                            'goals': cells[4].text.strip() if cells[4].text.strip() else '0',
                            'assists': cells[5].text.strip() if cells[5].text.strip() else '0'
                        }
                        
                        # Clean up numeric values
                        for key in ['appearances', 'goals', 'assists']:
                            if season_stats[key]:
                                season_stats[key] = re.sub(r'[^\d]', '', season_stats[key])
                                season_stats[key] = int(season_stats[key]) if season_stats[key] else 0
                        
                        stats.append(season_stats)
        
        except Exception as e:
            self.logger.error(f"Error extracting detailed stats: {e}")
        
        return stats
    
    def _extract_transfer_history(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract transfer history"""
        transfers = []
        
        try:
            # Find transfer history table
            table = soup.find('table', class_='transfer-history')
            if not table:
                # Try alternative selector
                table = soup.find('div', id='transfers')
                if table:
                    table = table.find('table')
            
            if table:
                rows = table.find_all('tr')[1:]  # Skip header
                
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 8:
                        transfer = {
                            'season': cells[0].text.strip(),
                            'date': cells[1].text.strip(),
                            'from_club': cells[2].text.strip(),
                            'to_club': cells[3].text.strip(),
                            'market_value': cells[4].text.strip(),
                            'fee': cells[5].text.strip()
                        }
                        transfers.append(transfer)
        
        except Exception as e:
            self.logger.error(f"Error extracting transfer history: {e}")
        
        return transfers
    
    def _extract_injury_history(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract injury history"""
        injuries = []
        
        try:
            injury_table = soup.find('table', class_='items')
            if injury_table:
                rows = injury_table.find_all('tr')[1:]  # Skip header
                
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 5:
                        injury = {
                            'season': cells[0].text.strip(),
                            'injury': cells[1].text.strip(),
                            'from_date': cells[2].text.strip(),
                            'until_date': cells[3].text.strip(),
                            'days_out': cells[4].text.strip(),
                            'games_missed': cells[5].text.strip() if len(cells) > 5 else '0'
                        }
                        injuries.append(injury)
        
        except Exception as e:
            self.logger.error(f"Error extracting injury history: {e}")
        
        return injuries
    
    def _extract_titles(self, soup: BeautifulSoup) -> List[str]:
        """Extract titles/honors"""
        titles = []
        
        try:
            titles_section = soup.find('div', class_='data-header__badge-container')
            if titles_section:
                badges = titles_section.find_all('img', class_='data-header__badge-icon')
                for badge in badges:
                    alt_text = badge.get('alt', '')
                    if alt_text:
                        titles.append(alt_text)
        
        except Exception as e:
            self.logger.error(f"Error extracting titles: {e}")
        
        return titles
    
    def save_as_json(self, player_data: Dict, filename: str = None):
        """Save scraped data as JSON file"""
        if filename is None:
            player_name = player_data['personal_info'].get('name', 'player')
            safe_name = re.sub(r'[^\w\s]', '', player_name).replace(' ', '_')
            filename = f"{safe_name}_{player_data['player_id']}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(player_data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Data saved to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving JSON: {e}")
    
    def save_as_csv(self, player_data: Dict, filename: str = None):
        """Save player data as CSV (flattened for easier analysis)"""
        if filename is None:
            player_name = player_data['personal_info'].get('name', 'player')
            safe_name = re.sub(r'[^\w\s]', '', player_name).replace(' ', '_')
            filename = f"{safe_name}_{player_data['player_id']}.csv"
        
        try:
            # Flatten the data structure
            flat_data = {}
            
            # Basic info
            flat_data['player_id'] = player_data['player_id']
            flat_data['scraped_at'] = player_data['scraped_at']
            
            # Personal info
            for key, value in player_data['personal_info'].items():
                if isinstance(value, list):
                    flat_data[key] = ', '.join(str(v) for v in value)
                else:
                    flat_data[key] = value
            
            # Club info
            for key, value in player_data['club_info'].items():
                flat_data[f'club_{key}'] = value
            
            # Market value
            for key, value in player_data['market_value'].items():
                if key == 'value_history':
                    continue
                flat_data[f'value_{key}'] = value
            
            # Create CSV
            import csv
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=flat_data.keys())
                writer.writeheader()
                writer.writerow(flat_data)
            
            self.logger.info(f"CSV data saved to {filename}")
            
        except Exception as e:
            self.logger.error(f"Error saving CSV: {e}")


# Usage example
if __name__ == "__main__":
    # Initialize scraper
    scraper = AdvancedTransfermarktScraper(use_cloudscraper=True)
    
    # Scrape a player
    player_id = "398073"  # Achraf Hakimi
    
    try:
        player_data = scraper.scrape_player(player_id)
        
        # Save results
        scraper.save_as_json(player_data)
        scraper.save_as_csv(player_data)
        
        # Print summary
        print(f"\n✅ Successfully scraped: {player_data['personal_info'].get('name')}")
        print(f"📊 Appearances: {sum(int(stat['appearances']) for stat in player_data['career_stats'] if stat['appearances'])}")
        print(f"⚽ Goals: {sum(int(stat['goals']) for stat in player_data['career_stats'] if stat['goals'])}")
        print(f"🔄 Transfers: {len(player_data['transfers'])}")
        print(f"🏥 Injuries: {len(player_data['injury_history'])}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        scraper.logger.error(f"Main execution failed: {e}")