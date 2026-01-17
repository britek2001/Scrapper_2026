import requests
import json
import logging
from bs4 import BeautifulSoup
from typing import List, Dict
from datetime import datetime

class PlayerScraper:
    """Scraper for extracting player names from club websites"""
    
    def __init__(self):
        self.setup_logging()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        })
    
    def setup_logging(self):
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def scrape_manchester_city_players(self) -> List[Dict]:
        """Scrape Manchester City players"""
        self.logger.info("=" * 60)
        self.logger.info("🔍 SCRAPING: Manchester City")
        self.logger.info("=" * 60)
        start_time = datetime.now()
        players = []
        
        try:
            url = "https://www.mancity.com/players/mens"
            self.logger.info(f"📡 Fetching URL: {url}")
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            self.logger.info(f"✓ Response received (Status: {response.status_code}, Size: {len(response.content)} bytes)")
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find player cards/sections
            player_elements = soup.find_all(['div', 'article', 'a'], class_=lambda x: x and ('player' in x.lower() or 'squad' in x.lower()))
            
            # Try multiple selectors
            selectors = [
                'div.player-card',
                'div.squad-player',
                'article.player',
                'a[href*="/players/"]',
                'div[class*="Player"]',
                'div[class*="player"]'
            ]
            
            for selector in selectors:
                elements = soup.select(selector)
                if elements:
                    self.logger.info(f"Found {len(elements)} elements with selector: {selector}")
                    
                    for elem in elements:
                        # Extract player name
                        name_elem = elem.find(['h2', 'h3', 'h4', 'span', 'p'])
                        if name_elem:
                            player_name = name_elem.get_text(strip=True)
                            
                            # Extract position if available
                            position_elem = elem.find(class_=lambda x: x and 'position' in x.lower())
                            position = position_elem.get_text(strip=True) if position_elem else 'Unknown'
                            
                            # Extract number if available
                            number_elem = elem.find(class_=lambda x: x and 'number' in x.lower())
                            number = number_elem.get_text(strip=True) if number_elem else ''
                            
                            if player_name and len(player_name) > 2:
                                player = {
                                    'club': 'Manchester City',
                                    'name': player_name,
                                    'position': position,
                                    'number': number,
                                    'source': url,
                                    'scraped_at': datetime.now().isoformat()
                                }
                                players.append(player)
                                self.logger.info(f"✅ {player_name} - {position}")
                    
                    if players:
                        break
            
            # If no players found, try text analysis
            if not players:
                self.logger.warning("⚠️ No players found with standard selectors")
                self.logger.info("Trying alternative text extraction...")
                text_content = soup.get_text()
                # Save HTML for debugging
                with open('debug_mancity.html', 'w', encoding='utf-8') as f:
                    f.write(soup.prettify())
                self.logger.info("Saved HTML to debug_mancity.html for inspection")
                
        except Exception as e:
            self.logger.error(f"❌ Error scraping Manchester City: {e}")
        
        elapsed_time = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"⏱️ Scraping completed in {elapsed_time:.2f} seconds")
        self.logger.info(f"📊 Total players found: {len(players)}")
        self.logger.info("=" * 60)
        
        return players
    
    def scrape_bayern_munich_players(self) -> List[Dict]:
        """Scrape Bayern Munich players"""
        self.logger.info("=" * 60)
        self.logger.info("🔍 SCRAPING: Bayern Munich")
        self.logger.info("=" * 60)
        start_time = datetime.now()
        players = []
        
        try:
            url = "https://fcbayern.com/en/teams/first-team"
            self.logger.info(f"📡 Fetching URL: {url}")
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            self.logger.info(f"✓ Response received (Status: {response.status_code}, Size: {len(response.content)} bytes)")
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Try multiple selectors for Bayern
            selectors = [
                'div.player-item',
                'div.squad-player',
                'article.player',
                'a[href*="/player/"]',
                'div[class*="Player"]',
                'div[class*="player"]',
                'div.team-player'
            ]
            
            for selector in selectors:
                elements = soup.select(selector)
                if elements:
                    self.logger.info(f"Found {len(elements)} elements with selector: {selector}")
                    
                    for elem in elements:
                        # Extract player name
                        name_elem = elem.find(['h2', 'h3', 'h4', 'span', 'p', 'div'])
                        if name_elem:
                            player_name = name_elem.get_text(strip=True)
                            
                            # Extract position
                            position_elem = elem.find(class_=lambda x: x and 'position' in x.lower())
                            position = position_elem.get_text(strip=True) if position_elem else 'Unknown'
                            
                            # Extract number
                            number_elem = elem.find(class_=lambda x: x and ('number' in x.lower() or 'shirt' in x.lower()))
                            number = number_elem.get_text(strip=True) if number_elem else ''
                            
                            if player_name and len(player_name) > 2:
                                player = {
                                    'club': 'Bayern Munich',
                                    'name': player_name,
                                    'position': position,
                                    'number': number,
                                    'source': url,
                                    'scraped_at': datetime.now().isoformat()
                                }
                                players.append(player)
                                self.logger.info(f"✅ {player_name} - {position}")
                    
                    if players:
                        break
            
            # If no players found, save HTML for debugging
            if not players:
                self.logger.warning("⚠️ No players found with standard selectors")
                with open('debug_bayern.html', 'w', encoding='utf-8') as f:
                    f.write(soup.prettify())
                self.logger.info("Saved HTML to debug_bayern.html for inspection")
                
        except Exception as e:
            self.logger.error(f"❌ Error scraping Bayern Munich: {e}")
        
        elapsed_time = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"⏱️ Scraping completed in {elapsed_time:.2f} seconds")
        self.logger.info(f"📊 Total players found: {len(players)}")
        self.logger.info("=" * 60)
        
        return players
    
    def scrape_url(self, url: str, club_name: str = None) -> List[Dict]:
        """Generic scraper for any club URL"""
        self.logger.info("=" * 60)
        self.logger.info(f"🔍 SCRAPING: {club_name or 'Unknown Club'}")
        self.logger.info("=" * 60)
        start_time = datetime.now()
        self.logger.info(f"📡 Fetching URL: {url}")
        players = []
        
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            self.logger.info(f"✓ Response received (Status: {response.status_code}, Size: {len(response.content)} bytes)")
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Generic selectors that work for most club websites
            selectors = [
                'div[class*="player"]',
                'article[class*="player"]',
                'a[href*="player"]',
                'div[class*="squad"]',
                'div[class*="team-member"]',
                'div.player-card',
                'div.squad-player'
            ]
            
            for selector in selectors:
                elements = soup.select(selector)
                if elements and len(elements) > 5:  # At least 5 players expected
                    self.logger.info(f"Found {len(elements)} elements with selector: {selector}")
                    
                    for elem in elements:
                        name_elem = elem.find(['h1', 'h2', 'h3', 'h4', 'span', 'p'])
                        if name_elem:
                            player_name = name_elem.get_text(strip=True)
                            
                            if player_name and 3 <= len(player_name) <= 50:
                                player = {
                                    'club': club_name or 'Unknown',
                                    'name': player_name,
                                    'source': url,
                                    'scraped_at': datetime.now().isoformat()
                                }
                                players.append(player)
                                self.logger.info(f"✅ {player_name}")
                    
                    if players:
                        break
            
            # Save HTML for debugging if no players found
            if not players:
                self.logger.warning("⚠️ No players found with standard selectors")
                filename = f"debug_{club_name.replace(' ', '_').lower() if club_name else 'unknown'}.html"
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(soup.prettify())
                self.logger.info(f"Saved HTML to {filename} for inspection")
                
        except Exception as e:
            self.logger.error(f"❌ Error scraping {url}: {e}")
        
        elapsed_time = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"⏱️ Scraping completed in {elapsed_time:.2f} seconds")
        self.logger.info(f"📊 Total players found: {len(players)}")
        self.logger.info("=" * 60)
        
        return players
    
    def scrape_all_clubs(self) -> Dict:
        """Scrape players from all configured clubs"""
        self.logger.info("=" * 60)
        self.logger.info("🚀 STARTING SCRAPING SESSION")
        self.logger.info("=" * 60)
        overall_start_time = datetime.now()
        
        all_players = {
            'clubs': [],
            'summary': {},
            'execution_info': {
                'started_at': overall_start_time.isoformat(),
                'total_clubs': 0,
                'total_players': 0,
                'successful_scrapes': 0,
                'failed_scrapes': 0
            }
        }
        
        # Scrape Manchester City