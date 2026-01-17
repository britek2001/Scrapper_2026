import requests
import time
import json
import logging
import re
from datetime import datetime
from typing import Dict, List
from bs4 import BeautifulSoup

class InjuryScraper:
    """Scraper for football player injuries and fitness news"""
    
    def __init__(self):
        self.setup_logging()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        })
        
        self.premier_league_clubs = [
            'Arsenal', 'Aston Villa', 'Bournemouth', 'Brentford', 'Brighton',
            'Chelsea', 'Crystal Palace', 'Everton', 'Fulham', 'Ipswich',
            'Leicester', 'Liverpool', 'Manchester City', 'Manchester United',
            'Newcastle', 'Nottingham Forest', 'Southampton', 'Tottenham',
            'West Ham', 'Wolverhampton', 'Wolves', 'Burnley', 'AFC Bournemouth',
            'Brighton & Hove Albion', 'Leeds United', 'Tottenham Hotspur',
            'Wolverhampton Wanderers', 'West Ham United', 'Sunderland'
        ]
    
    def setup_logging(self):
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def _is_valid_player_name(self, name: str) -> bool:
        """Check if string is a valid player name"""
        if not name or len(name) < 3:
            return False
        
        # Filter out club names (CRITICAL!)
        for club in self.premier_league_clubs:
            if club.lower() == name.lower() or club.lower() in name.lower():
                return False
        
        # Filter out numbers only
        if name.replace(' ', '').replace('×', '').isdigit():
            return False
        
        # Filter out common non-player strings
        invalid_patterns = ['Total', 'Injured', 'Summary', 'Table', 'Position', 'Unknown', 
                           'Premier League', 'Championship']
        if any(pattern.lower() in name.lower() for pattern in invalid_patterns):
            return False
        
        # Filter out lists (contains commas)
        if ',' in name:
            return False
        
        return True
    
    def scrape_physioroom(self) -> List[Dict]:
        """Scrape PhysioRoom - FIXED VERSION"""
        self.logger.info("Scraping PhysioRoom...")
        injuries = []
        
        try:
            # Try the main EPL injury page
            url = "https://www.physioroom.com/news/english_premier_league/epl_injury_table.php"
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Save HTML for inspection
            with open('debug_physioroom.html', 'w', encoding='utf-8') as f:
                f.write(str(soup.prettify()))
            self.logger.info("Saved HTML to debug_physioroom.html")
            
            # Strategy: Look for injury data in divs or tables
            # Find all links to individual club injury pages
            club_links = soup.select('a[href*="injury"]')
            
            self.logger.info(f"Found {len(club_links)} potential club injury links")
            
            # Alternative: Parse from visible content
            # Look for player names followed by injury info
            content_divs = soup.find_all(['div', 'p', 'span'], text=re.compile(r'[A-Z][a-z]+\s+[A-Z][a-z]+'))
            
            for div in content_divs[:50]:
                text = div.get_text(strip=True)
                
                # Look for pattern: Player Name - Injury Type
                if '-' in text or ',' in text:
                    parts = re.split(r'[-,]', text)
                    if len(parts) >= 2:
                        player_name = parts[0].strip()
                        injury_type = parts[1].strip()
                        
                        if self._is_valid_player_name(player_name):
                            injury = {
                                'source': 'PhysioRoom',
                                'club': 'Unknown',
                                'player': player_name,
                                'injury_type': injury_type,
                                'expected_return': 'Unknown',
                                'status': 'Active',
                                'scraped_at': datetime.now().isoformat()
                            }
                            injuries.append(injury)
                            self.logger.info(f"✅ {player_name} - {injury_type}")
            
        except Exception as e:
            self.logger.error(f"Error scraping PhysioRoom: {e}")
        
        # If PhysioRoom fails, use Premier League official as fallback
        if not injuries:
            self.logger.info("PhysioRoom returned no data, trying Premier League API...")
            return self.scrape_premier_league_api()
        
        return injuries
    
    def scrape_premier_league_api(self) -> List[Dict]:
        """Scrape Premier League official injuries"""
        self.logger.info("Scraping Premier League API...")
        injuries = []
        
        try:
            # Try API first
            api_url = "https://footballapi.pulselive.com/football/stats/injuries"
            headers = {
                'Origin': 'https://www.premierleague.com',
                'Referer': 'https://www.premierleague.com/'
            }
            
            params = {'comps': '1', 'compSeasons': '578'}
            
            response = self.session.get(api_url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                for item in data.get('content', []):
                    player_name = item.get('name', {}).get('display', '')
                    
                    if self._is_valid_player_name(player_name):
                        injury = {
                            'source': 'Premier League Official',
                            'player': player_name,
                            'club': item.get('team', {}).get('name', 'Unknown'),
                            'injury_type': item.get('reason', 'Unknown'),
                            'expected_return': item.get('expectedReturn', 'Unknown'),
                            'status': 'Active',
                            'scraped_at': datetime.now().isoformat()
                        }
                        injuries.append(injury)
                        self.logger.info(f"✅ {player_name} - {injury['injury_type']}")
                        
        except Exception as e:
            self.logger.error(f"Error scraping Premier League: {e}")
        
        return injuries
    
    def scrape_espn_injuries(self) -> List[Dict]:
        """Scrape ESPN injury report"""
        self.logger.info("Scraping ESPN injuries...")
        injuries = []
        
        try:
            response = self.session.get("https://www.espn.com/soccer/injuries/_/league/eng.1", timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'lxml')
            
            # ESPN injury table structure
            tables = soup.select('div.ResponsiveTable, table.Table')
            
            for table in tables:
                rows = table.select('tr.Table__TR')
                
                for row in rows:
                    try:
                        cells = row.find_all('td')
                        
                        if len(cells) >= 2:
                            # First cell usually has player info
                            player_cell = cells[0]
                            player_name = player_cell.get_text(strip=True)
                            
                            # Extract actual player name (remove position, team, etc)
                            player_name = re.sub(r'[A-Z]{2,3}$', '', player_name).strip()
                            player_name = re.sub(r'\d+', '', player_name).strip()
                            
                            if self._is_valid_player_name(player_name):
                                injury_type = cells[1].get_text(strip=True) if len(cells) > 1 else 'Unknown'
                                status = cells[2].get_text(strip=True) if len(cells) > 2 else 'Unknown'
                                
                                injury = {
                                    'source': 'ESPN',
                                    'club': 'Premier League',
                                    'player': player_name,
                                    'injury_type': injury_type,
                                    'status': status,
                                    'expected_return': status,
                                    'scraped_at': datetime.now().isoformat()
                                }
                                injuries.append(injury)
                                self.logger.info(f"✅ {player_name} - {injury_type}")
                    except Exception as e:
                        continue
            
        except Exception as e:
            self.logger.error(f"Error scraping ESPN: {e}")
        
        return injuries
    
    def scrape_sky_sports_injuries(self) -> List[Dict]:
        """Scrape Sky Sports injury news"""
        self.logger.info("Scraping Sky Sports injury news...")
        injuries = []
        
        try:
            response = self.session.get("https://www.skysports.com/football/news/11095", timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'lxml')
            articles = soup.select('div.news-list__item')
            injury_keywords = ['injury', 'fitness', 'injured', 'sidelined', 'ruled out', 'doubt', 'return']
            
            for article in articles[:20]:
                try:
                    headline = article.select_one('h3, h4')
                    if not headline:
                        continue
                    
                    title = headline.get_text(strip=True)
                    title_lower = title.lower()
                    
                    if any(keyword in title_lower for keyword in injury_keywords):
                        link_elem = article.select_one('a')
                        url = link_elem.get('href', '') if link_elem else ''
                        if url and not url.startswith('http'):
                            url = 'https://www.skysports.com' + url
                        
                        summary_elem = article.select_one('p')
                        summary = summary_elem.get_text(strip=True) if summary_elem else ''
                        
                        injury_news = {
                            'source': 'Sky Sports',
                            'title': title,
                            'summary': summary,
                            'url': url,
                            'type': 'news_article',
                            'scraped_at': datetime.now().isoformat()
                        }
                        injuries.append(injury_news)
                        self.logger.info(f"✅ News: {title[:60]}...")
                        
                except Exception as e:
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error scraping Sky Sports: {e}")
        
        return injuries
    
    def scrape_transfermarkt_injuries(self) -> List[Dict]:
        """Scrape Transfermarkt injury page"""
        self.logger.info("Scraping Transfermarkt injuries...")
        injuries = []
        
        try:
            # Direct link to Premier League injuries
            url = "https://www.transfermarkt.com/premier-league/verletzungen/wettbewerb/GB1"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'lxml')
            
            tables = soup.select('table.items')
            
            for table in tables:
                rows = table.select('tbody tr')
                
                for row in rows:
                    try:
                        cells = row.find_all('td')
                        if len(cells) >= 3:
                            # Player name is usually in the first or second cell
                            player_cell = cells[0] if len(cells[0].get_text(strip=True)) > 5 else cells[1]
                            player_name = player_cell.get_text(strip=True)
                            
                            # Clean player name
                            player_name = re.sub(r'\d+', '', player_name).strip()
                            player_name = re.sub(r'#\d+', '', player_name).strip()
                            
                            if self._is_valid_player_name(player_name):
                                club = cells[1].get_text(strip=True) if len(cells) > 1 else 'Unknown'
                                injury_type = cells[2].get_text(strip=True) if len(cells) > 2 else 'Unknown'
                                expected_return = cells[3].get_text(strip=True) if len(cells) > 3 else 'Unknown'
                                
                                injury = {
                                    'source': 'Transfermarkt',
                                    'player': player_name,
                                    'club': club,
                                    'injury_type': injury_type,
                                    'expected_return': expected_return,
                                    'status': 'Injured',
                                    'scraped_at': datetime.now().isoformat()
                                }
                                injuries.append(injury)
                                self.logger.info(f"✅ {player_name} ({club}) - {injury_type}")
                                
                    except Exception as e:
                        continue
                        
        except Exception as e:
            self.logger.error(f"Error scraping Transfermarkt: {e}")
        
        return injuries
    
    def _parse_status(self, expected_return: str) -> str:
        """Parse injury status from expected return text"""
        text = expected_return.lower()
        
        if any(word in text for word in ['back', 'training', 'recovered']):
            return 'Recovering'
        elif any(word in text for word in ['weeks', 'months', 'long-term']):
            return 'Long-term'
        elif any(word in text for word in ['days', 'soon']):
            return 'Short-term'
        elif 'unknown' in text or 'tbc' in text:
            return 'Unknown'
        else:
            return 'Active'
    
    def scrape_all_injuries(self) -> Dict:
        """Scrape injuries from all sources"""
        all_injuries = {
            'player_injuries': [],
            'injury_news': [],
            'summary': {}
        }
        
        sources = [
            ('Premier League API', self.scrape_premier_league_api, 'player_injuries'),
            ('ESPN Injuries', self.scrape_espn_injuries, 'player_injuries'),
            ('Transfermarkt', self.scrape_transfermarkt_injuries, 'player_injuries'),
            ('PhysioRoom', self.scrape_physioroom, 'player_injuries'),
            ('Sky Sports News', self.scrape_sky_sports_injuries, 'injury_news')
        ]
        
        for source_name, scraper_func, category in sources:
            try:
                self.logger.info(f"Starting scraping from {source_name}...")
                results = scraper_func()
                
                if results:
                    all_injuries[category].extend(results)
                    self.logger.info(f"✅ {source_name}: {len(results)} items found")
                else:
                    self.logger.warning(f"⚠️ {source_name}: No data found")
                
                # Be polite - wait between requests
                time.sleep(2)
                
            except Exception as e:
                self.logger.error(f"❌ {source_name} failed: {e}")
                continue
        
        # Generate summary
        all_injuries['summary'] = {
            'total_player_injuries': len(all_injuries['player_injuries']),
            'total_news_articles': len(all_injuries['injury_news']),
            'sources_scraped': len(sources),
            'timestamp': datetime.now().isoformat()
        }
        
        self.logger.info(f"\n📊 SUMMARY:")
        self.logger.info(f"Total player injuries: {all_injuries['summary']['total_player_injuries']}")
        self.logger.info(f"Total news articles: {all_injuries['summary']['total_news_articles']}")
        
        return all_injuries
    
    def save_to_json(self, data: Dict, filename: str = 'injuries_data.json'):
        """Save scraped data to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"✅ Data saved to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving to JSON: {e}")

def main():
    """Main execution function"""
    scraper = InjuryScraper()
    
    print("=" * 60)
    print("🏥 FOOTBALL INJURY SCRAPER")
    print("=" * 60)
    
    # Scrape all sources
    results = scraper.scrape_all_injuries()
    
    # Save results
    scraper.save_to_json(results)
    
    print("\n" + "=" * 60)
    print("✅ SCRAPING COMPLETE")
    print("=" * 60)
    print(f"Total Player Injuries: {results['summary']['total_player_injuries']}")
    print(f"Total News Articles: {results['summary']['total_news_articles']}")
    print(f"Data saved to: injuries_data.json")
    print("=" * 60)

if __name__ == "__main__":
    main()
