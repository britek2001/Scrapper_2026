# scraper.py - SIMPLE VERSION (no newspaper3k)
import requests
import time
import json
import os
import logging
from datetime import datetime
from typing import Dict, List
from bs4 import BeautifulSoup

class FootballScraper:
    """Simple football news scraper"""
    
    def __init__(self):
        self.setup_logging()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
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
    
    def _is_valid_football_article(self, title: str, url: str) -> bool:
        """Check if article is a valid football news article"""
        # Filter out navigation links, ads, etc.
        invalid_keywords = [
            'sign up', 'subscribe', 'watch on', 'quick links',
            'follow', 'trending now', 'legends', 'enhanced app',
            'privacy', 'terms', 'cookies', 'login', '📍', '📊',
            'uefa', 'fifa', 'app', 'newsletter', 'podcast',
            'killing', 'iran', 'trump', 'white house', 'visa'  # Filter out non-football news
        ]
        
        title_lower = title.lower()
        
        # Check for invalid keywords
        if any(keyword in title_lower for keyword in invalid_keywords):
            return False
        
        # Check URL validity
        if not url or 'javascript:' in url or '#' == url or 'plus.espn.com' in url or 'offers' in url:
            return False
        
        # Filter out non-football URLs
        non_football_patterns = ['/music/', '/politics/', '/world/', '/society/', '/us-news/']
        if any(pattern in url for pattern in non_football_patterns):
            return False
        
        # Must have reasonable title length
        if len(title) < 20 or len(title) > 200:
            return False
        
        # Must contain football-related keywords
        football_keywords = [
            'football', 'soccer', 'player', 'team', 'match', 'goal', 'league',
            'premier', 'champions', 'transfer', 'manager', 'coach', 'arsenal',
            'liverpool', 'chelsea', 'city', 'united', 'barcelona', 'madrid',
            'milan', 'juventus', 'bayern', 'psg', 'injury', 'win', 'defeat'
        ]
        
        # Check if title OR url contains football keywords
        if not any(keyword in title_lower or keyword in url.lower() for keyword in football_keywords):
            return False
        
        return True
    
    def scrape_bbc_football(self) -> List[Dict]:
        """Scrape BBC Sport Football page"""
        self.logger.info("Scraping BBC Sport Football...")
        articles = []
        
        try:
            response = self.session.get(
                "https://www.bbc.com/sport/football",
                timeout=10
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Try multiple selectors (BBC changes their structure frequently)
            selectors = [
                'a.ssrcss-1mrs5ns-PromoLink',
                'a[data-testid="internal-link"]',
                'article a',
                'h3 a',
                'div[data-testid="card-text-wrapper"] a',
                'a.gs-c-promo-heading'
            ]
            
            article_links = []
            for selector in selectors:
                article_links = soup.select(selector)
                self.logger.info(f"Selector '{selector}' found {len(article_links)} links")
                if article_links:
                    break
            
            if not article_links:
                self.logger.warning("No article links found with any selector. Saving HTML for debugging...")
                with open('debug_bbc.html', 'w', encoding='utf-8') as f:
                    f.write(str(soup.prettify()))
                self.logger.info("HTML saved to debug_bbc.html for inspection")
            
            for link in article_links[:15]:  # Get top 15 articles
                try:
                    title = link.get_text(strip=True)
                    href = link.get('href', '')
                    
                    if href and not href.startswith('http'):
                        href = 'https://www.bbc.com' + href
                    
                    # Filter for relevant football articles
                    if title and href and len(title) > 10 and '/sport/' in href and self._is_valid_football_article(title, href):
                        # Get summary if available
                        parent = link.find_parent('article') or link.find_parent('div', class_=True)
                        summary = ""
                        if parent:
                            # Try multiple summary selectors
                            summary_selectors = ['p', 'div[data-testid="card-description"]', '.gs-c-promo-summary']
                            for sum_sel in summary_selectors:
                                summary_elem = parent.select_one(sum_sel)
                                if summary_elem:
                                    summary = summary_elem.get_text(strip=True)
                                    break
                        
                        article_data = {
                            'source': 'BBC Sport',
                            'title': title,
                            'summary': summary,
                            'url': href,
                            'scraped_at': datetime.now().isoformat()
                        }
                        articles.append(article_data)
                        self.logger.info(f"Found article: {title[:50]}...")
                    
                except Exception as e:
                    self.logger.warning(f"Error processing article: {e}")
                    continue
            
        except Exception as e:
            self.logger.error(f"Error scraping BBC: {e}")
            
        return articles
    
    def scrape_espn_football(self) -> List[Dict]:
        """Scrape ESPN Football page"""
        self.logger.info("Scraping ESPN Football...")
        articles = []
        
        try:
            response = self.session.get(
                "https://www.espn.com/soccer/",
                timeout=10
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'lxml')
            
            # ESPN selectors
            article_containers = soup.select('article, div.contentItem')
            self.logger.info(f"Found {len(article_containers)} potential articles on ESPN")
            
            for container in article_containers[:15]:
                try:
                    link = container.select_one('a')
                    if not link:
                        continue
                    
                    title_elem = container.select_one('h1, h2, h3, h4')
                    if not title_elem:
                        title = link.get_text(strip=True)
                    else:
                        title = title_elem.get_text(strip=True)
                    
                    href = link.get('href', '')
                    if href and not href.startswith('http'):
                        href = 'https://www.espn.com' + href
                    
                    # Filter out non-articles
                    if self._is_valid_football_article(title, href):
                        summary_elem = container.select_one('p, div.contentItem__subhead')
                        summary = summary_elem.get_text(strip=True) if summary_elem else ""
                        
                        article_data = {
                            'source': 'ESPN',
                            'title': title,
                            'summary': summary,
                            'url': href,
                            'scraped_at': datetime.now().isoformat()
                        }
                        articles.append(article_data)
                        self.logger.info(f"Found article: {title[:50]}...")
                
                except Exception as e:
                    self.logger.warning(f"Error processing ESPN article: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error scraping ESPN: {e}")
        
        return articles
    
    def scrape_sky_sports(self) -> List[Dict]:
        """Scrape Sky Sports Football page"""
        self.logger.info("Scraping Sky Sports Football...")
        articles = []
        
        try:
            response = self.session.get(
                "https://www.skysports.com/football",
                timeout=10
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Sky Sports selectors
            article_links = soup.select('a.news-list__headline-link, h3.news-list__headline a, h4 a')
            self.logger.info(f"Found {len(article_links)} potential articles on Sky Sports")
            
            for link in article_links[:15]:
                try:
                    title = link.get_text(strip=True)
                    href = link.get('href', '')
                    
                    if href and not href.startswith('http'):
                        href = 'https://www.skysports.com' + href
                    
                    if title and href and len(title) > 10 and self._is_valid_football_article(title, href):
                        parent = link.find_parent('div', class_='news-list__item')
                        summary = ""
                        if parent:
                            summary_elem = parent.select_one('p')
                            if summary_elem:
                                summary = summary_elem.get_text(strip=True)
                        
                        article_data = {
                            'source': 'Sky Sports',
                            'title': title,
                            'summary': summary,
                            'url': href,
                            'scraped_at': datetime.now().isoformat()
                        }
                        articles.append(article_data)
                        self.logger.info(f"Found article: {title[:50]}...")
                
                except Exception as e:
                    self.logger.warning(f"Error processing Sky Sports article: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error scraping Sky Sports: {e}")
        
        return articles
    
    def scrape_the_guardian_football(self) -> List[Dict]:
        """Scrape The Guardian Football page"""
        self.logger.info("Scraping The Guardian Football...")
        articles = []
        
        try:
            response = self.session.get(
                "https://www.theguardian.com/football",
                timeout=10
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Guardian selectors
            article_links = soup.select('a[data-link-name="article"]')
            self.logger.info(f"Found {len(article_links)} potential articles on The Guardian")
            
            for link in article_links[:15]:
                try:
                    title_elem = link.select_one('h3, span.js-headline-text')
                    title = title_elem.get_text(strip=True) if title_elem else link.get_text(strip=True)
                    href = link.get('href', '')
                    
                    # Only football articles
                    if '/football/' in href and self._is_valid_football_article(title, href):
                        parent = link.find_parent('div')
                        summary = ""
                        if parent:
                            summary_elem = parent.select_one('p')
                            if summary_elem:
                                summary = summary_elem.get_text(strip=True)
                        
                        article_data = {
                            'source': 'The Guardian',
                            'title': title,
                            'summary': summary,
                            'url': href,
                            'scraped_at': datetime.now().isoformat()
                        }
                        articles.append(article_data)
                        self.logger.info(f"Found article: {title[:50]}...")
                
                except Exception as e:
                    self.logger.warning(f"Error processing Guardian article: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error scraping The Guardian: {e}")
        
        return articles
    
    def scrape_multiple_sources(self) -> List[Dict]:
        """Scrape from multiple football news sources"""
        all_articles = []
        
        # Try multiple sources
        sources = [
            ('ESPN', self.scrape_espn_football),
            ('Sky Sports', self.scrape_sky_sports),
            ('The Guardian', self.scrape_the_guardian_football),
            ('BBC Sport', self.scrape_bbc_football),
        ]
        
        for source_name, scrape_func in sources:
            try:
                self.logger.info(f"\n{'='*50}")
                articles = scrape_func()
                all_articles.extend(articles)
                self.logger.info(f"✅ {source_name}: {len(articles)} articles")
                time.sleep(2)  # Be polite
            except Exception as e:
                self.logger.error(f"❌ {source_name} failed: {e}")
                continue
        
        return all_articles
    
    def save_results(self, data: List[Dict], filename: str = None):
        """Save scraped data to file"""
        if not filename:
            filename = f"football_news_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Saved {len(data)} articles to {filename}")
        return filename
    
    def run(self):
        """Run the scraper"""
        self.logger.info("Starting football scraper...")
        
        # Scrape from multiple sources
        articles = self.scrape_multiple_sources()
        
        # Save results
        filename = self.save_results(articles)
        
        self.logger.info(f"Scraping complete. Found {len(articles)} articles.")
        
        return {
            'articles': articles,
            'total': len(articles),
            'filename': filename
        }