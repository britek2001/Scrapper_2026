"""
Complete functional replacement for interation_scraper.py
This provides a working implementation with all necessary classes and methods.
"""

import json
import os
import re
import time
import requests
import logging
from urllib.parse import urlparse, urljoin
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from bs4 import BeautifulSoup
# optional external parser for Transfermarkt pages
try:
    from transfermarkt_parser import (
        parse_profile as tm_parse_profile,
        parse_transfers as tm_parse_transfers,
        parse_injuries as tm_parse_injuries,
        parse_performance as tm_parse_performance,
        parse_achievements as tm_parse_achievements,
        parse_kit_numbers as tm_parse_kit_numbers,
        parse_market_value as tm_parse_market_value,
        parse_news as tm_parse_news,
        parse_debuts as tm_parse_debuts,
        parse_goal_involvements as tm_parse_goal_involvements,
        parse_table as tm_parse_table,
        extract_links as tm_extract_links,
    )
    EXTERNAL_PARSER_AVAILABLE = True
except Exception:
    EXTERNAL_PARSER_AVAILABLE = False
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException
import html2text
from newspaper import Article

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Check for optional dependencies
try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logger.warning("Playwright not available, some features disabled")

try:
    from gnews import GNews
    GNEWS_AVAILABLE = True
except ImportError:
    GNEWS_AVAILABLE = False
    logger.warning("GNews not available, news scraping disabled")


class Config:
    """Configuration class for all scrapers"""
    
    def __init__(self):
        self.output_directory = 'content_output'
        self.batch_size = 100
        self.delay_between_requests = 1.0
        self.max_content_length = 10000
        self.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        self.request_timeout = 30
        
    def load_config(self, path: Optional[str] = None) -> List[Any]:
        """Load configuration from JSON file"""
        if not path:
            path = os.path.join(os.path.dirname(__file__), 'config', 'urls.json')
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {path}")
            return []
        except Exception as e:
            logger.error(f"Error loading config from {path}: {e}")
            return []
    
    @classmethod
    def load_tasks(cls, path: Optional[str] = None) -> List[Any]:
        """Load tasks from JSON file"""
        if not path:
            path = os.path.join(os.path.dirname(__file__), 'config', 'tasks.json')
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []
    
    @classmethod
    def load_player_urls(cls, path: Optional[str] = None) -> List[Any]:
        """Load player URLs from JSON file"""
        if not path:
            path = os.path.join(os.path.dirname(__file__), 'config', 'players.json')
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []
    
    def save_batch(self, batch: List[Dict], output_dir: str, start_idx: int, end_idx: int, timestamp: int) -> None:
        """Save a batch of results to file"""
        os.makedirs(output_dir, exist_ok=True)
        filename = f"batch_{timestamp}_{start_idx:05d}-{end_idx:05d}.json"
        filepath = os.path.join(output_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(batch, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved batch {start_idx}-{end_idx} to {filepath}")
        except Exception as e:
            logger.error(f"Error saving batch: {e}")
    
    def generate_report(self, result: Dict, stats: Dict) -> None:
        """Update statistics based on result"""
        if not isinstance(result, dict):
            return
        if result.get('success'):
            stats['success'] = stats.get('success', 0) + 1
            stats['total_words'] = stats.get('total_words', 0) + result.get('word_count', 0)
            stats['total_lines'] = stats.get('total_lines', 0) + result.get('line_count', 0)
        else:
            stats['failed'] = stats.get('failed', 0) + 1


class BatchProcessor:
    """Handles batch processing and progress tracking"""
    
    def __init__(self, batch_size: int = 100, output_dir: str = "output"):
        self.batch_size = batch_size
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.progress_file = os.path.join(output_dir, "progress.json")
        
    def load_progress(self) -> Dict:
        """Load progress from file"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {
            "last_after": None,
            "current_batch": 1,
            "completed_batches": [],
            "pages_fetched": 0,
            "posts_collected": 0,
            "comments_collected": 0
        }
    
    def save_progress(self, progress: Dict) -> None:
        """Save progress to file"""
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(progress, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving progress: {e}")
    
    def process_batch(self, items: List, batch_num: int, fetch_comments: bool = True) -> Dict[str, int]:
        """Process a batch of items"""
        if not items:
            return {"posts": 0, "comments": 0}
        
        # Save batch to file
        timestamp = int(time.time())
        filename = f"batch_{batch_num:04d}_{timestamp}.json"
        filepath = os.path.join(self.output_dir, filename)
        
        # Convert items to dict if they have to_dict method
        batch_data = []
        for item in items:
            if hasattr(item, 'to_dict'):
                batch_data.append(item.to_dict())
            else:
                batch_data.append(item)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(batch_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved batch {batch_num} with {len(items)} items to {filepath}")
        
        return {
            "posts": len(items),
            "comments": sum(getattr(item, 'num_comments', 0) for item in items if hasattr(item, 'num_comments'))
        }
    
    def save_stats(self, stats: Dict) -> None:
        """Save statistics to file"""
        stats_file = os.path.join(self.output_dir, "stats.json")
        try:
            with open(stats_file, 'w') as f:
                json.dump(stats, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving stats: {e}")


def setup_driver(headless: bool = True) -> Any:
    """Setup Selenium WebDriver"""
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument(f"user-agent={Config().browser_user_agent}")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(30)
        return driver
    except Exception as e:
        logger.error(f"Error setting up driver: {e}")
        return None


class Urls_Extraction:
    """Extracts content from URLs"""
    
    def __init__(self):
        self.conf = Config()
        
    def ensure_driver_alive(self, driver: Any) -> Any:
        """Ensure driver is still alive, restart if needed"""
        try:
            driver.current_url
            return driver
        except:
            logger.warning("Driver died, restarting...")
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            return setup_driver()
    
    def extract_domain(self, driver: Any, url: str) -> Dict[str, Any]:
        """Extract content from a single URL"""
        try:
            driver.get(url)
            time.sleep(2)
            
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Extract title
            title = soup.title.string if soup.title else ""
            
            # Extract main content
            for element in soup.find_all(['script', 'style', 'nav', 'footer', 'header']):
                element.decompose()
            
            content_text = soup.get_text(separator=' ', strip=True)
            
            # Extract found URLs
            found_urls = set()
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('http'):
                    found_urls.add(href)
            
            # Convert to markdown if content is too short
            if len(content_text) < 200:
                h = html2text.HTML2Text()
                h.ignore_links = True
                h.ignore_images = True
                content_text = h.handle(str(soup))
            
            content_text = re.sub(r'\s+', ' ', content_text).strip()
            sentences = [s.strip() for s in content_text.split('. ') if s.strip()]
            line_count = len(sentences)
            words = content_text.split()
            word_count = len(words)
            
            content_text = content_text[:self.conf.max_content_length]
            
            return {
                'url': url,
                'title': title[:200],
                'content': content_text,
                'word_count': word_count,
                'line_count': line_count,
                'domain': urlparse(url).netloc,
                'found_urls': list(found_urls)[:100],  # Limit to first 100
                'success': True
            }
            
        except WebDriverException as e:
            logger.error(f"Selenium error for {url}: {e}")
            return {'url': url, 'success': False, 'error': str(e)}
        except Exception as e:
            logger.error(f"Extract error for {url}: {e}")
            return {'url': url, 'success': False, 'error': str(e)}
    
    def stats_dictonary_category(self, urls_data: List[Dict]) -> Dict[str, List]:
        """Group URLs by category"""
        dictonary_category = {}
        for entry in urls_data:
            if isinstance(entry, dict):
                category = entry.get('category', 'uncategorized')
                if category not in dictonary_category:
                    dictonary_category[category] = []
                dictonary_category[category].append(entry)
        return dictonary_category
    
    def process_url(self, driver: Any, url_dict: List[Dict], all_results: List, 
                    stats: Dict, output_dir: str, timestamp: int) -> None:
        """Process a list of URLs"""
        try:
            start_index = len(all_results)
            
            for i, url_info in enumerate(url_dict, 1):
                driver = self.ensure_driver_alive(driver)
                
                if isinstance(url_info, dict):
                    url_to_fetch = url_info.get('url')
                    category = url_info.get('category', '')
                else:
                    url_to_fetch = url_info
                    category = ''
                
                # Extract content with retry
                try:
                    result = self.extract_domain(driver, url_to_fetch)
                except WebDriverException as e:
                    logger.error(f"Selenium error: {str(e)[:80]}")
                    driver = self.ensure_driver_alive(driver)
                    try:
                        result = self.extract_domain(driver, url_to_fetch)
                    except Exception as e2:
                        logger.error(f"Second attempt failed: {str(e2)[:80]}")
                        result = {'url': url_to_fetch, 'success': False, 'error': str(e2)}
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    logger.error(f"Error processing URL: {str(e)[:80]}")
                    result = {'url': url_to_fetch, 'success': False, 'error': str(e)}
                
                if category:
                    result['category'] = category
                
                all_results.append(result)
                self.conf.generate_report(result, stats)
                
                # Save batch if needed
                if self.conf.batch_size > 0 and i % self.conf.batch_size == 0:
                    start_idx = i - self.conf.batch_size + 1
                    end_idx = i
                    batch = all_results[start_idx-1:end_idx]
                    self.conf.save_batch(batch, output_dir, start_idx, end_idx, timestamp)
                
                if i < len(url_dict):
                    time.sleep(self.conf.delay_between_requests)
            
            # Save remaining results
            new_count = len(all_results) - start_index
            if new_count > 0 and new_count < self.conf.batch_size:
                self.conf.save_batch(all_results[start_index:], output_dir, 
                                     start_index + 1, len(all_results), timestamp)
                
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, stopping early...")
            raise
    
    def execution_url_agentent(self, driver: Any, urls_data: List) -> List[Dict]:
        """Main execution method for URL extraction"""
        logger.info("URL AGENT STARTING URL SCRAPING...")
        
        all_results = []
        stats = {'success': 0, 'failed': 0, 'total_words': 0, 'total_lines': 0}
        
        # Setup driver if not provided
        if not driver:
            driver = setup_driver()
            if not driver:
                logger.error("Failed to setup driver")
                return []
        
        try:
            dictonary_category = self.stats_dictonary_category(urls_data)
            logger.info(f"Found categories: {list(dictonary_category.keys())}")
            
            output_dir = self.conf.output_directory
            timestamp = int(time.time())
            
            for category, urls in dictonary_category.items():
                logger.info(f"\nProcessing category: {category} ({len(urls)} URLs)")
                self.process_url(driver, urls, all_results, stats, output_dir, timestamp)
            
            logger.info(f"\nScraping complete: {stats['success']} successful, {stats['failed']} failed")
            logger.info(f"Total words: {stats['total_words']}, Total lines: {stats['total_lines']}")
            
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            try:
                driver.quit()
            except:
                pass
        
        return all_results


class News_Scraper:
    """Scrapes news articles using GNews"""
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    def __init__(self, use_playwright: bool = True, use_selenium: bool = True, delay: float = 1.0,
                 store_html: bool = True, use_external_parser: bool = False):
        self.conf = Config()
        self.use_playwright = use_playwright and PLAYWRIGHT_AVAILABLE
        self.use_selenium = use_selenium
        self.delay = delay
        self.store_html = store_html
        self.use_external_parser = use_external_parser and EXTERNAL_PARSER_AVAILABLE
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        # Additional defaults
        self.timeout = 30
        self.max_retries = 3
        # add referer to help avoid simple blocking
        try:
            self.session.headers.update({'Referer': 'https://www.transfermarkt.com/'})
        except Exception:
            pass
        # Use datetime objects for GNews compatibility
        self.start_date = datetime(2023, 1, 1)
        self.end_date = datetime(2024, 12, 31)
        self.max_results_per_keyword = 20
        self.languages = ['fr']
        self.countries = ['US', 'FR', 'DE', 'ES', 'IT']
        self.labels = ['france', 'usa', 'germany', 'spain', 'italy']
    
    def resolve_real_url(self, driver: Any, google_url: str) -> str:
        """Resolve Google redirect URL to actual URL"""
        try:
            driver.get(google_url)
            for _ in range(10):
                current_url = driver.current_url
                if "google.com" not in current_url and "consent.google.com" not in current_url:
                    return current_url
                time.sleep(0.5)
            return driver.current_url
        except:
            return google_url
    
    def get_content_from_url(self, url: str) -> Optional[str]:
        """Extract article content using newspaper3k"""
        try:
            config = Config()
            article = Article(url, config=config)
            article.download()
            article.parse()
            return article.text
        except Exception as e:
            logger.error(f"Error extracting content from {url}: {e}")
            return None
    
    def execution_url_agentent(self, driver: Any, tasks: List[Dict]) -> List[Dict]:
        """Execute news scraping based on tasks"""
        if not GNEWS_AVAILABLE:
            logger.error("GNews not available, cannot scrape news")
            return []
        
        stats = {'success': 0, 'failed': 0, 'total_words': 0, 'total_lines': 0}
        output_dir = self.conf.output_directory
        timestamp = int(time.time())
        all_data = []
        total_count = 0
        seen_titles = set()
        
        if not driver:
            driver = setup_driver()
        
        try:
            for task in tasks:
                keywords = task.get('keywords', [])
                label = task.get('label', 'unknown')
                
                news_api = GNews(
                    language=self.languages[0],
                    country=self.countries[0],
                    max_results=self.max_results_per_keyword,
                    start_date=self.start_date,
                    end_date=self.end_date
                )
                
                for keyword in keywords:
                    try:
                        logger.info(f"Searching for keyword: {keyword}")
                        results = news_api.get_news(keyword)
                        
                        for item in results:
                            title = item.get('title')
                            if title in seen_titles:
                                continue
                            
                            google_url = item.get('url')
                            if not google_url:
                                continue
                            
                            real_url = self.resolve_real_url(driver, google_url)
                            if "google.com" in real_url:
                                continue
                            
                            seen_titles.add(title)
                            full_text = self.get_content_from_url(real_url)
                            
                            if full_text and len(full_text) > 200:
                                all_data.append({
                                    'url': real_url,
                                    'title': title[:200],
                                    'domain': item.get('publisher', {}).get('title', ''),
                                    'date': item.get('published date', ''),
                                    'source_label': label,
                                    'publisher': item.get('publisher', {}).get('title', ''),
                                    'content': full_text,
                                    'word_count': len(full_text.split()),
                                    'line_count': len(full_text.split('. ')),
                                    'success': True
                                })
                                
                                total_count += 1
                                logger.info(f"[{total_count}] {label}: {title[:50]}...")
                                
                                self.conf.generate_report({'success': True, 'word_count': len(full_text.split()),
                                                          'line_count': len(full_text.split('. '))}, stats)
                                
                                # Save batch if needed
                                if self.conf.batch_size > 0 and total_count % self.conf.batch_size == 0:
                                    start_idx = total_count - self.conf.batch_size + 1
                                    batch = all_data[-self.conf.batch_size:]
                                    self.conf.save_batch(batch, output_dir, start_idx, total_count, timestamp)
                    
                    except Exception as e:
                        logger.error(f"Error processing keyword '{keyword}': {str(e)[:80]}")
            
            # Save remaining data
            if all_data and len(all_data) % self.conf.batch_size != 0:
                remaining = all_data[-(len(all_data) % self.conf.batch_size):]
                if remaining:
                    start_idx = total_count - len(remaining) + 1
                    self.conf.save_batch(remaining, output_dir, start_idx, total_count, timestamp)
            
            logger.info(f"News scraping complete: {len(all_data)} articles collected")
            
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
        
        return all_data


class TransderMarkt_Scraper:
    """Scrapes Transfermarkt data for players"""
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
    
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
    
    def __init__(self, use_playwright: bool = True, use_selenium: bool = True, delay: float = 1.0,
                 follow_links: bool = False, follow_patterns: Optional[List[str]] = None, max_follow_links: int = 20):
        self.conf = Config()
        self.use_playwright = use_playwright and PLAYWRIGHT_AVAILABLE
        self.use_selenium = use_selenium
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)

        # Link-following options
        self.follow_links = follow_links
        self.follow_patterns = follow_patterns or []
        self.max_follow_links = max_follow_links

        # Setup directories
        self.base_directory = os.path.join(os.getcwd(), "transfermarkt_data")
        self.data_directory = os.path.join(self.base_directory, "data")
        self.player_data_directory = os.path.join(self.data_directory, "players")
        # We no longer store HTML pages on disk; parsed data will be saved as JSON per player
        self.player_html_directory = os.path.join(self.data_directory, "html")  # kept for compatibility but unused
        self.use_external_parser = EXTERNAL_PARSER_AVAILABLE

        for folder in [self.base_directory, self.data_directory, self.player_data_directory]:
            os.makedirs(folder, exist_ok=True)

    def _normalize_and_filter_link(self, href: str, base_url: str) -> Optional[str]:
        """Normalize href to absolute URL and filter obvious noise."""
        try:
            if not href or href.strip() == '':
                return None
            href = href.strip()
            # skip javascript/mailto anchors
            if href.startswith('javascript:') or href.startswith('mailto:') or href.startswith('#'):
                return None
            # convert relative to absolute
            if href.startswith('/'):
                full = urljoin('https://www.transfermarkt.com', href)
            elif href.startswith('http'):
                full = href
            else:
                # relative without leading slash
                full = urljoin(base_url, href)

            parsed = urlparse(full)
            # only follow transfermarkt domain by default
            if 'transfermarkt.' not in parsed.netloc:
                return None

            # allow if explicit follow_patterns match
            patterns = [p.lower() for p in (getattr(self, 'follow_patterns', []) or [])]
            if patterns and any(pat in full.lower() for pat in patterns):
                return full

            # avoid large navigation sections by simple heuristics (keep statistik out of blanket exclusion)
            lower = parsed.path.lower()
            if any(prefix in lower for prefix in ['/navigation', '/detailsuche', '/aktuell', '/rumour-mill', '/betting', '/intern']):
                return None

            return full
        except Exception:
            return None

    def parse_generic_page(self, html: str, url: str) -> Dict[str, Any]:
        """Basic generic parser for followed linked pages (title, h1, paragraphs, small tables)."""
        soup = BeautifulSoup(html, 'html.parser')
        try:
            # remove scripts/styles/navigation
            for el in soup.find_all(['script', 'style', 'nav', 'footer', 'header']):
                el.decompose()

            title = soup.title.string.strip() if soup.title and soup.title.string else ''
            h1s = [h.get_text(' ', strip=True) for h in soup.find_all('h1')][:3]
            paragraphs = [p.get_text(' ', strip=True) for p in soup.find_all('p')][:3]

            tables = []
            for table in soup.find_all('table')[:3]:
                rows = []
                for tr in table.find_all('tr')[:20]:
                    cols = [td.get_text(' ', strip=True) for td in tr.find_all(['td', 'th'])]
                    if cols:
                        rows.append(cols)
                if rows:
                    tables.append(rows)

            return {
                'url': url,
                'title': title,
                'h1': h1s,
                'paragraphs': paragraphs,
                'tables': tables
            }
        except Exception as e:
            logger.debug(f"Generic parse error for {url}: {e}")
            return {'url': url, 'error': str(e)}
    
    def extract_player_id_from_url(self, url: str) -> Optional[str]:
        """Extract player ID from Transfermarkt URL"""
        match = re.search(r'/spieler/(\d+)', url)
        return match.group(1) if match else None
    
    def extract_slug_from_url(self, url: str) -> str:
        """Extract slug from URL"""
        try:
            parsed = urlparse(url)
            segments = [s for s in parsed.path.split('/') if s]
            if segments:
                return segments[0]
        except Exception:
            pass
        return "unknown"
    
    def build_all_page_urls(self, base_url: str) -> Dict[str, str]:
        """Build URLs for all page types"""
        urls = {}
        base = base_url.rstrip('/')
        
        # Add all page types
        for page_type in self.PAGE_TYPES.keys():
            if page_type == 'profil':
                urls[page_type] = base
            else:
                urls[page_type] = f"{base}/{page_type}"
        
        return urls
    
    def fetch_with_playwright(self, url: str) -> Optional[str]:
        """Fetch page using Playwright"""
        if not PLAYWRIGHT_AVAILABLE:
            return None
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url, timeout=30000)
                time.sleep(2)
                content = page.content()
                browser.close()
                return content
        except Exception as e:
            logger.error(f"Playwright error: {e}")
            return None
    
    def fetch_page(self, url: str, page_type: str, player_id: str, slug: str) -> Optional[str]:
        """Fetch page from URL or cache"""
        # Do not use on-disk caching; fetch and return HTML for immediate parsing
        time.sleep(self.delay)
        html_content = None

        # Try requests with retries
        for attempt in range(1, getattr(self, 'max_retries', 3) + 1):
            try:
                logger.info(f"Downloading {url} (attempt {attempt})")
                response = self.session.get(url, timeout=getattr(self, 'timeout', 30))
                status = getattr(response, 'status_code', None)
                if status == 200:
                    html_content = response.text
                    lower = html_content.lower()
                    invalid_indicators = [
                        'player not found',
                        'seite nicht gefunden',
                        'page not found',
                        'player nicht gefunden'
                    ]
                    if any(indicator in lower for indicator in invalid_indicators):
                        logger.warning(f"Invalid page content for {url}")
                        html_content = None
                    else:
                        break
                else:
                    logger.warning(f"Non-200 response {status} for {url}")
            except Exception as e:
                logger.debug(f"Requests fetch failed for {url} attempt {attempt}: {e}")

            # backoff before next attempt
            time.sleep(min(2 ** attempt, 10))

        # Playwright fallback (no disk writes)
        if not html_content and self.use_playwright and page_type not in ['news']:
            try:
                html_content = self.fetch_with_playwright(url)
            except Exception as e:
                logger.debug(f"Playwright fetch failed for {url}: {e}")

        return html_content
    
    def parse_profile_page(self, html: str) -> Dict[str, Any]:
        """Parse player profile page"""
        soup = BeautifulSoup(html, 'html.parser')
        data = {}
        
        try:
            # Player name
            name_elem = soup.find('h1', class_='data-header__headline-wrapper')
            if name_elem:
                raw = name_elem.get_text(" ", strip=True)
                raw = re.sub(r'^#\d+\s*', '', raw)
                data['name'] = raw.strip()
            else:
                title = soup.find('title')
                if title:
                    data['name'] = title.get_text(strip=True).split('-')[0].strip()
            
            # Current club
            current_club = soup.find('span', class_='data-header__club')
            if current_club:
                a = current_club.find('a')
                if a:
                    data['current_club'] = a.get_text(strip=True)
                else:
                    data['current_club'] = current_club.get_text(strip=True)
            
            # Shirt number
            shirt_number = soup.find('span', class_='data-header__shirt-number')
            if shirt_number:
                data['shirt_number'] = shirt_number.get_text(strip=True)
            
            # Basic info
            info = {}
            for ul in soup.find_all('ul', class_='data-header__items'):
                for li in ul.find_all('li'):
                    label = li.get_text(":", strip=True)
                    if ':' in label:
                        k, v = [part.strip() for part in label.split(':', 1)]
                        info[k] = v
            if info:
                data['basic_info'] = info
            
            # Market value
            mv = soup.find('a', class_='data-header__market-value-wrapper')
            if mv:
                data['current_market_value'] = mv.get_text(" ", strip=True)
            
            # Position/foot
            for li in soup.find_all('li', class_='data-header__label'):
                txt = li.get_text(" ", strip=True)
                if 'Position' in txt:
                    data['position'] = txt.split(':', 1)[-1].strip()
                if 'Pied' in txt or 'Foot' in txt:
                    data['preferred_foot'] = txt.split(':', 1)[-1].strip()
        
        except Exception as e:
            logger.error(f"Error parsing profile: {e}")
        
        return data
    
    def parse_injuries_page(self, html: str) -> Dict[str, Any]:
        """Parse injuries page"""
        soup = BeautifulSoup(html, 'html.parser')
        injuries = []
        totals = {}
        
        try:
            tables = soup.find_all('table', class_='items')
            
            # First table: injuries list
            if tables and len(tables) > 0:
                tbody = tables[0].find('tbody')
                if tbody:
                    for row in tbody.find_all('tr'):
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
            
            # Second table: totals
            if len(tables) > 1:
                tbody = tables[1].find('tbody')
                if tbody:
                    for row in tbody.find_all('tr'):
                        cols = row.find_all('td')
                        if len(cols) >= 4:
                            season = cols[0].get_text(strip=True)
                            totals[season] = {
                                'total_days': cols[1].get_text(strip=True),
                                'injuries_count': cols[2].get_text(strip=True),
                                'games_missed': cols[3].get_text(strip=True)
                            }
        
        except Exception as e:
            logger.error(f"Error parsing injuries: {e}")
        
        return {
            'injuries_list': injuries,
            'season_totals': totals
        }
    
    def parse_market_value_page(self, html: str) -> List[Dict[str, str]]:
        """Parse market value page"""
        soup = BeautifulSoup(html, 'html.parser')
        values = []
        
        try:
            table = soup.find('table', class_='items')
            if table and table.find('tbody'):
                for row in table.find('tbody').find_all('tr'):
                    cols = row.find_all('td')
                    if len(cols) >= 2:
                        values.append({
                            'date': cols[0].get_text(strip=True),
                            'market_value': cols[1].get_text(" ", strip=True),
                            'club': cols[2].get_text(strip=True) if len(cols) > 2 else ""
                        })
        except Exception as e:
            logger.error(f"Error parsing market values: {e}")
        
        return values
    
    def parse_transfers_page(self, html: str) -> List[Dict[str, str]]:
        """Parse transfers page"""
        soup = BeautifulSoup(html, 'html.parser')
        transfers = []
        
        try:
            table = soup.find('table', class_='items')
            if table and table.find('tbody'):
                for row in table.find('tbody').find_all('tr'):
                    cols = row.find_all('td')
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
            logger.error(f"Error parsing transfers: {e}")
        
        return transfers
    
    def parse_balance_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse balance page"""
        soup = BeautifulSoup(html, 'html.parser')
        results = []
        
        try:
            table = soup.find('table', class_='items')
            if table and table.find('tbody'):
                for row in table.find('tbody').find_all('tr'):
                    cols = row.find_all('td')
                    if not cols:
                        continue
                    
                    entry = {}
                    try:
                        if len(cols) > 1:
                            club_a = cols[1].find('a')
                            entry['club'] = club_a.get_text(strip=True) if club_a else cols[1].get_text(strip=True)
                        if len(cols) > 2:
                            entry['appearances'] = cols[2].get_text(strip=True)
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
            logger.error(f"Error parsing balance page: {e}")
        
        return results
    
    def parse_achievements_page(self, html: str) -> Dict[str, Any]:
        """Parse achievements page"""
        soup = BeautifulSoup(html, 'html.parser')
        achievements = {}
        
        try:
            boxes = soup.find_all('div', class_='box')
            for box in boxes:
                h2 = (box.find('h2', class_='content-box-headline') or 
                      box.find('div', class_='content-box-headline') or 
                      box.find('h2'))
                title = h2.get_text(strip=True) if h2 else None
                if not title:
                    th = box.find('th')
                    title = th.get_text(strip=True) if th else None
                if not title:
                    continue
                
                items = []
                tbl = box.find('table', class_='auflistung') or box.find('table')
                if tbl:
                    for tr in tbl.find_all('tr'):
                        tds = tr.find_all('td')
                        if not tds:
                            continue
                        
                        season = tds[0].get_text(strip=True) if len(tds) > 0 else ''
                        club = ''
                        club_id = None
                        
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
                        
                        if season == '' and club == '':
                            continue
                        
                        items.append({
                            'season': season,
                            'club': club,
                            'club_id': club_id
                        })
                
                achievements[title] = items
        
        except Exception as e:
            logger.error(f"Error parsing achievements: {e}")
        
        return achievements
    
    def parse_performance_page(self, html: str, page_type: str) -> Dict[str, Any]:
        """Parse performance pages"""
        soup = BeautifulSoup(html, 'html.parser')
        data = {}
        
        try:
            if page_type == 'bilanz':
                return {'balance': self.parse_balance_page(html)}
            
            tables = soup.find_all('table', class_='items')
            if not tables:
                tables = soup.find_all('table')
            
            for i, table in enumerate(tables):
                headers = []
                thead = table.find('thead')
                if thead:
                    for th in thead.find_all('th'):
                        text = th.get_text(" ", strip=True)
                        if text == '':
                            text = f"col_{len(headers)+1}"
                        headers.append(text)
                
                tbody = table.find('tbody')
                rows = []
                if tbody:
                    first_row = tbody.find('tr')
                    if first_row and not headers:
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
            logger.error(f"Error parsing {page_type}: {e}")
        
        return data
    
    def save_player_json(self, player_data: Dict, slug: str, player_id: str) -> None:
        """Save player data to JSON file"""
        filename = f"{slug}_{player_id}.json"
        filepath = os.path.join(self.player_data_directory, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(player_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Player data saved: {filepath}")
        except Exception as e:
            logger.error(f"Error saving player data: {e}")
    
    def execution_url_agentent(self, driver: Any, players: List[Dict]) -> List[Dict]:
        """Execute Transfermarkt scraping for all players"""
        all_player_data = []
        
        for player in players:
            if isinstance(player, dict):
                base_url = player.get('url')
                player_name = player.get('name', 'unknown')
            else:
                base_url = player
                player_name = 'unknown'
            
            if not base_url:
                continue
            
            logger.info(f"Processing player: {player_name}")
            
            player_id = self.extract_player_id_from_url(base_url)
            slug = self.extract_slug_from_url(base_url)
            
            if not player_id:
                logger.error(f"Invalid URL, cannot extract ID: {base_url}")
                continue
            
            urls = self.build_all_page_urls(base_url)
            
            player_data = {
                'player_id': player_id,
                'slug': slug,
                'name': player_name,
                'base_url': base_url,
                'scraped_at': datetime.utcnow().isoformat() + 'Z',
                'pages': {}
            }
            
            for page_type, url in urls.items():
                try:
                    html = self.fetch_page(url, page_type, player_id, slug)
                    if not html:
                        player_data['pages'][page_type] = {'error': 'Failed to fetch page'}
                        time.sleep(self.delay)
                        continue
                    
                    # Select parser based on page type
                    if getattr(self, 'use_external_parser', False):
                        # Use transfermarkt_parser functions when available
                        if page_type == 'profil':
                            page_data = tm_parse_profile(html)
                        elif page_type == 'verletzungen':
                            page_data = tm_parse_injuries(html)
                        elif page_type == 'marktwertverlauf':
                            page_data = tm_parse_market_value(html)
                        elif page_type == 'transfers':
                            page_data = tm_parse_transfers(html)
                        elif page_type == 'bilanz':
                            page_data = tm_parse_table(html)
                        elif page_type == 'erfolge':
                            page_data = tm_parse_achievements(html)
                        elif page_type == 'rueckennummern':
                            page_data = tm_parse_kit_numbers(html)
                        elif page_type == 'news':
                            page_data = tm_parse_news(html)
                        elif page_type in [
                            'leistungsdaten', 'leistungsdatendetails', 'detaillierteleistungsdaten',
                            'leistungsdatenverein', 'leistungsdatentrainer', 'elfmetertore',
                            'meistetore', 'meistetorbeteiligungen', 'nationalmannschaft',
                            'debuets', 'siege', 'niederlagen'
                        ]:
                            page_data = tm_parse_performance(html)
                        else:
                            page_data = tm_parse_table(html)
                    else:
                        # Use built-in parsers
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
                            page_data = {'html_saved': True, 'url': url}
                    
                    # extract links found on the page
                    try:
                        if self.use_external_parser and 'tm_extract_links' in globals():
                            links = tm_extract_links(html)
                        else:
                            soup_links = BeautifulSoup(html, 'html.parser')
                            found_urls = set()
                            for a in soup_links.find_all('a', href=True):
                                href = a['href'].strip()
                                if href.startswith('http'):
                                    found_urls.add(href)
                            links = [{'href': u} for u in list(found_urls)[:200]]
                    except Exception:
                        links = []

                    if page_data:
                        if isinstance(page_data, dict):
                            page_data['found_urls'] = links
                        else:
                            page_data = {'data': page_data, 'found_urls': links}
                    else:
                        page_data = {'error': 'No data extracted', 'found_urls': links}

                    player_data['pages'][page_type] = page_data
                    
                except Exception as e:
                    logger.error(f"Error scraping {page_type}: {e}")
                    player_data['pages'][page_type] = {'error': str(e)}
                
                time.sleep(self.delay)
            
            # Optionally follow selected links found on the player's pages and parse them
            if getattr(self, 'follow_links', False):
                try:
                    seen = set()
                    linked_results = []
                    patterns = [p.lower() for p in (self.follow_patterns or [])]

                    # collect candidate hrefs from all pages' found_urls
                    candidates = []
                    for page_obj in player_data.get('pages', {}).values():
                        if isinstance(page_obj, dict):
                            furls = page_obj.get('found_urls') or []
                        else:
                            furls = []
                        if isinstance(furls, list):
                            for fu in furls:
                                if isinstance(fu, dict):
                                    href = fu.get('href')
                                else:
                                    href = fu
                                if href:
                                    candidates.append(href)

                    for href in candidates:
                        full = self._normalize_and_filter_link(href, base_url)
                        if not full:
                            continue
                        if full in seen:
                            continue
                        # if patterns are provided, require at least one to match
                        if patterns and not any(pat in full.lower() for pat in patterns):
                            continue
                        seen.add(full)
                        logger.info(f"Following linked URL: {full}")
                        linked_html = self.fetch_page(full, 'linked', player_id, slug)
                        # fallback: try Playwright if available
                        if not linked_html and getattr(self, 'use_playwright', False):
                            try:
                                logger.info(f"Requests failed, trying Playwright for {full}")
                                linked_html = self.fetch_with_playwright(full)
                            except Exception as e:
                                logger.debug(f"Playwright fallback failed for {full}: {e}")
                        # fallback: try Selenium if available
                        if not linked_html and getattr(self, 'use_selenium', False):
                            try:
                                logger.info(f"Trying Selenium for linked page {full}")
                                drv = setup_driver(headless=True)
                                if drv:
                                    try:
                                        drv.get(full)
                                        time.sleep(2)
                                        linked_html = drv.page_source
                                    finally:
                                        try:
                                            drv.quit()
                                        except:
                                            pass
                            except Exception as e:
                                logger.debug(f"Selenium fallback failed for {full}: {e}")

                        if not linked_html:
                            logger.info(f"Could not fetch linked URL: {full}")
                            continue

                        parsed = self.parse_generic_page(linked_html, full)
                        linked_results.append(parsed)
                        if len(linked_results) >= int(getattr(self, 'max_follow_links', 20)):
                            break
                        time.sleep(self.delay)

                    if linked_results:
                        player_data['pages']['linked_pages'] = linked_results
                except Exception as e:
                    logger.error(f"Error following links for {slug}: {e}")

            self.save_player_json(player_data, slug, player_id)
            all_player_data.append(player_data)
            
            logger.info(f"Completed player: {player_name} ({player_id})")
        
        return all_player_data


@dataclass
class RedditComment:
    """Reddit comment data class"""
    comment_id: str
    author: str
    body: str
    score: int
    created_utc: float
    parent_id: str
    permalink: str
    is_submitter: bool
    depth: int = 0
    parent_body: str = ""
    post_title: str = ""
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        data = asdict(self)
        data['created_time'] = datetime.fromtimestamp(self.created_utc).strftime('%Y-%m-%d %H:%M:%S')
        return data


@dataclass
class RedditPost:
    """Reddit post data class"""
    post_id: str
    title: str
    author: str
    selftext: str
    url: str
    permalink: str
    score: int
    num_comments: int
    created_utc: float
    subreddit: str
    flair: Optional[str]
    comments: List[RedditComment] = None
    
    def __post_init__(self):
        if self.comments is None:
            self.comments = []
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        data = asdict(self)
        data['created_time'] = datetime.fromtimestamp(self.created_utc).strftime('%Y-%m-%d %H:%M:%S')
        data['comments'] = [c.to_dict() for c in self.comments]
        return data


class Redit_Twitter_Scraper:
    """Scrapes Reddit and Twitter (via Nitter). Supports keyword-driven searches from external config."""
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    BASE_URL = "https://www.reddit.com"
    
    def __init__(self, delay: float = 2.0, keywords_file: Optional[str] = None):
        self.conf = Config()
        self.delay = delay
        self.target_pages = 10000
        self.posts_per_page = 100
        self.subreddit = "soccer"
        self.sort = "new"
        self.time_filter = "all"
        self.fetch_comments = True
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.last_request_time = 0
        # External keyword/subreddit lists
        self.keywords: List[str] = []
        self.subreddits: List[str] = []
        if keywords_file:
            try:
                self.load_comment_config(keywords_file)
            except Exception:
                logger.warning(f"Could not load keywords from {keywords_file}")
        
        # Twitter/Nitter configuration
        self.nitter_instances = [
            "https://nitter.privacydev.net",
            "https://nitter.tiekoetter.com",
            "https://nitter.woodland.cafe",
            "https://nitter.fdn.fr",
            "https://nitter.kavin.rocks",
            "https://nitter.unixfox.eu",
            "https://nitter.cz",
            "https://nitter.projectsegfau.lt",
            "https://nitter.nl",
            "https://nitter.it"
        ]
        self.current_instance = 0
        self.timeout = 10
        self.max_workers = 3
        
        # Event keywords for categorization
        self.event_keywords = {
            'goal': ['goal', 'but', 'score', 'scored'],
            'match': ['match', 'game', 'vs', 'against'],
            'controversy': ['controversy', 'protest', 'scandal', 'dispute'],
            'penalty': ['penalty', 'pénalty', 'pk'],
            'save': ['save', 'arrêt', 'saved'],
            'celebration': ['celebration', 'celebrate', 'célébration'],
            'injury': ['injury', 'blessure', 'injured'],
            'red_card': ['red card', 'carton rouge', 'expelled'],
            'final': ['final', 'finale'],
            'semifinal': ['semifinal', 'semi-final', 'demi-finale']
        }
        
        self.team_data = {}
    
    def _rate_limit(self) -> None:
        """Simple rate limiting"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_request_time = time.time()
    
    def get_subreddit_posts(self, subreddit: str = "soccer", sort: str = "new", 
                            limit: int = 100, time_filter: str = "all", 
                            after: Optional[str] = None) -> Tuple[List[RedditPost], Optional[str]]:
        """Get posts from subreddit"""
        url = f"{self.BASE_URL}/r/{subreddit}/{sort}.json"
        params = {
            'limit': min(limit, 100),
            'raw_json': 1
        }
        
        if after:
            params['after'] = after
        if time_filter and sort == 'top':
            params['t'] = time_filter
        
        self._rate_limit()
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            posts = []
            for item in data['data']['children']:
                post_data = item['data']
                
                post = RedditPost(
                    post_id=post_data['id'],
                    title=post_data['title'],
                    author=post_data.get('author', '[deleted]'),
                    selftext=post_data.get('selftext', ''),
                    url=post_data.get('url', ''),
                    permalink=post_data['permalink'],
                    score=post_data['score'],
                    num_comments=post_data['num_comments'],
                    created_utc=post_data['created_utc'],
                    subreddit=post_data['subreddit'],
                    flair=post_data.get('link_flair_text')
                )
                posts.append(post)
            
            next_after = data['data'].get('after')
            return posts, next_after
            
        except Exception as e:
            logger.error(f"Error fetching subreddit posts: {e}")
            return [], None
    
    def get_post_comments(self, post: RedditPost, limit: int = 100) -> List[RedditComment]:
        """Get comments for a post"""
        url = f"{self.BASE_URL}{post.permalink}.json"
        params = {
            'limit': limit,
            'raw_json': 1,
            'depth': 10
        }
        
        self._rate_limit()
        
        comments = []
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if len(data) > 1:
                comments_data = data[1]['data']['children']
                
                def extract_comments(comment_list, depth=0):
                    for comment_item in comment_list:
                        if comment_item['kind'] == 't1':
                            comment_data = comment_item['data']
                            
                            comment = RedditComment(
                                comment_id=comment_data['id'],
                                author=comment_data.get('author', '[deleted]'),
                                body=comment_data.get('body', ''),
                                score=comment_data['score'],
                                created_utc=comment_data['created_utc'],
                                parent_id=comment_data['parent_id'],
                                permalink=comment_data['permalink'],
                                is_submitter=comment_data.get('is_submitter', False),
                                depth=depth,
                                parent_body='',
                                post_title=post.title
                            )
                            comments.append(comment)
                            
                            # Process replies
                            if 'replies' in comment_data and comment_data['replies']:
                                if isinstance(comment_data['replies'], dict):
                                    replies = comment_data['replies']['data']['children']
                                    extract_comments(replies, depth + 1)
                
                extract_comments(comments_data)
        
        except Exception as e:
            logger.error(f"Error fetching comments for post {post.post_id}: {e}")
        
        return comments
    
    def scrape_reddit_pages(self, num_pages: int = 10) -> List[RedditPost]:
        """Scrape multiple pages of Reddit posts"""
        all_posts = []
        after = None
        posts_per_page = 100
        total_pages = min(num_pages, self.target_pages // posts_per_page)
        
        logger.info(f"Scraping {total_pages} pages from r/{self.subreddit}")
        
        for page in range(total_pages):
            logger.info(f"Fetching page {page + 1}/{total_pages} (after: {after})")
            
            posts, next_after = self.get_subreddit_posts(
                subreddit=self.subreddit,
                sort=self.sort,
                limit=posts_per_page,
                time_filter=self.time_filter,
                after=after
            )
            
            if not posts:
                logger.warning("No posts received, stopping")
                break
            
            if self.fetch_comments:
                logger.info(f"Fetching comments for {len(posts)} posts...")
                for i, post in enumerate(posts, 1):
                    if i % 10 == 0:
                        logger.info(f"  Processed {i}/{len(posts)} posts")
                    post.comments = self.get_post_comments(post)
            
            all_posts.extend(posts)
            after = next_after
            
            if not after:
                logger.info("No more pages available")
                break
        
        logger.info(f"Scraped {len(all_posts)} posts with comments")
        return all_posts

    def load_comment_config(self, path: str) -> None:
        """Load keywords and optional subreddits from a JSON config file.

        Expected format: {"keywords": [...], "subreddits": [...]}
        """
        try:
            if not os.path.exists(path):
                logger.warning(f"Comment config not found: {path}")
                return
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                kws = data.get('keywords') or data.get('key_words') or data.get('keyWords')
                subs = data.get('subreddits') or data.get('subreddits_list') or data.get('subreddit')
                if isinstance(kws, list):
                    self.keywords = [str(k).strip() for k in kws if k]
                if isinstance(subs, list):
                    self.subreddits = [str(s).strip() for s in subs if s]
                logger.info(f"Loaded {len(self.keywords)} keywords and {len(self.subreddits)} subreddits from {path}")
        except Exception as e:
            logger.error(f"Error loading comment config: {e}")

    def get_search_posts(self, keyword: str, limit: int = 100) -> Tuple[List[RedditPost], Optional[str]]:
        """Search Reddit globally for a keyword using the public search endpoint."""
        url = f"{self.BASE_URL}/search.json"
        params = {
            'q': keyword,
            'limit': min(limit, 100),
            'sort': self.sort,
            't': self.time_filter,
            'raw_json': 1
        }

        self._rate_limit()
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            posts = []
            for item in data.get('data', {}).get('children', []):
                post_data = item.get('data', {})
                try:
                    post = RedditPost(
                        post_id=post_data.get('id', ''),
                        title=post_data.get('title', ''),
                        author=post_data.get('author', '[deleted]'),
                        selftext=post_data.get('selftext', ''),
                        url=post_data.get('url', ''),
                        permalink=post_data.get('permalink', ''),
                        score=post_data.get('score', 0),
                        num_comments=post_data.get('num_comments', 0),
                        created_utc=post_data.get('created_utc', 0),
                        subreddit=post_data.get('subreddit', ''),
                        flair=post_data.get('link_flair_text')
                    )
                    posts.append(post)
                except Exception:
                    continue

            next_after = data.get('data', {}).get('after')
            return posts, next_after
        except Exception as e:
            logger.error(f"Error searching posts for '{keyword}': {e}")
            return [], None

    def scrape_by_keywords(self, keywords: List[str] = None, per_keyword_limit: int = 100) -> List[RedditPost]:
        """Scrape Reddit posts by keyword list. Fetch comments if enabled."""
        if keywords is None:
            keywords = self.keywords
        if not keywords:
            logger.warning("No keywords provided for scrape_by_keywords")
            return []

        all_posts: List[RedditPost] = []
        processor = BatchProcessor(batch_size=self.conf.batch_size, output_dir="reddit_data")
        batch = []
        batch_num = 1

        for kw in keywords:
            logger.info(f"Searching Reddit for keyword: {kw}")
            posts, _ = self.get_search_posts(kw, limit=per_keyword_limit)
            if not posts:
                logger.info(f"No posts found for keyword: {kw}")
                continue

            if self.fetch_comments:
                for i, post in enumerate(posts, 1):
                    post.comments = self.get_post_comments(post)

            batch.extend(posts)
            all_posts.extend(posts)

            # Save batches periodically
            if len(batch) >= processor.batch_size:
                processor.process_batch(batch, batch_num, fetch_comments=self.fetch_comments)
                batch = []
                batch_num += 1

            time.sleep(self.delay)

        if batch:
            processor.process_batch(batch, batch_num, fetch_comments=self.fetch_comments)

        logger.info(f"Keyword-based scraping complete: {len(all_posts)} posts collected")
        return all_posts
    
    def scrape_soccer_10000_pages(self) -> None:
        """Scrape 10000 pages of soccer content (main method)"""
        logger.info("Starting Reddit scraper for soccer content...")
        
        processor = BatchProcessor(batch_size=self.conf.batch_size, output_dir="reddit_data")
        progress = processor.load_progress()
        
        current_after = progress["last_after"]
        current_batch = progress["current_batch"]
        
        posts_per_page = 100
        pages_per_batch = 10
        num_batches = self.target_pages // pages_per_batch
        
        logger.info(f"Target: {self.target_pages} pages, {num_batches} batches")
        
        while current_batch <= num_batches:
            if current_batch in progress["completed_batches"]:
                logger.info(f"Batch {current_batch} already completed, skipping")
                current_batch += 1
                continue
            
            logger.info(f"\n{'='*60}")
            logger.info(f"STARTING BATCH {current_batch}/{num_batches}")
            logger.info(f"{'='*60}")
            
            batch_posts = []
            pages_in_batch = 0
            
            while pages_in_batch < pages_per_batch:
                remaining = pages_per_batch - pages_in_batch
                limit = min(posts_per_page, remaining * posts_per_page)
                
                logger.info(f"Fetching page {progress['pages_fetched'] + 1} (after: {current_after})")
                
                posts, next_after = self.get_subreddit_posts(
                    subreddit=self.subreddit,
                    sort=self.sort,
                    limit=limit,
                    time_filter=self.time_filter,
                    after=current_after
                )
                
                if not posts:
                    logger.warning("No posts received, stopping batch")
                    break
                
                batch_posts.extend(posts)
                pages_in_batch += 1
                progress["pages_fetched"] += 1
                current_after = next_after
                
                progress["last_after"] = current_after
                processor.save_progress(progress)
                
                if not current_after:
                    logger.info("No more pages available (after=None)")
                    break
            
            if batch_posts:
                # Fetch comments if enabled
                if self.fetch_comments:
                    logger.info(f"Fetching comments for {len(batch_posts)} posts...")
                    for i, post in enumerate(batch_posts, 1):
                        if i % 10 == 0:
                            logger.info(f"  Processed {i}/{len(batch_posts)} posts")
                        post.comments = self.get_post_comments(post)
                
                stats = processor.process_batch(batch_posts, current_batch, self.fetch_comments)
                progress["posts_collected"] += stats["posts"]
                progress["comments_collected"] += stats.get("comments", 0)
                progress["completed_batches"].append(current_batch)
                progress["current_batch"] = current_batch + 1
                progress["last_after"] = current_after
                processor.save_progress(progress)
                
                logger.info(f"Batch {current_batch} complete: {stats['posts']} posts, {stats.get('comments', 0)} comments")
            
            current_batch += 1
            
            if current_batch <= num_batches:
                logger.info("Pausing 30 seconds between batches...")
                time.sleep(30)
        
        logger.info("Reddit scraping complete!")
    
    def twitter_scraper(self, queries: List[str] = None) -> List[Dict]:
        """Scrape Twitter via Nitter instances"""
        if not queries:
            return []
        
        results = []
        
        for query in queries:
            for instance in self.nitter_instances:
                try:
                    url = f"{instance}/search?f=tweets&q={requests.utils.quote(query)}"
                    
                    self._rate_limit()
                    response = self.session.get(url, timeout=self.timeout)
                    
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        tweets = []
                        for tweet_div in soup.find_all('div', class_='timeline-item'):
                            content = tweet_div.find('div', class_='tweet-content')
                            if content:
                                tweet_text = content.get_text(strip=True)
                                tweets.append({
                                    'text': tweet_text,
                                    'query': query,
                                    'source': instance
                                })
                        
                        results.extend(tweets)
                        logger.info(f"Found {len(tweets)} tweets for query '{query}' on {instance}")
                        break  # Success, move to next query
                    
                except Exception as e:
                    logger.warning(f"Error with {instance} for query '{query}': {e}")
                    continue
            
            time.sleep(self.delay)
        
        return results


def main() -> None:
    """Main function to orchestrate all scrapers"""
    logger.info("Starting main scraping process...")
    
    # Setup driver
    driver = setup_driver()
    
    # Create config instance
    config = Config()
    
    # 1. URL Extraction
    logger.info("\n" + "="*60)
    logger.info("STEP 1: URL Extraction")
    logger.info("="*60)
    
    scraper1 = Urls_Extraction()
    path = os.path.join(os.path.dirname(__file__), "config", "urls.json")
    urls_data = config.load_config(path)
    
    if urls_data:
        results1 = scraper1.execution_url_agentent(driver, urls_data)
        logger.info(f"URL Extraction complete: {len(results1)} items processed")
    else:
        logger.warning("No URL data found, skipping URL extraction")
    
    # 2. News Scraping
    logger.info("\n" + "="*60)
    logger.info("STEP 2: News Scraping")
    logger.info("="*60)
    
    scraper2 = News_Scraper()
    tasks_path = os.path.join(os.path.dirname(__file__), "config", "tasks.json")
    tasks = Config.load_tasks(tasks_path)
    
    if tasks:
        results2 = scraper2.execution_url_agentent(driver, tasks)
        logger.info(f"News scraping complete: {len(results2)} articles collected")
    else:
        logger.warning("No tasks found, skipping news scraping")
    
    # 3. Transfermarkt Scraping
    logger.info("\n" + "="*60)
    logger.info("STEP 3: Transfermarkt Scraping")
    logger.info("="*60)
    
    players_path = os.path.join(os.path.dirname(__file__), "config", "players.json")
    players = Config.load_player_urls(players_path)
    
    if players:
        scraper3 = TransderMarkt_Scraper()
        results3 = scraper3.execution_url_agentent(driver, players)
        logger.info(f"Transfermarkt scraping complete: {len(results3)} players processed")
    else:
        logger.warning("No player URLs found, skipping Transfermarkt scraping")
    
    # 4. Reddit Scraping
    logger.info("\n" + "="*60)
    logger.info("STEP 4: Reddit Scraping")
    logger.info("="*60)
    
    # Load external comment.json (keywords/subreddits) if present
    comment_path = os.path.join(os.path.dirname(__file__), "config", "comment.json")
    reddit_scraper = Redit_Twitter_Scraper(keywords_file=comment_path)

    # If keywords are provided, perform keyword-based search; otherwise fallback to subreddit scraping
    if reddit_scraper.keywords:
        reddit_scraper.scrape_by_keywords()
    else:
        reddit_scraper.scrape_soccer_10000_pages()
    
    # 5. Twitter Scraping (optional)
    logger.info("\n" + "="*60)
    logger.info("STEP 5: Twitter Scraping (via Nitter)")
    logger.info("="*60)
    
    # Example queries - these should come from config in production
    twitter_queries = ["soccer", "football", "worldcup"]
    twitter_results = reddit_scraper.twitter_scraper(twitter_queries)
    logger.info(f"Twitter scraping complete: {len(twitter_results)} tweets collected")
    
    # Clean up
    if driver:
        try:
            driver.quit()
        except:
            pass
    
    logger.info("\n" + "="*60)
    logger.info("ALL SCRAPING COMPLETE!")
    logger.info("="*60)


if __name__ == "__main__":
    main()