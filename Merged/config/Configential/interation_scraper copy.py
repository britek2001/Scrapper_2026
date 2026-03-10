import json 
import argparse 
import shutil 
from pathlib import Path 
import os
import time 
import json
from typing import Optional
import re
import csv
import html2text
from urllib.parse import urlparse, urljoin
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options    
from gnews import GNews
from newspaper import Article
import pandas as pd
from bs4 import BeautifulSoup 
from abc import ABC, abstractmethod
import requests

# Aproach Using PLAYWRIGHT
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

# Aproach Using SELENIUM  
try:   
    from selenium import webdriver
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False


HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

class Config:

    def __init__(self):
        self.timeout = 30 
        self.max_content_length = 15000
        self.delay_between_requests = 2  
        self.user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        self.output_directory = 'content_output'
        self.headless = False  
        self.batch_size = 100

    def load_config(self, path=None):

        if not os.path.exists(self.output_directory):
            os.makedirs(self.output_directory)

        if not path:
            path = os.path.join(os.path.dirname(__file__), "config", "urls_news.json")

        try:
            with open(path, "r", encoding="utf-8") as f:
                            data = json.load(f)
                            if isinstance(data, dict):
                                return [data]
                            return data
        except Exception:
            return {}
    
    def generate_report(result, stats):
        if not isinstance(result, dict):
            return
        if result.get('success'):
            stats['success'] += 1
            stats['total_words'] += result.get('word_count', 0)
            stats['total_lines'] += result.get('line_count', 0)
        else:
            stats['failed'] += 1
    
    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new") 
        chrome_options.add_argument(f"user-agent={self.conf.user_agent}")
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(self.config.timeout)
        return driver
    
    def ensure_driver_alive(self, driver):
        try:
            _ = driver.current_url
            return driver
        except Exception as e:
            print("  Driver dead, restarting Selenium driver...")
            try:
                driver.quit()
            except:
                pass
            return self.setup_driver()

class Scraper(ABC):
    def __init__(self):
        self.conf = Config()
        self.name = self.__class__.__name__.lower()
        self.driver = None

    @abstractmethod
    def execution_scrapper(self, driver, urls: list):
        pass
    
    @abstractmethod
    def generate_report(self, result, stats):
        if not isinstance(result, dict):
            return
        if result.get('success'):
            stats['success'] += 1
            stats['total_words'] += result.get('word_count', 0)
            stats['total_lines'] += result.get('line_count', 0)
        else:
            stats['failed'] += 1
    
    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new") 
        chrome_options.add_argument(f"user-agent={self.user_agent}")
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(self.config.timeout)
        return driver

class DataExporter:

    def to_json(self, posts, filename):
        data = []
        for p in posts:
            data.append({
                "post_id": p.post_id,
                "title": p.title,
                "author": p.author,
                "selftext": p.selftext,
                "url": p.url,
                "permalink": p.permalink,
                "score": p.score,
                "num_comments": p.num_comments,
                "created_utc": p.created_utc,
                "subreddit": p.subreddit,
                "flair": p.flair,
                "comments": [c.__dict__ for c in p.comments]
            })

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def to_csv_posts(self, posts, filename):

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            writer.writerow([
                "post_id",
                "title",
                "author",
                "score",
                "num_comments",
                "created_utc",
                "subreddit",
                "flair"
            ])

            for p in posts:
                writer.writerow([
                    p.post_id,
                    p.title,
                    p.author,
                    p.score,
                    p.num_comments,
                    p.created_utc,
                    p.subreddit,
                    p.flair
                ])

    def to_csv_comments(self, posts, filename):

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            writer.writerow([
                "comment_id",
                "post_id",
                "author",
                "body",
                "score",
                "created_utc",
                "parent_id",
                "depth"
            ])

            for p in posts:
                for c in p.comments:
                    writer.writerow([
                        c.comment_id,
                        p.post_id,
                        c.author,
                        c.body,
                        c.score,
                        c.created_utc,
                        c.parent_id,
                        c.depth
                    ])

class BatchProcessor:

    def __init__(self, scraper: Scraper, output_dir: str = "reddit_data"):
        self.scraper_name = scraper.name
        self.output_dir = output_dir
        #self.exporter = DataExporter()
        os.makedirs(output_dir, exist_ok=True)
        self.progress_file = os.path.join(output_dir, "progress.json")
        self.stats_file = os.path.join(output_dir, "stats.json")
        
    def load_progress(self) -> dict:
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass

        return {
            "items_processed": 0,
            "successful": 0,
            "failed": 0,
            "current_batch": 1,
            "completed_batches": [],
            "last_item": None,
            "start_time": datetime.now().isoformat(),
            "last_update": datetime.now().isoformat(),
            "scraper_specific": {} 
        }
    
    def save_progress(self, progress: dict):
        progress["last_update"] = datetime.now().isoformat()
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress, f, indent=2, ensure_ascii=False)

    def get_batch_filename(self, batch_num: int, data_type: str, ext: str) -> str:
       timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(
            self.output_dir, 
            f"batch_{batch_num:04d}_{data_type}_{timestamp}_{self.scraper_name}.{ext}"
        )

    def update_stats_and_progress(self, stats, all_results, batch_counter):
            progress = self.batch_processor.load_progress()
            progress['items_processed'] = len(all_results)
            progress['successful'] = stats.get('success', 0)
            progress['failed'] = stats.get('failed', 0)
            progress['total_words'] = stats.get('total_words', 0)
            progress['total_lines'] = stats.get('total_lines', 0)
            progress['completed_batches'].append(batch_counter)
            progress['current_batch'] = batch_counter + 1
            self.batch_processor.save_progress(progress)
            self.batch_processor.save_stats(stats)
    
    def save_stats(self, stats: dict):
        stats["last_updated"] = datetime.now().isoformat()
        with open(self.stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
    

    def save_batch_json(self, data: List[Any], batch_num: int, metadata: Optional[Dict] = None) -> str:
        filename = self.get_batch_filename(batch_num, "full", "json")
        output = {
            "metadata": metadata or {},
            "scraper": self.scraper_name,
            "batch_number": batch_num,
            "timestamp": datetime.now().isoformat(),
            "total_items": len(data),
            "data": data
        }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"  Saved JSON: {filename}")
        return filename
    
    def process_batch(self,  items: List[T],  batch_num: int,  exporter: Optional['DataExporter'] = None, data_type: str = "generic") -> Dict[str, Any]:
      
        json_file = self.save_batch_json(items, batch_num, {"type": data_type})

        successful = sum(1 for item in items if isinstance(item, dict) and item.get('success', False))
        failed = sum(1 for item in items if isinstance(item, dict) and not item.get('success', True))
        
        if successful == 0 and failed == 0:
            successful = len(items)  
        
        return {
            "batch_number": batch_num,
            "total_items": len(items),
            "successful": successful,
            "failed": failed,
            "files": {
                "json": json_file,
            },
            "timestamp": datetime.now().isoformat()
        }

class Urls_Extraction(Scraper): 

    def __init__(self, use_playwright: bool = True, use_selenium: bool = True, delay: float = 1.0):
        super().__init__()
        self.conf = Config()
        self.use_playwright = use_playwright and PLAYWRIGHT_AVAILABLE
        self.use_selenium = use_selenium and SELENIUM_AVAILABLE
        self.delay = delay 
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.session.headers.update({'User-Agent': self.conf.user_agent})
        self.driver = None
        self.batch_processor = None  

    def set_batch_processor(self, batch_processor):
        self.batch_processor = batch_processor

    def save_batch(self, batch, output_dir, start_idx, end_idx, timestamp):
    
        if self.batch_processor:
            progress = self.batch_processor.load_progress()
            batch_num = progress.get('current_batch', 1)
            
            json_file = self.batch_processor.save_batch_json(
                data=batch,
                batch_num=batch_num,
                metadata={
                    'start_idx': start_idx,
                    'end_idx': end_idx,
                    'timestamp': timestamp,
                    'type': 'url_extraction',
                    'scraper': 'urls_extraction'
                }
            )
            print(f"  Saved batch {start_idx}-{end_idx} as batch #{batch_num}")

        else: 
            exit(0)

    def setup_driver(self):  
        chrome_options = Options()
        chrome_options.add_argument("--headless=new") 
        chrome_options.add_argument(f"user-agent={self.user_agent}")
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(self.timeout)
        self.driver = driver
        return driver

    def ensure_driver_alive(self, driver): 

        try:
            _ = driver.current_url
            return driver
        except Exception as e:
            print("  Driver dead, restarting Selenium driver...")
            try:
                driver.quit()
            except:
                pass
            return self.setup_driver()  
    
    def extract_domain(self, driver, url):
        
        try: 
            print(f"Processing Execusion: {url[:60]}...")        
            parsed = urlparse(url)
            if not parsed.scheme:
                url = 'http://' + url
            driver.get(url)
            time.sleep(self.delay)

            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')

            for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
                element.decompose()
            
            found_urls = set()
            for link in soup.find_all('a', href=True):
                href = link['href'].strip()
                if href.startswith('http'):
                    found_urls.add(href)
                elif href.startswith('/'):
                    found_urls.add(urljoin(url, href))
            print(f"Found new URLS: {len(found_urls)}")

            title = soup.title.string.strip() if soup.title else ""
            
            content_text = ""
            article_tags = soup.find_all(['article', 'main'])
            if article_tags:
                content_text = ' '.join([tag.get_text(strip=True) for tag in article_tags])
            
            if not content_text or len(content_text) < 300:
                paragraphs = soup.find_all('p')
                content_text = ' '.join([p.get_text(strip=True) for p in paragraphs])
            
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
                'found_urls': list(found_urls), 
                'success': True
            }

        except Exception as e:
            print(f"Extract_domain error for {url}: {e}")
            return {'url': url, 'success': False, 'error': str(e)}

    def generate_report(self, result, stats):  
        if not isinstance(result, dict):
            return
        if result.get('success'):
            stats['success'] = stats.get('success', 0) + 1
            stats['total_words'] = stats.get('total_words', 0) + result.get('word_count', 0)
            stats['total_lines'] = stats.get('total_lines', 0) + result.get('line_count', 0)
        else:
            stats['failed'] = stats.get('failed', 0) + 1
    
    def process_url(self, driver, url_dict, all_results, stats, output_dir, timestamp):
        try:
            start_index = len(all_results)
            batch_counter = 1
            for i, url_info in enumerate(url_dict, 1):
                driver = self.ensure_driver_alive(driver)

                if isinstance(url_info, dict):
                    url_to_fetch = url_info.get('url')
                    category = url_info.get('category', '')
                else:
                    url_to_fetch = url_info
                    category = ''
                try:
                    result = self.extract_domain(driver, url_to_fetch)
                except WebDriverException as e:
                    print(f"  Selenium error: {str(e)[:80]}")
                    driver = self.ensure_driver_alive(driver)
                    try:
                        result = self.extract_domain(driver, url_to_fetch)
                    except Exception as e2:
                        print(f"  Second attempt failed: {str(e2)[:80]}")
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    print(f"  Error processing URL: {str(e)[:80]}")
                    result = {'url': url_to_fetch, 'success': False, 'error': str(e)}
                if category:
                    result['category'] = category
                
                all_results.append(result)
                self.generate_report(result, stats)
                
                
                batch_size = self.conf.batch_size
                if batch_size > 0 and i % batch_size == 0:
                    start_idx = i - batch_size + 1
                    end_idx = i
                    batch = all_results[start_idx-1:end_idx]
                    self.save_batch(batch, output_dir, start_idx, end_idx, timestamp)
                    
                    self.update_stats_and_progress(stats, all_results, batch_counter)
                    batch_counter += 1


                if i < len(url_dict):
                    time.sleep(self.conf.delay_between_requests)
            
            new_count = len(all_results) - start_index
            if new_count > 0 and new_count < self.conf.batch_size:
                self.save_batch(all_results[start_index:], output_dir, start_index + 1, len(all_results), timestamp)
                
        except KeyboardInterrupt:
            print("\nKeyboard interrupt received, stopping early...")
                if self.batch_processor:
                    self.batch_processor.save_stats(stats)
        finally:
            print("\nClosing Selenium driver...")
            try:
                driver.quit()
            except:
                pass
        
    def stats_dictonary_category(self, urls_data):
        dictonary_category = {}
        for entry in urls_data:
            category = entry.get('category', '')
            url = entry.get('url', '')
            if category not in dictonary_category:
                dictonary_category[category] = []
            dictonary_category[category].append(entry)
        return dictonary_category
    
    def execution_url_agentent(self, driver, urls_data):
        print("URL AGENT STARTING URL SCRAPING.... first we need some information")

        print("Loading URLs from config ... ")
        all_results = []
        
        driver = self.setup_driver()
        all_data = []
        total_count = 0 
        seen_titles = set()
        
        dictonary_category = self.stats_dictonary_category(urls_data)
        print("Dictionary", dictonary_category.keys())
        
        stats = {'success':0, 'failed':0, 'total_words':0, 'total_lines':0}

        output_dir = self.conf.output_directory
        timestamp = int(time.time())

        driver = self.setup_driver()

        try:
            for category, urls in dictonary_category.items():
                print(f"\nProcessing category: {category}")
                self.process_url(driver, urls, all_results, stats, output_dir, timestamp)
        finally:
            driver.quit() 

class News_Scraper:
    
    def __init__(self, use_playwright: bool = True, use_selenium: bool = True, delay: float = 1.0):
        self.conf = Config()
        self.use_playwright = use_playwright and PLAYWRIGHT_AVAILABLE
        self.use_selenium = use_selenium and SELENIUM_AVAILABLE
        self.delay = delay 
        self.session = request.Session()
        self.session.headers.update(HEADERS)
        self.start_date = "2023-01-01"
        self.end_date = "2024-12-31"
        self.max_results_per_keyword = 20
        self.languages = ['fr']
        self.countries = ['US', 'FR', 'DE', 'ES', 'IT']
        self.labels = ['france', 'usa', 'germany', 'spain', 'italy']
    
    def resolve_real_url(driver, google_url):
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

    def get_content_from_url(url):
        try:
            config = Config()
            config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            config.request_timeout = 15
            article = Article(url, config=config)
            article.download()
            article.parse()
            return article.text
        except:
            return None

    def setup_driver_GNews():
                keywords_news = GNews(
                    language=self.languages[0], 
                    country=self.countries[0], 
                    max_results=self.max_results_per_keyword,
                    start_date=self.start_date,
                    end_date=self.end_date
                )

    def execution_url_agentent(driver, tasks):
        stats = {'success':0, 'failed':0, 'total_words':0, 'total_lines':0}
        output_dir = self.conf['output_directory']
        timestamp = int(time.time())
        all_data = []
        total_count = 0
        seen_titles = set()
        try:
            for task in tasks:
                setup_driver_GNews()
                for keyword in task['keywords']:
                    try:
                        results = keywords_news.get_news(keyword)
                        for item in results:
                            title = item.get('title')
                            if title in seen_titles: continue
                            google_url = item.get('url')
                            if google_url:
                                real_url = resolve_real_url(driver, google_url)
                                if "google.com" in real_url: continue
                                seen_titles.add(title)
                                full_text = get_content_from_url(real_url)
                                if full_text and len(full_text) > 200: 
                                    all_data.append({
                                        'url': real_url,
                                        'title': title[:200],
                                        "domain": item.get('publisher', {}).get('title'),
                                        "date": item.get('published date'),
                                        "source_label": task['label'],
                                        "publisher": item.get('publisher', {}).get('title'),
                                        "date": item.get('published date'), 
                                        "content": full_text, 
                                        "word_count": len(full_text.split()),
                                        "lind_count": len(full_text.split('. ')),
                                        "success": True
                                    })

                                    total_count += 1
                                    print(f" [{total_count}] {label}: {title[:20]}...")
                                    
                                    News_Scraper.conf.generate_report(full_text, stats)
                                    batch_size = int(News_Scraper.conf.get('batch_size', 100))

                                    if batch_size > 0 and i % batch_size == 0:
                                        start_idx = i - (batch_size - 1)
                                        end_idx = i
                                        batch = all_results[start_idx-1:end_idx]
                                        News_Scraper.conf.save_batch(batch, output_dir, start_idx, end_idx, timestamp)
                    except Exception as e:
                        print(f"  Error processing keyword '{keyword}': {str(e)[:80]}")

class  TransderMarkt_Scraper():
   
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

    def __init__(self, use_playwright: bool = True, use_selenium: bool = True, delay: float = 1.0):
        
        self.conf = Config()
        self.use_playwright = use_playwright and PLAYWRIGHT_AVAILABLE
        self.use_selenium = use_selenium and SELENIUM_AVAILABLE
        self.delay = delay 
        self.session = request.Session()
        self.session.headers.update(HEADERS)
        self.base_directory = os.path.join(os.getcwd(), "transfermarkt_data")
        self.data_directory = os.path.join(self.base_directory, "data")
        self.player_data_directory = os.path.join(self.data_directory, "players")
        self.player_html_directory = os.path.join(self.data_directory, "html")
        
        for folder in [BASE_DIR, DATA_DIR, PLAYER_DATA_DIR, PLAYER_HTML_DIR]:
            os.makedirs(folder, exist_ok=True)

        for subfolder in self.PAGE_TYPES.values():
            os.makedirs(os.path.join(self.player_html_directory, subfolder), exist_ok=True)
    
    def extract_player_id_from_url(url):
        match = re.search(r'/spieler/(\d+)', url)
        return match.group(1) if match else None
    
    def extract_slug_from_url(url):
        try:
            parsed = urlparse(url)
            segments = [s for s in parsed.path.split('/') if s]
            if segments:
                return segments[0]
        except Exception:
            pass
        return "unknown"
   
    def fetch_page(self, url: str, page_type: str, player_id: str, slug: str) -> Optional[str]:
        html_filename = f"{slug}_{player_id}_{page_type}.html"
        html_path = os.path.join(PLAYER_HTML_DIR, PAGE_TYPES[page_type], html_filename)
        
        if os.path.exists(html_path):
            logger.info(f"Chargement depuis le cache: {html_path}")
            try:
                with open(html_path, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception as e:
                logger.warning(f"Erreur lors de la lecture du cache: {e}")
        
        time.sleep(self.delay)
        
        html_content = None
        
        try:
            logger.info(f"Téléchargement de {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            if response.status_code == 200:
                html_content = response.text
                
                if any(indicator in html_content.lower() for indicator in 
                       ['player not found', 'error', '404', 'seite nicht gefunden']):
                    logger.warning(f"Page invalide ou non trouvée: {url}")
                    html_content = None
        except Exception as e:
            logger.warning(f"Erreur avec requests pour {url}: {e}")
            html_content = None
        
        if not html_content and self.use_playwright and page_type not in ['news']:  # Éviter les pages dynamiques trop lourdes
            try:
                html_content = self.fetch_with_playwright(url)
            except Exception as e:
                logger.error(f"Erreur avec playwright pour {url}: {e}")
        
        if html_content:
            try:
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                logger.info(f"HTML sauvegardé: {html_path}")
            except Exception as e:
                logger.error(f"Erreur lors de la sauvegarde du HTML: {e}")
        
        return html_content
    

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

    def execution_url_agentent(driver, players):
        
        player_id = self.extract_player_id_from_url(base_url)
        slug = self.extract_slug_from_url(base_url) 
        
        if not player_id :
            logger.error(f"URL invalide, impossible d'extraire l'ID: {base_url}")
            return {}
        
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
                    page_data = {'html_saved': True, 'url': url}

                player_data['pages'][page_type] = page_data if page_data else {'error': 'No data extracted'}

            except Exception as e:
                logger.error(f"Erreur lors du scraping de {page_type}: {e}")
                player_data['pages'][page_type] = {'error': str(e)}

            time.sleep(self.delay) 

        self.save_player_json(player_data, slug, player_id)
        return player_data

@dataclass
class RedditComment:
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
    
    def to_dict(self):
        data = asdict(self)
        data['created_time'] = datetime.fromtimestamp(self.created_utc).strftime('%Y-%m-%d %H:%M:%S')
        return data

@dataclass
class RedditPost:
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
    
    def to_dict(self):
        data = asdict(self)
        data['created_time'] = datetime.fromtimestamp(self.created_utc).strftime('%Y-%m-%d %H:%M:%S')
        data['comments'] = [c.to_dict() for c in self.comments]
        return data

def Redit_Twitter_Scraper():

    BASE_URL = "https://www.reddit.com"

    def __init__(self, delay: float = 2.0):
        self.conf = Config()
        self.delay = delay
        self.target_pages = 10000
        self.posts_per_page = 100
        self.subreddit = "soccer"
        self.sort = "new"
        self.subreddit = "soccer"
        self.time_filter = "all"
        self.fetch_comments = True
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.last_request_time = 0
        self.time_filter = "all"
        self.fetch_comments = True
    
    def scrape_soccer_10000_pages(self):
        scraper = Redit_Twitter_Scraper(delay=2.0)
        processor = BatchProcessor(batch_size=scraper.conf.batch_size, output_dir="reddit_data")

        progess  = processor.load_progress()
        current_after = progress["last_after"]
        current_batch = progress["current_batch"]

        while current_batch <= num_batches:
            if current_batch in progress["completed_batches"]:
                logger.info(f"  Lot {current_batch} déjà complété, passage au suivant")
                current_batch += 1
                continue
            
            logger.info(f"\n{'='*60}")
            logger.info(f" DÉMARRAGE DU LOT {current_batch}/{num_batches}")
            logger.info(f"{'='*60}")
            
            batch_posts = []
            pages_in_batch = 0
            
            while pages_in_batch < BATCH_SIZE:
                remaining_in_batch = BATCH_SIZE - pages_in_batch
                limit = min(POSTS_PER_PAGE, remaining_in_batch * POSTS_PER_PAGE)
                
                logger.info(f" Récupération page {progress['pages_fetched'] + 1} (after: {current_after})")
                
                posts, next_after = scraper.get_subreddit_posts(
                    subreddit=SUBREDDIT,
                    sort=SORT,
                    limit=limit,
                    time_filter=TIME_FILTER,
                    after=current_after
                )
                
                if not posts:
                    logger.warning("  Aucun post récupéré, arrêt du lot")
                    break
                
                batch_posts.extend(posts)
                pages_in_batch += 1
                progress["pages_fetched"] += 1
                current_after = next_after
                
                progress["last_after"] = current_after
                processor.save_progress(progress)
                
                if not current_after:
                    logger.info("🏁 Plus de pages disponibles (after=None)")
                    break
            
            if batch_posts:

                stats = processor.process_batch(batch_posts, current_batch, FETCH_COMMENTS)
                progress["posts_collected"] += stats["posts"]
                progress["comments_collected"] += stats["comments"]
                progress["completed_batches"].append(current_batch)
                progress["current_batch"] = current_batch + 1
                progress["last_after"] = current_after
                processor.save_progress(progress)
                logger.info(f" Lot {current_batch} terminé: {stats['posts']} posts, {stats['comments']} commentaires")
            
            current_batch += 1
            
            if current_batch <= num_batches:
                logger.info("  Pause de 30 secondes entre les lots...")
                time.sleep(30)
        
    def Twitter_Scraper():
        def __init__(self):
            self.conf = Config()
            self.session = requests.Session()
            self.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,fr;q=0.8,ar;q=0.7',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            })
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
            self.match_config = match_config or {}
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
            
            if self.match_config:
                teams_cfg = self.match_config.get('teams', {})
                if teams_cfg:
                    for key, t in teams_cfg.items():
                        self.team_data[key] = {
                            'name': t.get('name', key),
                            'nicknames': t.get('nicknames', []),
                            'hashtags': t.get('hashtags', []),
                            'players': t.get('players', []),
                            'coach': t.get('coach', '')
                        }
                
                self.base_queries = [(q, f"cfg_{i}") for i, q in enumerate(self.match_config.get('queries', []), start=1)]
            else:
                self.base_queries = []

        def test_nitter_

def main(): 

    # Defining the driver
    driver = self.setup_driver()

    # Calling Agent Urls Extraction
    scraper = Urls_Extraction()

    # Uploading Commun data  
    path = os.path.join(os.path.dirname(__file__), "config", "urls.json")
    csv = Config.load_config(path)   
    
    # Url Agent for news scraping
    scraper.execution_url_agentent(driver, csv)


    scraper2 = News_Scraper()

    # Obtain the players Urls List
    config_path = os.path.join(os.path.dirname(__file__), "config", "tasks.json")
    tasks = Config.load_tasks(config_path)
    
    # News Scrapper Running 
    scraper2.execution_url_agentent(driver, tasks)
    
    # Load Players Urls List
    config_path = os.path.join(os.path.dirname(__file__), "config", "players.json")
    players = Config.load_player_urls(config_path)    

    # Transdermarkt Scraper Running
    scraper3 = TransderMarkt_Scraper()
    scraper3.execution_url_agentent(driver, players) 

    # Exute Redit Scraper 
    reddit_scraper = Redit_Twitter_Scraper()
    reddit_scraper.scrape_soccer_10000_pages()

    # Twitter Scraper Running 

if __name__ == "__main__":
    main()