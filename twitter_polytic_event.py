import time
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import json
import re
from urllib.parse import quote_plus, urlparse
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import logging
from dateutil import parser as date_parser
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BingSportsPoliticsScraper:
    def __init__(self, headless=True):
        """Initialize the scraper with Chrome driver"""
        self.setup_driver(headless)
        self.target_date = datetime(2026, 1, 18)
        self.articles = []
        self.base_url = "https://www.bing.com/news"
        
    def setup_driver(self, headless=True):
        """Setup Chrome driver with options"""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        chrome_options.add_argument("--start-maximized")
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(self.driver, 20)
        
    def extract_date_from_text(self, text):
        """Extract date from various formats in Bing news"""
        try:
            # Common Bing date formats
            patterns = [
                r'(\d{1,2}\s+\w+\s+\d{4})',  # 18 January 2026
                r'(\d{1,2}/\d{1,2}/\d{4})',   # 18/01/2026
                r'(\d{4}-\d{2}-\d{2})',       # 2026-01-18
                r'(\w+\s+\d{1,2},\s+\d{4})',  # January 18, 2026
                r'(\d{1,2}\s+\w+\s+\d{4},\s+\d{1,2}:\d{2})',  # 18 January 2026, 12:00
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    return date_parser.parse(match.group(1), dayfirst=True)
            
            # Check for relative dates
            if 'hour' in text.lower() or 'minute' in text.lower():
                return datetime.now()
            elif 'day' in text.lower():
                match = re.search(r'(\d+)\s+day', text.lower())
                if match:
                    days_ago = int(match.group(1))
                    return datetime.now() - timedelta(days=days_ago)
                    
        except Exception as e:
            logger.warning(f"Could not parse date from '{text}': {e}")
        
        return None
    
    def is_sports_politics_related(self, title, description):
        """Check if article relates to sports AND politics intersection"""
        title_lower = title.lower()
        desc_lower = description.lower() if description else ""
        
        # Sports keywords
        sports_keywords = [
            'afcon', 'football', 'soccer', 'fifa', 'olympics', 'world cup',
            'sports', 'athlete', 'team', 'tournament', 'championship',
            'player', 'coach', 'league', 'match', 'game', 'stadium',
            'uefa', 'caf', 'nba', 'nfl', 'mlb', 'nhl'
        ]
        
        # Politics keywords
        politics_keywords = [
            'government', 'president', 'prime minister', 'minister',
            'political', 'diplomatic', 'protest', 'sanction', 'boycott',
            'embassy', 'foreign policy', 'relations', 'tension',
            'corruption', 'scandal', 'investigation', 'court', 'law',
            'regulation', 'policy', 'funding', 'subsidy', 'tax',
            'national', 'international', 'geopolitical', 'crisis',
            'election', 'vote', 'campaign', 'party', 'opposition'
        ]
        
        # Combined sports-politics intersection
        combined_keywords = [
            'sports politics', 'football diplomacy', 'sports sanctions',
            'political protest sports', 'government funding sports',
            'national team politics', 'sports boycott', 'stadium politics',
            'sports corruption', 'fifa scandal', 'olympics politics',
            'world cup politics', 'athlete protest', 'taking a knee',
            'political statement sports', 'sports and human rights',
            'sports washing', 'sports embargo', 'diplomatic incident sports'
        ]
        
        # Check if article contains both sports AND politics keywords
        has_sports = any(keyword in title_lower or keyword in desc_lower for keyword in sports_keywords)
        has_politics = any(keyword in title_lower or keyword in desc_lower for keyword in politics_keywords)
        
        # Or contains combined keywords
        has_combined = any(keyword in title_lower or keyword in desc_lower for keyword in combined_keywords)
        
        return (has_sports and has_politics) or has_combined
    
    def search_bing_news(self, query, date_filter=None):
        """Search Bing News with specific queries"""
        search_results = []
        
        try:
            # Encode query for URL
            encoded_query = quote_plus(query)
            
            # Build search URL with date filters if specified
            if date_filter == "before":
                search_url = f"{self.base_url}/search?q={encoded_query}+before:01/18/2026&qft=interval%3d%227%22&form=QBNH"
            elif date_filter == "after":
                search_url = f"{self.base_url}/search?q={encoded_query}+after:01/18/2026&qft=interval%3d%227%22&form=QBNH"
            else:
                search_url = f"{self.base_url}/search?q={encoded_query}&qft=interval%3d%227%22&form=QBNH"
            
            logger.info(f"Searching: {search_url}")
            self.driver.get(search_url)
            time.sleep(3)
            
            # Scroll to load more content
            for _ in range(3):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
            
            # Parse page
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Find news cards
            news_cards = soup.find_all(['div', 'article'], class_=re.compile(r'news-card|t_t|newsitem', re.I))
            
            # Also look for news items in other structures
            if not news_cards:
                news_cards = soup.find_all('div', {'class': re.compile(r'news|card|item', re.I)})
            
            logger.info(f"Found {len(news_cards)} news cards")
            
            for card in news_cards:
                try:
                    # Extract title
                    title_elem = card.find(['h2', 'h3', 'h4', 'a'], class_=re.compile(r'title|headline', re.I))
                    if not title_elem:
                        title_elem = card.find(['h2', 'h3', 'h4', 'a'])
                    
                    title = title_elem.text.strip() if title_elem else "No title"
                    
                    # Extract URL
                    link_elem = card.find('a', href=True)
                    if not link_elem and title_elem and title_elem.name == 'a':
                        link_elem = title_elem
                    
                    url = link_elem['href'] if link_elem else ""
                    
                    # Skip if no valid URL
                    if not url or 'bing.com' in url:
                        continue
                    
                    # Extract description
                    desc_elem = card.find(['p', 'div'], class_=re.compile(r'desc|snippet|summary', re.I))
                    description = desc_elem.text.strip() if desc_elem else ""
                    
                    # Extract source
                    source_elem = card.find(['span', 'div'], class_=re.compile(r'source|provider|author', re.I))
                    source = source_elem.text.strip() if source_elem else "Unknown"
                    
                    # Extract date
                    date_elem = card.find(['span', 'div'], class_=re.compile(r'time|date|timestamp', re.I))
                    date_text = date_elem.text.strip() if date_elem else ""
                    article_date = self.extract_date_from_text(date_text)
                    
                    # Determine period (before/after target date)
                    period = "unknown"
                    if article_date:
                        if article_date < self.target_date:
                            period = "before"
                        elif article_date > self.target_date:
                            period = "after"
                        else:
                            period = "on_date"
                    
                    # Check if sports-politics related
                    if self.is_sports_politics_related(title, description):
                        article_data = {
                            'title': title,
                            'url': url,
                            'description': description,
                            'source': source,
                            'date_text': date_text,
                            'article_date': article_date.strftime("%Y-%m-%d") if article_date else "Unknown",
                            'period': period,
                            'search_query': query,
                            'date_filter': date_filter,
                            'scraped_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        
                        search_results.append(article_data)
                        logger.info(f"Found: {title[:80]}... ({period})")
                        
                except Exception as e:
                    logger.warning(f"Error processing card: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error searching Bing for '{query}': {e}")
        
        return search_results
    
    def get_comprehensive_searches(self):
        """Get comprehensive search queries for sports and politics"""
        return [
            # AFCON specific
            ("AFCON politics Morocco Senegal", "afcon_politics"),
            ("Africa Cup of Nations political protest", "afcon_protest"),
            ("CAN 2026 diplomatic tension", "can_diplomatic"),
            ("Football diplomacy Africa", "football_diplomacy"),
            
            # General sports-politics intersection
            ("sports political protest 2026", "sports_protest"),
            ("government funding sports stadium", "stadium_politics"),
            ("athlete political statement 2026", "athlete_politics"),
            ("sports sanctions 2026", "sports_sanctions"),
            ("world cup political controversy", "worldcup_politics"),
            ("olympics geopolitics 2026", "olympics_geopolitics"),
            
            # Country specific
            ("Morocco sports politics 2026", "morocco_sports_pol"),
            ("Senegal football government", "senegal_football_gov"),
            ("Egypt sports political crisis", "egypt_sports_crisis"),
            ("South Africa sports apartheid legacy", "sa_sports_history"),
            
            # Event specific around Jan 18, 2026
            ("January 2026 sports political scandal", "jan2026_scandal"),
            ("2026 sports boycott political", "2026_boycott"),
            ("stadium protest January 2026", "stadium_protest_jan"),
            
            # International relations
            ("sports diplomatic incident 2026", "sports_diplomacy"),
            ("football war Africa 2026", "football_war"),
            ("sports embargo 2026", "sports_embargo"),
            
            # Corruption and investigations
            ("FIFA corruption 2026 investigation", "fifa_corruption"),
            ("sports corruption Africa 2026", "africa_sports_corrupt"),
            ("match fixing political 2026", "matchfixing_political"),
            
            # Social issues
            ("athlete human rights 2026", "athlete_rights"),
            ("sports racial discrimination 2026", "sports_racism"),
            ("gender equality sports politics 2026", "gender_sports_pol"),
            
            # Economic aspects
            ("sports economy politics 2026", "sports_economy"),
            ("world cup economic impact political", "wc_economic_pol"),
            ("stadium construction political corruption", "stadium_corruption")
        ]
    
    def run_comprehensive_search(self):
        """Run comprehensive search for all periods"""
        all_articles = []
        search_queries = self.get_comprehensive_searches()
        
        logger.info(f"Starting comprehensive search with {len(search_queries)} queries")
        
        for query, tag in search_queries:
            try:
                logger.info(f"\n{'='*60}")
                logger.info(f"Searching: {query}")
                logger.info(f"{'='*60}")
                
                # Search before Jan 18, 2026
                articles_before = self.search_bing_news(query, date_filter="before")
                for article in articles_before:
                    article['search_tag'] = tag
                    all_articles.append(article)
                
                # Search after Jan 18, 2026
                articles_after = self.search_bing_news(query, date_filter="after")
                for article in articles_after:
                    article['search_tag'] = tag
                    all_articles.append(article)
                
                # Search without date filter (to catch recent articles)
                articles_general = self.search_bing_news(query, date_filter=None)
                for article in articles_general:
                    article['search_tag'] = tag
                    # Classify based on extracted date
                    if article['period'] != 'unknown':
                        all_articles.append(article)
                
                # Be polite
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Error in search '{query}': {e}")
                continue
        
        # Remove duplicates by URL
        unique_articles = []
        seen_urls = set()
        
        for article in all_articles:
            if article['url'] not in seen_urls:
                seen_urls.add(article['url'])
                unique_articles.append(article)
        
        self.articles = unique_articles
        return unique_articles
    
    def extract_full_article_content(self, url):
        """Extract full article content from URL"""
        try:
            self.driver.get(url)
            time.sleep(3)
            
            # Try multiple selectors for article content
            selectors = [
                'article',
                '[itemprop="articleBody"]',
                '.article-content',
                '.post-content',
                '.story-content',
                'main',
                '.content',
                '#content',
                '.entry-content'
            ]
            
            content_parts = []
            for selector in selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elements:
                        text = elem.text.strip()
                        if len(text) > 100:  # Likely actual content
                            content_parts.append(text)
                except:
                    continue
            
            # If no specific selectors found, get all paragraphs
            if not content_parts:
                paragraphs = self.driver.find_elements(By.TAG_NAME, 'p')
                for p in paragraphs[:50]:  # Limit to first 50 paragraphs
                    text = p.text.strip()
                    if len(text) > 50:
                        content_parts.append(text)
            
            full_content = "\n\n".join(content_parts[:20])  # Limit content length
            
            return full_content[:5000]  # Truncate very long articles
            
        except Exception as e:
            logger.warning(f"Could not extract content from {url}: {e}")
            return ""
    
    def enhance_with_content(self, max_articles=50):
        """Extract full content for top articles"""
        logger.info(f"\nExtracting full content for {min(max_articles, len(self.articles))} articles...")
        
        enhanced_articles = []
        for i, article in enumerate(self.articles[:max_articles]):
            try:
                logger.info(f"Extracting content {i+1}/{min(max_articles, len(self.articles))}: {article['title'][:60]}...")
                
                content = self.extract_full_article_content(article['url'])
                article['full_content'] = content
                article['content_length'] = len(content)
                
                # Extract key phrases
                if content:
                    # Look for political references
                    political_terms = ['government', 'minister', 'president', 'policy', 'protest', 
                                      'sanction', 'diplomatic', 'political', 'corruption']
                    
                    found_terms = [term for term in political_terms if term in content.lower()]
                    article['political_terms_found'] = ", ".join(found_terms[:5])
                    
                    # Look for sports references
                    sports_terms = ['football', 'soccer', 'match', 'player', 'team', 'tournament',
                                   'stadium', 'coach', 'league', 'championship']
                    
                    found_sports = [term for term in sports_terms if term in content.lower()]
                    article['sports_terms_found'] = ", ".join(found_sports[:5])
                
                enhanced_articles.append(article)
                time.sleep(1)  # Be polite
                
            except Exception as e:
                logger.warning(f"Failed to enhance article {article['url']}: {e}")
                article['full_content'] = ""
                enhanced_articles.append(article)
        
        self.articles = enhanced_articles
        return enhanced_articles
    
    def save_results(self, format='all'):
        """Save results to files"""
        if not self.articles:
            logger.warning("No articles to save!")
            return None
        
        df = pd.DataFrame(self.articles)
        
        # Sort by period and date
        df['date_sort'] = pd.to_datetime(df['article_date'], errors='coerce')
        df = df.sort_values(['period', 'date_sort'], ascending=[True, False])
        df = df.drop('date_sort', axis=1)
        
        # Create summary statistics
        period_counts = df['period'].value_counts()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save to multiple formats
        if format in ['csv', 'all']:
            csv_file = f'bing_sports_politics_{timestamp}.csv'
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            logger.info(f"Saved {len(df)} articles to {csv_file}")
        
        if format in ['excel', 'all']:
            excel_file = f'bing_sports_politics_{timestamp}.xlsx'
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='All Articles', index=False)
                
                # Create summary sheet
                summary_data = {
                    'Period': ['Before Jan 18, 2026', 'After Jan 18, 2026', 'On Jan 18, 2026', 'Unknown'],
                    'Count': [
                        period_counts.get('before', 0),
                        period_counts.get('after', 0),
                        period_counts.get('on_date', 0),
                        period_counts.get('unknown', 0)
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
            logger.info(f"Saved {len(df)} articles to {excel_file}")
        
        if format in ['json', 'all']:
            json_file = f'bing_sports_politics_{timestamp}.json'
            df.to_json(json_file, orient='records', indent=2, force_ascii=False)
            logger.info(f"Saved {len(df)} articles to {json_file}")
        
        # Print comprehensive report
        print(f"\n{'='*80}")
        print("BING SPORTS & POLITICS NEWS SCRAPER - COMPREHENSIVE REPORT")
        print(f"{'='*80}")
        print(f"\nSearch Period: Around January 18, 2026")
        print(f"Total Articles Found: {len(df)}")
        print(f"Search Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print(f"\n{'='*80}")
        print("DISTRIBUTION BY PERIOD:")
        print(f"{'='*80}")
        print(f"Before Jan 18, 2026: {period_counts.get('before', 0)} articles")
        print(f"After Jan 18, 2026:  {period_counts.get('after', 0)} articles")
        print(f"On Jan 18, 2026:    {period_counts.get('on_date', 0)} articles")
        print(f"Date Unknown:       {period_counts.get('unknown', 0)} articles")
        
        print(f"\n{'='*80}")
        print("TOP SOURCES:")
        print(f"{'='*80}")
        for source, count in df['source'].value_counts().head(10).items():
            print(f"{source}: {count} articles")
        
        print(f"\n{'='*80}")
        print("SAMPLE ARTICLES:")
        print(f"{'='*80}")
        
        # Show sample from each period
        for period in ['before', 'after', 'on_date']:
            period_df = df[df['period'] == period]
            if not period_df.empty:
                print(f"\n{period.upper()} Jan 18, 2026:")
                for idx, row in period_df.head(3).iterrows():
                    print(f"\n{idx+1}. {row['title'][:80]}...")
                    print(f"   Source: {row['source']}")
                    print(f"   Date: {row['article_date']}")
                    print(f"   URL: {row['url'][:80]}...")
        
        print(f"\n{'='*80}")
        print("SEARCH QUERIES USED:")
        print(f"{'='*80}")
        for query_tag, count in df['search_tag'].value_counts().head(10).items():
            print(f"{query_tag}: {count} articles")
        
        # Save URLs separately for easy access
        urls_file = f'bing_article_urls_{timestamp}.txt'
        with open(urls_file, 'w', encoding='utf-8') as f:
            f.write("Bing Sports & Politics News Article URLs\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total URLs: {len(df)}\n")
            f.write("="*80 + "\n\n")
            
            for period in ['before', 'on_date', 'after', 'unknown']:
                period_urls = df[df['period'] == period]
                if not period_urls.empty:
                    f.write(f"\n{'='*60}\n")
                    f.write(f"ARTICLES {period.upper()} JAN 18, 2026 ({len(period_urls)} articles)\n")
                    f.write(f"{'='*60}\n\n")
                    
                    for idx, row in period_urls.iterrows():
                        f.write(f"{idx+1}. {row['title']}\n")
                        f.write(f"   URL: {row['url']}\n")
                        f.write(f"   Source: {row['source']}\n")
                        f.write(f"   Date: {row['article_date']}\n")
                        f.write(f"   Query: {row['search_query']}\n")
                        f.write("-"*60 + "\n")
        
        logger.info(f"Saved URL list to {urls_file}")
        print(f"\n{'='*80}")
        print(f"All files saved with timestamp: {timestamp}")
        print(f"{'='*80}")
        
        return df
    
    def close(self):
        """Close the driver"""
        if self.driver:
            self.driver.quit()
            logger.info("Browser closed")

def main():
    """Main function to run the scraper"""
    print("\n" + "="*80)
    print("BING SPORTS & POLITICS NEWS SCRAPER")
    print("Focused on articles around January 18, 2026")
    print("="*80)
    print("\nNote: This scraper collects articles at the intersection of sports and politics.")
    print("It searches for articles BEFORE and AFTER January 18, 2026.")
    print("-"*80)
    
    # User options
    print("\nSelect options:")
    print("1. Quick search (50 articles max)")
    print("2. Comprehensive search (all queries)")
    print("3. Extract full article content (slower but more detailed)")
    
    choice = input("\nEnter choice (1-3, default=2): ").strip() or "2"
    
    # Initialize scraper
    scraper = BingSportsPoliticsScraper(headless=False)  # Set to False to see browser
    
    try:
        if choice == "1":
            print("\nRunning quick search...")
            # Run specific query as example
            articles = scraper.search_bing_news("AFCON politics protest", date_filter=None)
            scraper.articles = articles
            
        elif choice == "2":
            print("\nRunning comprehensive search...")
            articles = scraper.run_comprehensive_search()
            
        elif choice == "3":
            print("\nRunning comprehensive search with content extraction...")
            articles = scraper.run_comprehensive_search()
            articles = scraper.enhance_with_content(max_articles=30)
        
        # Save results
        if scraper.articles:
            df = scraper.save_results(format='all')
            
            # Ask about content extraction
            if choice != "3" and len(scraper.articles) > 0:
                extract_content = input("\nExtract full article content for top articles? (y/n, default=n): ").strip().lower()
                if extract_content == 'y':
                    max_articles = input(f"How many articles? (1-{len(scraper.articles)}, default=20): ").strip()
                    max_articles = int(max_articles) if max_articles.isdigit() else 20
                    scraper.enhance_with_content(max_articles=min(max_articles, len(scraper.articles)))
                    scraper.save_results(format='excel')
        else:
            print("\nNo articles found. Try adjusting search terms.")
            
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        scraper.close()
    
    print("\n" + "="*80)
    print("Scraping completed!")
    print("="*80)

if __name__ == "__main__":
    main()