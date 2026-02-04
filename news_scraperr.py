import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import datetime
import time
import re
from typing import List, Dict, Optional
import feedparser  # For RSS feeds
from urllib.parse import urljoin, urlparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FootballNewsScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # News outlets configuration
        self.outlets = {
            # 🇫🇷 French outlets
            'lequipe': {
                'name': 'L\'Équipe',
                'url': 'https://www.lequipe.fr/Football/',
                'type': 'website',
                'country': 'France',
                'category': 'Sports Newspaper'
            },
            'lemonde_football': {
                'name': 'Le Monde Football',
                'url': 'https://www.lemonde.fr/football/',
                'type': 'website',
                'country': 'France',
                'category': 'Newspaper'
            },
            'onzemondial': {
                'name': 'Onze Mondial',
                'url': 'https://www.onzemondial.com/',
                'type': 'website',
                'country': 'France',
                'category': 'Football Magazine'
            },        
            'caf_online': {
                'name': 'CAF Online',
                'url': 'https://www.cafonline.com',
                'type': 'website',
                'country': 'Africa',
                'category': 'Football Federation'
            },
            'olympics_com': {
                'name': 'Olympics.com',
                'url': 'https://www.olympics.com/en/news/afcon-2025-final-morocco-vs-senegal-live-updates',
                'type': 'article',
                'country': 'Global',
                'category': 'Sports Organization'
            },
            'aol_sports': {
                'name': 'AOL Sports',
                'url': 'https://www.aol.com/articles/africa-cup-nations-thrown-chaos-221424075.html',
                'type': 'article',
                'country': 'USA',
                'category': 'News Portal'
            },
            'reuters_sports': {
                'name': 'Reuters Sports',
                'url': 'https://www.reuters.com/sports/soccer/',
                'type': 'website',
                'country': 'Global',
                'category': 'News Agency'
            },
            'guardian_football': {
                'name': 'The Guardian Football',
                'url': 'https://www.theguardian.com/football',
                'type': 'website',
                'country': 'Global',
                'category': 'Newspaper'
            },
            'ap_sports': {
                'name': 'AP News Sports',
                'url': 'https://apnews.com/sports',
                'type': 'website',
                'country': 'Global',
                'category': 'News Agency'
            },
            'bbc_football': {
                'name': 'BBC Sport Football',
                'url': 'https://www.bbc.com/sport/football',
                'type': 'website',
                'country': 'Global',
                'category': 'Broadcaster'
            },
            
            # 🇲🇦 Moroccan outlets
            'morocco_world_news': {
                'name': 'Morocco World News',
                'url': 'https://www.moroccoworldnews.com/sports',
                'type': 'website',
                'country': 'Morocco',
                'category': 'News Portal'
            },
            'hespress_sport': {
                'name': 'Hespress Sport',
                'url': 'https://www.hespress.com/sport/',
                'type': 'website',
                'country': 'Morocco',
                'category': 'News Portal',
                'language': 'Arabic'
            },
            
            # 🇸🇳 Senegalese/African outlets
            'jeune_afrique': {
                'name': 'Jeune Afrique',
                'url': 'https://www.jeuneafrique.com/sports/',
                'type': 'website',
                'country': 'Pan-African',
                'category': 'Magazine'
            },
            
            # 🇺🇸 USA outlets
            'espn_fc': {
                'name': 'ESPN FC',
                'url': 'https://www.espn.com/soccer/',
                'type': 'website',
                'country': 'USA',
                'category': 'Sports Network'
            },
            'sports_illustrated': {
                'name': 'Sports Illustrated Soccer',
                'url': 'https://www.si.com/soccer',
                'type': 'website',
                'country': 'USA',
                'category': 'Sports Magazine'
            }
        }
        
        # AFCON 2025 specific keywords for filtering
        self.afcon_keywords = [
            'AFCON', 'Africa Cup of Nations', 'Africa Cup', 'CAN 2025',
            'Senegal', 'Morocco', 'Maroc', 'Sénégal',
            'Pape Gueye', 'Sadio Mané', 'Hakim Ziyech', 'Achraf Hakimi',
            'AFCON final', 'African Cup'
        ]
        
        # Specific search patterns for AFCON final
        self.afcon_final_patterns = [
            r'Senegal.*Morocco.*final',
            r'Maroc.*Sénégal.*finale',
            r'AFCON.*final.*2025',
            r'Pape Gueye.*goal',
            r'1-0.*extra time',
            r'VAR.*AFCON',
            r'Africa Cup.*final.*2025'
        ]

    def fetch_page(self, url: str, timeout: int = 10) -> Optional[str]:
        """Fetch webpage content"""
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    def parse_lequipe(self, html: str) -> List[Dict]:
        """Parse L'Équipe football news"""
        soup = BeautifulSoup(html, 'html.parser')
        articles = []
        
        # Look for article elements
        for item in soup.select('article, .feed-item, .article-item'):
            title_elem = item.select_one('h1, h2, h3, .title, .article-title')
            link_elem = item.select_one('a')
            summary_elem = item.select_one('p, .summary, .article-desc')
            date_elem = item.select_one('time, .date, .article-date')
            
            if title_elem and link_elem:
                title = title_elem.get_text(strip=True)
                link = link_elem.get('href')
                if link and not link.startswith('http'):
                    link = urljoin('https://www.lequipe.fr', link)
                
                summary = summary_elem.get_text(strip=True) if summary_elem else ''
                date = date_elem.get_text(strip=True) if date_elem else ''
                
                # Check if article is about football/AFCON
                if self.is_football_related(title, summary):
                    articles.append({
                        'title': title,
                        'link': link,
                        'summary': summary,
                        'date': date,
                        'outlet': 'L\'Équipe',
                        'country': 'France',
                        'category': 'Sports Newspaper'
                    })
        
        return articles

    def parse_guardian(self, html: str) -> List[Dict]:
        """Parse The Guardian football news"""
        soup = BeautifulSoup(html, 'html.parser')
        articles = []
        
        for item in soup.select('[data-testid="card-wrapper"]'):
            title_elem = item.select_one('a[data-testid="card-link"]')
            summary_elem = item.select_one('p')
            
            if title_elem:
                title = title_elem.get_text(strip=True)
                link = title_elem.get('href')
                if link and not link.startswith('http'):
                    link = urljoin('https://www.theguardian.com', link)
                
                summary = summary_elem.get_text(strip=True) if summary_elem else ''
                
                if self.is_football_related(title, summary):
                    articles.append({
                        'title': title,
                        'link': link,
                        'summary': summary,
                        'date': datetime.now().strftime('%Y-%m-%d'),
                        'outlet': 'The Guardian Football',
                        'country': 'Global',
                        'category': 'Newspaper'
                    })
        
        return articles

    def parse_bbc(self, html: str) -> List[Dict]:
        """Parse BBC Sport football news"""
        soup = BeautifulSoup(html, 'html.parser')
        articles = []
        
        for item in soup.select('article.sp-o-story, .gs-c-promo'):
            title_elem = item.select_one('h3, .gs-c-promo-heading')
            link_elem = item.select_one('a')
            summary_elem = item.select_one('p')
            
            if title_elem and link_elem:
                title = title_elem.get_text(strip=True)
                link = link_elem.get('href')
                if link and not link.startswith('http'):
                    link = urljoin('https://www.bbc.com', link)
                
                summary = summary_elem.get_text(strip=True) if summary_elem else ''
                
                if self.is_football_related(title, summary):
                    articles.append({
                        'title': title,
                        'link': link,
                        'summary': summary,
                        'date': datetime.now().strftime('%Y-%m-%d'),
                        'outlet': 'BBC Sport Football',
                        'country': 'Global',
                        'category': 'Broadcaster'
                    })
        
        return articles

    def parse_espn(self, html: str) -> List[Dict]:
        """Parse ESPN FC news"""
        soup = BeautifulSoup(html, 'html.parser')
        articles = []
        
        for item in soup.select('.contentItem__content'):
            title_elem = item.select_one('h2, .contentItem__title')
            link_elem = item.select_one('a')
            summary_elem = item.select_one('p')
            
            if title_elem and link_elem:
                title = title_elem.get_text(strip=True)
                link = link_elem.get('href')
                if link and not link.startswith('http'):
                    link = urljoin('https://www.espn.com', link)
                
                summary = summary_elem.get_text(strip=True) if summary_elem else ''
                
                if self.is_football_related(title, summary):
                    articles.append({
                        'title': title,
                        'link': link,
                        'summary': summary,
                        'date': datetime.now().strftime('%Y-%m-%d'),
                        'outlet': 'ESPN FC',
                        'country': 'USA',
                        'category': 'Sports Network'
                    })
        
        return articles

    def parse_morocco_news(self, html: str) -> List[Dict]:
        """Parse Morocco World News sports section"""
        soup = BeautifulSoup(html, 'html.parser')
        articles = []
        
        for item in soup.select('article, .post, .news-item'):
            title_elem = item.select_one('h2, h3, .entry-title, .title')
            link_elem = item.select_one('a')
            summary_elem = item.select_one('p, .entry-summary')
            
            if title_elem and link_elem:
                title = title_elem.get_text(strip=True)
                link = link_elem.get('href')
                if link and not link.startswith('http'):
                    link = urljoin('https://www.moroccoworldnews.com', link)
                
                summary = summary_elem.get_text(strip=True) if summary_elem else ''
                
                if self.is_football_related(title, summary):
                    articles.append({
                        'title': title,
                        'link': link,
                        'summary': summary,
                        'date': datetime.now().strftime('%Y-%m-%d'),
                        'outlet': 'Morocco World News',
                        'country': 'Morocco',
                        'category': 'News Portal'
                    })
        
        return articles

    def parse_jeune_afrique(self, html: str) -> List[Dict]:
        """Parse Jeune Afrique sports news"""
        soup = BeautifulSoup(html, 'html.parser')
        articles = []
        
        for item in soup.select('article, .post, .article'):
            title_elem = item.select_one('h2, h3, .article-title')
            link_elem = item.select_one('a')
            summary_elem = item.select_one('p, .article-excerpt')
            
            if title_elem and link_elem:
                title = title_elem.get_text(strip=True)
                link = link_elem.get('href')
                if link and not link.startswith('http'):
                    link = urljoin('https://www.jeuneafrique.com', link)
                
                summary = summary_elem.get_text(strip=True) if summary_elem else ''
                
                if self.is_football_related(title, summary):
                    articles.append({
                        'title': title,
                        'link': link,
                        'summary': summary,
                        'date': datetime.now().strftime('%Y-%m-%d'),
                        'outlet': 'Jeune Afrique',
                        'country': 'Pan-African',
                        'category': 'Magazine'
                    })
        
        return articles

    def is_football_related(self, title: str, summary: str = "") -> bool:
        """Check if content is football-related"""
        text = f"{title} {summary}".lower()
        football_keywords = [
            'football', 'soccer', 'match', 'goal', 'player', 'team',
            'championship', 'league', 'tournament', 'cup', 'fixture',
            'transfer', 'manager', 'coach'
        ]
        
        # Check for football keywords
        if any(keyword in text for keyword in football_keywords):
            return True
        
        # Check for AFCON specifically
        if any(keyword.lower() in text for keyword in self.afcon_keywords):
            return True
            
        return False

    def is_afcon_final_related(self, title: str, summary: str = "") -> bool:
        """Check if content is specifically about AFCON 2025 final"""
        text = f"{title} {summary}".lower()
        
        # Check for AFCON keywords
        if not any(keyword.lower() in text for keyword in self.afcon_keywords):
            return False
        
        # Check for final-specific patterns
        for pattern in self.afcon_final_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        
        return False

    def scrape_outlet(self, outlet_key: str) -> List[Dict]:
        """Scrape a specific news outlet"""
        outlet = self.outlets.get(outlet_key)
        if not outlet:
            logger.error(f"Outlet {outlet_key} not found in configuration")
            return []
        
        logger.info(f"Scraping {outlet['name']}...")
        
        html = self.fetch_page(outlet['url'])
        if not html:
            return []
        
        # Route to appropriate parser
        parsers = {
            'lequipe': self.parse_lequipe,
            'guardian_football': self.parse_guardian,
            'bbc_football': self.parse_bbc,
            'espn_fc': self.parse_espn,
            'morocco_world_news': self.parse_morocco_news,
            'jeune_afrique': self.parse_jeune_afrique
        }
        
        parser = parsers.get(outlet_key, lambda x: [])
        articles = parser(html)
        
        # Add outlet info to each article
        for article in articles:
            article['outlet'] = outlet['name']
            article['country'] = outlet['country']
            article['category'] = outlet['category']
            article['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Check if it's AFCON final related
            article['is_afcon_final'] = self.is_afcon_final_related(
                article['title'], article.get('summary', '')
            )
        
        return articles

    def scrape_all_outlets(self) -> List[Dict]:
        """Scrape all configured outlets"""
        all_articles = []
        
        for outlet_key in self.outlets.keys():
            try:
                articles = self.scrape_outlet(outlet_key)
                all_articles.extend(articles)
                logger.info(f"Found {len(articles)} articles from {self.outlets[outlet_key]['name']}")
                
                # Be respectful with requests
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error scraping {outlet_key}: {e}")
                continue
        
        return all_articles

    def search_afcon_final_articles(self, articles: List[Dict]) -> List[Dict]:
        """Filter articles specifically about AFCON 2025 final"""
        return [article for article in articles if article.get('is_afcon_final', False)]

    def save_to_csv(self, articles: List[Dict], filename: str = "football_news.csv"):
        """Save articles to CSV file"""
        if not articles:
            logger.warning("No articles to save")
            return
        
        df = pd.DataFrame(articles)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        logger.info(f"Saved {len(articles)} articles to {filename}")

    def save_to_json(self, articles: List[Dict], filename: str = "football_news.json"):
        """Save articles to JSON file"""
        if not articles:
            logger.warning("No articles to save")
            return
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(articles, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(articles)} articles to {filename}")

    def export_afcon_summary(self, articles: List[Dict]):
        """Create a summary report for AFCON final coverage"""
        afcon_articles = self.search_afcon_final_articles(articles)
        
        if not afcon_articles:
            print("No AFCON 2025 final articles found.")
            return
        
        print("\n" + "="*80)
        print("AFCON 2025 FINAL COVERAGE SUMMARY")
        print("="*80)
        print(f"\nTotal articles about AFCON 2025 final: {len(afcon_articles)}\n")
        
        # Group by outlet
        outlets_coverage = {}
        for article in afcon_articles:
            outlet = article['outlet']
            outlets_coverage.setdefault(outlet, []).append(article)
        
        print("Coverage by outlet:")
        for outlet, articles in outlets_coverage.items():
            print(f"  • {outlet}: {len(articles)} article(s)")
            
            # Show titles of articles from this outlet
            for article in articles[:3]:  # Show up to 3 articles per outlet
                print(f"    - {article['title']}")
        
        # Key facts from articles
        print("\nKey facts extracted from coverage:")
        key_phrases = [
            "Senegal", "Morocco", "1-0", "extra time", "Pape Gueye",
            "VAR", "controversy", "final", "AFCON", "champion"
        ]
        
        for article in afcon_articles[:5]:  # Check first 5 articles
            text = f"{article['title']} {article.get('summary', '')}".lower()
            found_facts = []
            
            for phrase in key_phrases:
                if phrase.lower() in text:
                    found_facts.append(phrase)
            
            if found_facts:
                print(f"\nFrom {article['outlet']}:")
                print(f"  Title: {article['title']}")
                print(f"  Key mentions: {', '.join(found_facts)}")
                if article.get('link'):
                    print(f"  Link: {article['link']}")

    def get_twitter_search_links(self):
        """Generate Twitter search links for AFCON 2025 final discussions"""
        searches = [
            # English searches
            "AFCON 2025 final Senegal Morocco",
            "Senegal 1-0 Morocco extra time",
            "Pape Gueye goal AFCON final",
            "VAR controversy AFCON 2025",
            "Africa Cup of Nations 2025 final",
            
            # French searches
            "finale CAN 2025 Sénégal Maroc",
            "Sénégal 1-0 Maroc prolongation",
            "Pape Gueye but finale CAN",
            "VAR controverse CAN 2025",
            
            # Arabic/Moroccan searches
            "المغرب السنغال نهائي كان 2025",
            "السنغال 1-0 المغرب الوقت الإضافي",
            
            # Hashtags
            "#AFCON2025", "#CAN2025",
            "#Senegal", "#Morocco", "#Maroc",
            "#AFCONFinal", "#FinalCAN", 
            "#AFCON2025", "#CAN2025",
            "#AFCONFinal", "#FinalCAN",
        
        "#Senegal", "#Morocco", "#Maroc", "#Sénégal",
        "#TeamSenegal", "#TeamMorocco",
        "#TerangaLions", "#AtlasLions",
        
        "#PapeGueye", "#SadioMane", "#BrahimDiaz",
        "#EdouardMendy", "#Bounou",
        
        "#VAR", "#WalkOff", "#Panenka",
        "#AFCONControversy", "#AFCONChaos",

        "#Football", "#Soccer", "#AfricanFootball",
        
        "AFCON 2025 final",
        "Senegal vs Morocco",
        "Pape Gueye goal",
        "Senegal walk off protest",
        "Mendy penalty save"
        ]
        
        base_url = "https://twitter.com/search?q="
        twitter_links = {}
        
        print("\n" + "="*80)
        print("TWITTER SEARCH LINKS FOR AFCON 2025 FINAL")
        print("="*80)
        
        for search in searches:
            encoded_search = requests.utils.quote(search)
            url = f"{base_url}{encoded_search}&f=live"
            twitter_links[search] = url
            print(f"\n{search}:")
            print(f"  {url}")
        
        return twitter_links

    def get_direct_match_links(self):
        """Return direct links to AFCON 2025 final match coverage"""
        match_links = {
            # Potential direct article links (you'll need to find actual URLs)
            "Reuters AFCON Final": "https://www.reuters.com/sports/soccer/",
            "The Guardian Match Report": "https://www.theguardian.com/football",
            "L'Équipe Tactical Analysis": "https://www.lequipe.fr/Football/",
            "BBC Sport Coverage": "https://www.bbc.com/sport/football",
            "Jeune Afrique African Perspective": "https://www.jeuneafrique.com/sports/",
            "Morocco World News Local Coverage": "https://www.moroccoworldnews.com/sports",
            "ESPN FC International View": "https://www.espn.com/soccer/"
        }
        
        print("\n" + "="*80)
        print("DIRECT LINKS TO FOOTBALL NEWS OUTLETS")
        print("="*80)
        
        for outlet, link in match_links.items():
            print(f"\n{outlet}:")
            print(f"  {link}")
        
        return match_links


def main():
    """Main function to run the scraper"""
    scraper = FootballNewsScraper()
    
    print("="*80)
    print("FOOTBALL NEWS SCRAPER - AFCON 2025 FINAL COVERAGE")
    print("="*80)
    
    # Option 1: Generate search links
    print("\n1. Generating search links for AFCON 2025 final...")
    scraper.get_twitter_search_links()
    scraper.get_direct_match_links()
    
    # Ask user if they want to scrape
    choice = input("\nDo you want to scrape current news articles? (yes/no): ").lower()
    
    if choice == 'yes':
        # Option 2: Scrape all outlets
        print("\n2. Scraping football news from all outlets...")
        all_articles = scraper.scrape_all_outlets()
        
        if all_articles:
            print(f"\nTotal articles scraped: {len(all_articles)}")
            
            # Save results
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            scraper.save_to_csv(all_articles, f"football_news_{timestamp}.csv")
            scraper.save_to_json(all_articles, f"football_news_{timestamp}.json")
            
            # Show AFCON final summary
            scraper.export_afcon_summary(all_articles)
            
            # Show sample of articles
            print("\n" + "="*80)
            print("SAMPLE OF SCRAPED ARTICLES")
            print("="*80)
            
            for i, article in enumerate(all_articles[:10], 1):
                print(f"\n{i}. {article['outlet']} - {article['country']}")
                print(f"   Title: {article['title']}")
                print(f"   Summary: {article.get('summary', 'N/A')[:100]}...")
                if article.get('is_afcon_final'):
                    print(f"   ⭐ AFCON 2025 FINAL COVERAGE")
                print(f"   Link: {article.get('link', 'N/A')}")
        else:
            print("No articles were scraped. Check your internet connection or try again later.")
    
    print("\n" + "="*80)
    print("SCRAPING COMPLETE")
    print("="*80)
    
    # Display configuration
    print("\nConfigured outlets:")
    for key, outlet in scraper.outlets.items():
        print(f"  • {outlet['name']} ({outlet['country']}) - {outlet['category']}")


if __name__ == "__main__":
    # Install required packages if not already installed
    required_packages = ['requests', 'beautifulsoup4', 'pandas', 'feedparser']
    
    import subprocess
    import sys
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            print(f"Installing {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    
    main()