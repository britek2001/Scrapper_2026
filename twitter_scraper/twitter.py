import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import json
import time
import re
import random
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from typing import List, Dict, Tuple
import warnings
warnings.filterwarnings('ignore')

try:
    from config_loader import load_matches
except Exception:
    load_matches = lambda: []

class TwitterAFCONScraper:
    
    def __init__(self, match_config: dict = None):
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
        
        try:
            pass
        except Exception:
            pass
        
        self.match_config = match_config or {}
        
        # Initialiser event_keywords par défaut
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
        
        # Initialiser team_data par défaut
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

    def test_nitter_instance(self, instance_url: str) -> bool:
        try:
            test_url = f"{instance_url}/search?q=test"
            response = self.session.get(test_url, timeout=5)
            return response.status_code in (200, 302)
        except:
            return False
    
    def get_working_instances(self) -> List[str]:
        working = []
        print(" Testing Nitter instances...")
        
        for instance in self.nitter_instances:
            if self.test_nitter_instance(instance):
                working.append(instance)
                print(f"  {instance}")
            else:
                print(f"  {instance}")
        
        return working
    
    def generate_search_urls(self) -> List[Tuple[str, str, str]]:
        queries = getattr(self, 'base_queries', [])
        
        if not queries:
            raise ValueError("No queries configured. Please provide a match configuration with queries.")
        
        urls = []
        working_instances = self.get_working_instances()
        
        if not working_instances:
            print(" Aucune instance Nitter fonctionnelle!")
            return urls
        
        instance = working_instances[0]
        
        for query, label in queries:
            encoded_query = urllib.parse.quote(query)
            url = f"{instance}/search?f=tweets&q={encoded_query}"
            urls.append((url, label, query))
            for page in range(2, 6):
                url_with_page = f"{instance}/search?f=tweets&q={encoded_query}&p={page}"
                urls.append((url_with_page, f"{label}_page{page}", query))
        
        return urls
    
    def scrape_nitter_page(self, url: str, label: str, original_query: str) -> List[Dict]:

        tweets = []
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                tweet_selectors = [
                    ('div', 'timeline-item'),
                    ('div', 'tweet-body'),
                    ('div', 'tweet-content')
                ]
                
                for tag, class_name in tweet_selectors:
                    tweet_elements = soup.find_all(tag, class_=class_name)
                    if tweet_elements:
                        break

                if not tweet_elements:
                    status_links = soup.find_all('a', href=re.compile(r'/[^/]+/status/\d+'))
                    tweet_elements = []
                    for a in status_links:
                        parent = a
                        for _ in range(4):
                            parent = parent.parent or parent
                            if parent is None:
                                break
                            p = parent.find('p')
                            if p and len(p.get_text(strip=True)) > 10:
                                tweet_elements.append(parent)
                                break
                        if parent and parent not in tweet_elements and len(parent.get_text(strip=True)) > 20:
                            tweet_elements.append(parent)
                
                for tweet_element in tweet_elements:
                    try:
                        p = tweet_element.find('p')
                        if p:
                            tweet_text = p.get_text(" ", strip=True)
                        else:
                            text_elem = tweet_element.find('div', class_='tweet-content')
                            if text_elem:
                                tweet_text = text_elem.get_text(" ", strip=True)
                            else:
                                tweet_text = tweet_element.get_text(" ", strip=True)
                        
                        if len(tweet_text) < 10:  
                            continue
                        
                        user_elem = tweet_element.find('a', class_='username')
                        username = 'unknown'
                        if user_elem:
                            username = user_elem.get_text(strip=True).replace('@', '')
                        else:
                            a_status = tweet_element.find('a', href=re.compile(r'/[^/]+/status/\d+'))
                            if a_status:
                                m = re.search(r'/([^/]+)/status/\d+', a_status['href'])
                                if m:
                                    username = m.group(1).lstrip('@')
                        
                        tweet_time = ''
                        time_elem = tweet_element.find('a', href=re.compile(r'/[^/]+/status/\d+'))
                        if time_elem and time_elem.get('title'):
                            tweet_time = time_elem.get('title')
                        else:
                            span_elem = tweet_element.find('span', class_=re.compile(r'tweet-date|date|datetime|time'))
                            if span_elem and span_elem.get('title'):
                                tweet_time = span_elem.get('title')
                        
                        event_type = self.detect_event_type(tweet_text)
                        
                        mentioned_teams = self.detect_mentioned_teams(tweet_text)
                        
                        tweet_data = {
                            'text': tweet_text[:500],  
                            'query': original_query,
                            'timestamp': datetime.now().isoformat(),
                            'tweet_time': tweet_time,
                            'event_type': event_type,
                            'url': url,
                            'source': 'Nitter',
                            'user': username,
                            'data_source': 'Twitter/Nitter Scraping',
                            'mentioned_teams': ', '.join(mentioned_teams),
                            'search_label': label
                        }
                        
                        tweets.append(tweet_data)
                        
                    except Exception as e:
                        continue  
                
                time.sleep(random.uniform(1, 3))
                
            else:
                print(f"  HTTP {response.status_code} for {label}")
                
        except requests.exceptions.Timeout:
            print(f"   Timeout for {label}")
        except requests.exceptions.ConnectionError:
            print(f"   Connection error for {label}")
        except Exception as e:
            print(f"   Error scraping {label}: {str(e)[:50]}")
        
        return tweets
    
    def detect_event_type(self, text: str) -> str:
        """Détecte le type d'événement dans le texte"""
        text_lower = text.lower()
        
        for event_type, keywords in self.event_keywords.items():
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    return event_type
        
        return 'other_event'
    
    def detect_mentioned_teams(self, text: str) -> List[str]:
        mentioned = []
        text_lower = text.lower()
        
        for team_key, team_info in self.team_data.items():

            if team_info['name'].lower() in text_lower:
                mentioned.append(team_info['name'])
                continue
            
            for nickname in team_info['nicknames']:
                if nickname.lower() in text_lower:
                    mentioned.append(team_info['name'])
                    break
            
            for hashtag in team_info['hashtags']:
                if hashtag.lower() in text_lower:
                    mentioned.append(team_info['name'])
                    break
            
            for player in team_info['players']:
                if player.lower() in text_lower:
                    mentioned.append(team_info['name'])
                    break
        
        return list(set(mentioned))
    
    def scrape_massive_tweets(self, target_count: int = 1000) -> List[Dict]:

        print(f" Target: {target_count} tweets from Nitter...")
        
        all_tweets = []
        search_urls = self.generate_search_urls()
        
        print(f"Found {len(search_urls)} search URLs to scrape")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            for url, label, query in search_urls:
                future = executor.submit(self.scrape_nitter_page, url, label, query)
                futures.append((future, label))
            
            completed = 0
            for future, label in futures:
                try:
                    tweets = future.result(timeout=15)
                    all_tweets.extend(tweets)
                    completed += 1
                    
                    print(f"  {label}: {len(tweets)} tweets (Total: {len(all_tweets)})")
                    
                    if len(all_tweets) >= target_count:
                        print(f"Target reached: {len(all_tweets)} tweets")
                        break
                    
                except Exception as e:
                    print(f"  Failed: {label} - {str(e)[:50]}")
        

        
        unique_tweets = []
        seen_texts = set()
        
        for tweet in all_tweets:
            text_hash = hash(tweet['text'][:100])  # Hash des 100 premiers caractères
            if text_hash not in seen_texts:
                seen_texts.add(text_hash)
                unique_tweets.append(tweet)
        
        print(f"Final unique tweets: {len(unique_tweets)}")
        
        return unique_tweets[:target_count]  # Retourner exactement target_count tweets
    

    def export_to_csv(self, tweets: List[Dict], filename: str = None):

        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"afcon_twitter_data_{timestamp}.csv"
        
        df = pd.DataFrame(tweets)
        
        columns_order = [
            'text', 'query', 'timestamp', 'event_type', 'url',
            'source', 'user', 'data_source', 'tweet_time',
            'mentioned_teams', 'search_label'
        ]
        
        existing_columns = [col for col in columns_order if col in df.columns]
        
        df = df[existing_columns]
        
        df.to_csv(filename, index=False, encoding='utf-8')
        
        print(f"Saved {len(df)} tweets to: {filename}")
        
        print("\n Sample of collected tweets:")
        print("=" * 80)
        for i, tweet in enumerate(df.head(5).to_dict('records')):
            print(f"\n{i+1}. {tweet['text'][:100]}...")
            print(f"   User: {tweet['user']} | Event: {tweet['event_type']} | Teams: {tweet.get('mentioned_teams', 'N/A')}")
        
        return filename
    

def main():
    """Fonction principale"""
    
    print("=" * 80)
    print(" AFCON 2026 TWITTER MASS SCRAPER")
    print("=" * 80)
    
    # Charger les matchs disponibles depuis config
    matches = load_matches()
    selected_match_cfg = None
    if matches:
        print("\nMatchs disponibles:")
        for i, m in enumerate(matches):
            print(f"  [{i}] {m.get('name','unnamed')}")
        choice = input("Choisissez un numéro de match (entrée=0): ").strip()
        try:
            idx = int(choice) if choice else 0
            selected_match_cfg = matches[idx]
            print(f"→ Match sélectionné: {selected_match_cfg.get('name')}")
        except Exception:
            print("Choix invalide, utilisation du premier match par défaut")
            selected_match_cfg = matches[0]
    else:
        print("Aucune config de match trouvée — utilisation du comportement par défaut")
    
    # Initialiser le scraper avec la config de match sélectionnée (ou None)
    scraper = TwitterAFCONScraper(match_config=selected_match_cfg)
    
    # Déterminer target_count depuis la config du match si présente
    target_count = 1000
    if selected_match_cfg and selected_match_cfg.get('target_count'):
        try:
            target_count = int(selected_match_cfg.get('target_count'))
        except:
            pass
    
    # Collecter les tweets
    print("\n Starting massive tweet collection...")
    start_time = time.time()
    
    try:
        tweets = scraper.scrape_massive_tweets(target_count=target_count)
        csv_file = scraper.export_to_csv(tweets)
        print(f" File: {csv_file}")
        print(f" Total tweets: {len(tweets)}")
        
    except KeyboardInterrupt:
        print("\n\n Stopped by user")
    except Exception as e:
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        import requests
        import pandas as pd
        from bs4 import BeautifulSoup
    except ImportError as e:
        exit(1)
    
    main()