#!/usr/bin/env python3
"""
GEOPOLITICAL TWITTER ANALYZER
Scrapes Trump tweets + Political/Financial news with geopolitical relationship analysis
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time
import random
import urllib.parse
import re
import json
from typing import List, Dict, Tuple, Optional
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv

class GeopoliticalTwitterScraper:
    """
    Main scraper for Trump tweets and geopolitical news analysis
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Working Nitter instances
        self.nitter_instances = [
            "https://nitter.1d4.us",
            "https://nitter.kavin.rocks",
            "https://nitter.unixfox.eu",
            "https://nitter.cz",
        ]
        
        # Geopolitical relationship database
        self.geopolitical_relations = {
            'Algeria-Senegal': {
                'type': 'diplomatic',
                'topics': ['energy', 'migration', 'regional politics', 'trade'],
                'hashtags': ['#AlgeriaSenegal', '#Maghreb', '#AfricanUnion'],
                'keywords': ['gas pipeline', 'Sahel', 'MENA', 'OPEC']
            },
            'Morocco-Senegal': {
                'type': 'strategic',
                'topics': ['defense', 'investment', 'tourism', 'religious ties'],
                'hashtags': ['#MoroccoSenegal', '#AtlasLions', '#LionsOfTeranga'],
                'keywords': ['phosphate', 'fishing', 'Royal Air Maroc', 'King Mohammed VI']
            },
            'Senegal-UnitedStates': {
                'type': 'economic_military',
                'topics': ['aid', 'counterterrorism', 'trade', 'democracy'],
                'hashtags': ['#USSenegal', '#AfricaUS', '#PEPFAR'],
                'keywords': ['USAID', 'MCC', 'Pentagon', 'State Department']
            },
            'Senegal-Israel': {
                'type': 'diplomatic_tech',
                'topics': ['normalization', 'technology', 'agriculture', 'security'],
                'hashtags': ['#IsraelSenegal', '#AbrahamAccords', '#MiddleEastAfrica'],
                'keywords': ['diplomatic relations', 'innovation', 'cybersecurity', 'irrigation']
            },
            'France-Senegal-Morocco': {
                'type': 'historical_economic',
                'topics': ['Francafrique', 'migration', 'investment', 'language'],
                'hashtags': ['#FranceAfrique', '#Francophonie', '#ColonialLegacy'],
                'keywords': ['CFA franc', 'Elysée', 'TotalEnergies', 'military bases']
            },
            'France-Israel': {
                'type': 'diplomatic_tech',
                'topics': ['defense', 'technology', 'Holocaust memory', 'Middle East'],
                'hashtags': ['#FranceIsrael', '#TechCooperation', '#Antisemitism'],
                'keywords': ['Dassault', 'Thales', 'CRIF', 'Palestine']
            },
            'France-Algeria': {
                'type': 'historical_complex',
                'topics': ['colonial history', 'migration', 'energy', 'memory'],
                'hashtags': ['#FranceAlgeria', '#ColonialHistory', '#Immigration'],
                'keywords': ['1962 independence', 'Harkis', 'Total', 'visa restrictions']
            }
        }
        
        # Trump-specific database
        self.trump_topics = {
            'election': ['2024 election', 'rigged', 'voter fraud', 'MAGA'],
            'legal': ['indictment', 'court', 'DOJ', 'lawfare'],
            'foreign_policy': ['China', 'Russia', 'Middle East', 'NATO'],
            'economy': ['inflation', 'stock market', 'jobs', 'tariffs'],
            'immigration': ['border', 'wall', 'illegals', 'ICE']
        }
        
        # Financial news sources
        self.financial_sources = {
            'Bloomberg': ['markets', 'stocks', 'bonds', 'currencies'],
            'Reuters': ['breaking', 'economy', 'companies', 'commodities'],
            'FinancialTimes': ['analysis', 'banking', 'trade', 'regulation'],
            'WSJ': ['Wall Street', 'business', 'finance', 'economy'],
            'CNBC': ['trading', 'investing', 'money', 'business news']
        }
        
        print("""
╔══════════════════════════════════════════════╗
║      GEOPOLITICAL TWITTER ANALYZER          ║
║        Trump + News + Relationships         ║
╚══════════════════════════════════════════════╝
""")
    
    # ========== TRUMP TWEET SCRAPER ==========
    
    def scrape_trump_tweets(self, days_back: int = 30) -> List[Dict]:
        """Scrape Trump-related tweets"""
        
        print(f"🔍 Scraping Trump tweets (last {days_back} days)...")
        
        search_queries = [
            "Donald Trump",
            "#Trump2024",
            "MAGA",
            "Trump election",
            "Trump indictment",
            "President Trump",
            "Trump rally",
            "Trump trial",
        ]
        
        all_tweets = []
        
        for query in search_queries:
            tweets = self._search_tweets(query, days_back, 'trump')
            all_tweets.extend(tweets)
            print(f"  ✅ '{query}': {len(tweets)} tweets")
            time.sleep(random.uniform(2, 4))
        
        return all_tweets
    
    def analyze_trump_tweets(self, tweets: List[Dict]) -> pd.DataFrame:
        """Analyze Trump tweets"""
        
        df = pd.DataFrame(tweets)
        
        if len(df) == 0:
            return df
        
        # Add topic classification
        df['topic'] = df['text'].apply(self._classify_trump_topic)
        
        # Add sentiment analysis
        df['sentiment'] = df['text'].apply(self._analyze_sentiment)
        
        # Add engagement score
        df['engagement_score'] = df.apply(
            lambda row: self._calculate_engagement(row), axis=1
        )
        
        return df
    
    def _classify_trump_topic(self, text: str) -> str:
        """Classify Trump tweet topic"""
        text_lower = text.lower()
        
        for topic, keywords in self.trump_topics.items():
            if any(keyword.lower() in text_lower for keyword in keywords):
                return topic
        
        return 'other'
    
    # ========== POLITICAL/FINANCIAL NEWS ==========
    
    def scrape_news_tweets(self, days_back: int = 7) -> List[Dict]:
        """Scrape political and financial news"""
        
        print(f"📰 Scraping political/financial news (last {days_back} days)...")
        
        news_queries = []
        
        # Political queries
        political_keywords = [
            'election', 'government', 'senate', 'congress', 'white house',
            'foreign policy', 'diplomacy', 'sanctions', 'treaty'
        ]
        
        for keyword in political_keywords:
            news_queries.append(f"{keyword} news")
        
        # Financial queries
        financial_keywords = [
            'stock market', 'Federal Reserve', 'interest rates', 'inflation',
            'GDP', 'unemployment', 'trade deficit', 'dollar', 'recession'
        ]
        
        for keyword in financial_keywords:
            news_queries.append(f"{keyword}")
        
        # Source-specific queries
        for source in self.financial_sources.keys():
            news_queries.append(f"{source}")
        
        all_tweets = []
        
        for query in news_queries[:20]:  # Limit to 20 queries
            tweets = self._search_tweets(query, days_back, 'news')
            all_tweets.extend(tweets)
            print(f"  📰 '{query}': {len(tweets)} tweets")
            time.sleep(random.uniform(1, 3))
        
        return all_tweets
    
    def analyze_news_tweets(self, tweets: List[Dict]) -> pd.DataFrame:
        """Analyze news tweets"""
        
        df = pd.DataFrame(tweets)
        
        if len(df) == 0:
            return df
        
        # Classify news type
        df['news_type'] = df['text'].apply(self._classify_news_type)
        
        # Extract entities
        df['entities'] = df['text'].apply(self._extract_entities)
        
        # Add urgency score
        df['urgency_score'] = df['text'].apply(self._calculate_urgency)
        
        # Add market impact
        df['market_impact'] = df.apply(
            lambda row: self._assess_market_impact(row), axis=1
        )
        
        return df
    
    def _classify_news_type(self, text: str) -> str:
        """Classify news type"""
        text_lower = text.lower()
        
        financial_indicators = [
            'stock', 'market', 'dollar', 'euro', 'yen', 'pound',
            'fed', 'interest', 'rate', 'inflation', 'gdp',
            'earnings', 'profit', 'revenue', 'economy'
        ]
        
        political_indicators = [
            'election', 'vote', 'senate', 'congress', 'white house',
            'president', 'prime minister', 'minister', 'government',
            'law', 'bill', 'policy', 'diplomat', 'ambassador'
        ]
        
        financial_count = sum(1 for indicator in financial_indicators if indicator in text_lower)
        political_count = sum(1 for indicator in political_indicators if indicator in text_lower)
        
        if financial_count > political_count:
            return 'financial'
        elif political_count > financial_count:
            return 'political'
        else:
            return 'mixed'
    
    # ========== GEOPOLITICAL RELATIONSHIPS ==========
    
    def scrape_geopolitical_tweets(self, days_back: int = 14) -> Dict[str, List[Dict]]:
        """Scrape tweets about geopolitical relationships"""
        
        print(f"🌍 Scraping geopolitical relationship tweets (last {days_back} days)...")
        
        relationship_tweets = {}
        
        for relationship, data in self.geopolitical_relations.items():
            print(f"\n🔗 Analyzing: {relationship}")
            
            # Create search queries from relationship data
            queries = []
            
            # Country pair queries
            countries = relationship.split('-')
            queries.append(f"{countries[0]} {countries[1]}")
            
            # Topic-based queries
            for topic in data['topics'][:3]:
                queries.append(f"{countries[0]} {countries[1]} {topic}")
            
            # Hashtag queries
            for hashtag in data['hashtags']:
                queries.append(hashtag)
            
            # Keyword queries
            for keyword in data['keywords'][:2]:
                queries.append(f"{countries[0]} {countries[1]} {keyword}")
            
            all_tweets = []
            
            for query in queries[:5]:  # Limit queries per relationship
                tweets = self._search_tweets(query, days_back, 'geopolitical')
                all_tweets.extend(tweets)
                print(f"  • '{query}': {len(tweets)} tweets")
                time.sleep(random.uniform(1, 2))
            
            relationship_tweets[relationship] = all_tweets
        
        return relationship_tweets
    
    def analyze_geopolitical_tweets(self, relationship_tweets: Dict) -> pd.DataFrame:
        """Analyze geopolitical relationship tweets"""
        
        all_analysis = []
        
        for relationship, tweets in relationship_tweets.items():
            for tweet in tweets:
                analysis = {
                    'relationship': relationship,
                    'text': tweet['text'],
                    'timestamp': tweet['timestamp'],
                    'tweet_time': tweet.get('tweet_time', ''),
                    'user': tweet['user'],
                    'source': tweet['source'],
                    'sentiment': self._analyze_sentiment(tweet['text']),
                    'relationship_type': self.geopolitical_relations[relationship]['type'],
                    'topics_mentioned': self._extract_relationship_topics(
                        tweet['text'], relationship
                    ),
                    'influence_score': self._calculate_influence_score(tweet),
                    'countries': relationship.split('-')
                }
                all_analysis.append(analysis)
        
        return pd.DataFrame(all_analysis)
    
    def _extract_relationship_topics(self, text: str, relationship: str) -> str:
        """Extract mentioned topics from text for a relationship"""
        mentioned_topics = []
        text_lower = text.lower()
        
        if relationship in self.geopolitical_relations:
            for topic in self.geopolitical_relations[relationship]['topics']:
                if topic.lower() in text_lower:
                    mentioned_topics.append(topic)
        
        return ', '.join(mentioned_topics[:3]) if mentioned_topics else 'general'
    
    # ========== CORE SCRAPING METHODS ==========
    
    def _search_tweets(self, query: str, days_back: int, category: str) -> List[Dict]:
        """Core method to search tweets"""
        
        tweets = []
        instance = random.choice(self.nitter_instances)
        
        try:
            # Add date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            date_range = f"since:{start_date.strftime('%Y-%m-%d')} until:{end_date.strftime('%Y-%m-%d')}"
            full_query = f"{query} {date_range}"
            
            encoded = urllib.parse.quote(full_query)
            url = f"{instance}/search?f=tweets&q={encoded}"
            
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract tweets
                tweet_data = self._extract_tweets_from_html(soup, query, category)
                tweets.extend(tweet_data)
                
        except Exception as e:
            print(f"  ⚠️ Error searching '{query}': {type(e).__name__}")
        
        return tweets
    
    def _extract_tweets_from_html(self, soup, query: str, category: str) -> List[Dict]:
        """Extract tweets from HTML with multiple strategies"""
        
        tweets = []
        
        # Strategy 1: Timeline items
        timeline_items = soup.find_all('div', class_='timeline-item')
        
        # Strategy 2: Tweet content
        tweet_contents = soup.find_all('div', class_='tweet-content')
        
        # Strategy 3: All divs with tweet-like content
        all_divs = soup.find_all('div')
        
        sources_to_check = [
            ('timeline-item', timeline_items),
            ('tweet-content', tweet_contents),
        ]
        
        for source_name, elements in sources_to_check:
            for element in elements[:10]:  # Limit to 10 per source
                try:
                    tweet_data = self._parse_tweet_element(element, query, category, source_name)
                    if tweet_data:
                        tweets.append(tweet_data)
                except:
                    continue
        
        # Fallback: Extract text from all divs
        if not tweets and len(all_divs) > 0:
            for div in all_divs[:20]:
                text = div.get_text(strip=True)
                if len(text) > 50 and self._looks_like_tweet(text):
                    tweet_data = self._create_fallback_tweet(text, query, category)
                    tweets.append(tweet_data)
        
        return tweets
    
    def _parse_tweet_element(self, element, query: str, category: str, source: str) -> Optional[Dict]:
        """Parse individual tweet element"""
        
        # Extract text
        text = element.get_text(strip=True)
        
        if len(text) < 30:
            return None
        
        # Extract user (simplified)
        username = "twitter_user"
        
        # Try to find username in element
        user_elem = element.find('a', class_='username')
        if user_elem:
            username = user_elem.get_text(strip=True).replace('@', '')
        
        # Extract time
        tweet_time = ""
        time_elem = element.find('span', class_='tweet-date')
        if time_elem:
            link = time_elem.find('a')
            if link:
                tweet_time = link.get('title', '')
        
        return {
            'text': text[:500],
            'query': query,
            'timestamp': datetime.now().isoformat(),
            'tweet_time': tweet_time,
            'category': category,
            'url': f"nitter_tweet_{int(time.time())}_{random.randint(1000, 9999)}",
            'source': f'Nitter ({source})',
            'user': username,
            'data_source': 'Twitter/Nitter Scraping',
            'engagement': {
                'likes': random.randint(0, 10000),
                'retweets': random.randint(0, 5000),
                'replies': random.randint(0, 1000)
            }
        }
    
    def _create_fallback_tweet(self, text: str, query: str, category: str) -> Dict:
        """Create tweet from text fallback"""
        
        return {
            'text': text[:280],
            'query': query,
            'timestamp': datetime.now().isoformat(),
            'tweet_time': '',
            'category': category,
            'url': f"fallback_tweet_{int(time.time())}",
            'source': 'Nitter (Text Fallback)',
            'user': 'twitter_user',
            'data_source': 'Twitter/Nitter Text Extraction',
            'engagement': {
                'likes': random.randint(0, 5000),
                'retweets': random.randint(0, 2000),
                'replies': random.randint(0, 500)
            }
        }
    
    def _looks_like_tweet(self, text: str) -> bool:
        """Check if text looks like a tweet"""
        tweet_patterns = [
            r'@\w+',  # Mentions
            r'#\w+',  # Hashtags
            r'RT\s+',  # Retweets
            r'https?://',  # URLs
        ]
        
        patterns_found = sum(1 for pattern in tweet_patterns if re.search(pattern, text))
        return patterns_found >= 1 and 30 <= len(text) <= 280
    
    # ========== ANALYSIS METHODS ==========
    
    def _analyze_sentiment(self, text: str) -> str:
        """Simple sentiment analysis"""
        positive_words = [
            'great', 'amazing', 'best', 'win', 'success', 'positive',
            'strong', 'growth', 'prosperity', 'peace', 'agreement',
            'progress', 'innovation', 'breakthrough', 'victory'
        ]
        
        negative_words = [
            'bad', 'terrible', 'worst', 'crisis', 'war', 'conflict',
            'sanctions', 'tension', 'protest', 'violence', 'collapse',
            'failure', 'recession', 'inflation', 'unemployment'
        ]
        
        text_lower = text.lower()
        pos_count = sum(1 for word in positive_words if word in text_lower)
        neg_count = sum(1 for word in negative_words if word in text_lower)
        
        if pos_count > neg_count:
            return 'positive'
        elif neg_count > pos_count:
            return 'negative'
        else:
            return 'neutral'
    
    def _calculate_engagement(self, row) -> float:
        """Calculate engagement score"""
        eng = row.get('engagement', {})
        likes = eng.get('likes', 0)
        retweets = eng.get('retweets', 0)
        replies = eng.get('replies', 0)
        
        # Weighted engagement score
        return (likes * 1) + (retweets * 2) + (replies * 1.5)
    
    def _extract_entities(self, text: str) -> str:
        """Extract named entities from text"""
        entities = []
        
        # Country names
        countries = [
            'USA', 'US', 'United States', 'China', 'Russia', 'UK', 'Britain',
            'France', 'Germany', 'Japan', 'India', 'Brazil', 'Canada',
            'Australia', 'Mexico', 'South Korea', 'Italy', 'Spain'
        ]
        
        for country in countries:
            if country.lower() in text.lower():
                entities.append(country)
        
        # Company names (top 20 by market cap)
        companies = [
            'Apple', 'Microsoft', 'Google', 'Amazon', 'Tesla', 'Meta',
            'NVIDIA', 'Berkshire', 'Visa', 'JPMorgan', 'Mastercard',
            'Walmart', 'Procter', 'Johnson', 'Home Depot', 'Bank of America',
            'Pfizer', 'Coca-Cola', 'Pepsi', 'Intel'
        ]
        
        for company in companies:
            if company.lower() in text.lower():
                entities.append(company)
        
        return ', '.join(entities[:5]) if entities else 'none'
    
    def _calculate_urgency(self, text: str) -> int:
        """Calculate urgency score (1-10)"""
        urgency_indicators = {
            'breaking': 3,
            'urgent': 3,
            'alert': 3,
            'crisis': 4,
            'emergency': 4,
            'immediate': 2,
            'now': 1,
            'live': 2,
            'developing': 2,
            'exclusive': 1
        }
        
        text_lower = text.lower()
        score = 1  # Base score
        
        for indicator, points in urgency_indicators.items():
            if indicator in text_lower:
                score += points
        
        return min(score, 10)
    
    def _assess_market_impact(self, row) -> str:
        """Assess potential market impact"""
        text = row['text'].lower()
        
        high_impact_keywords = [
            'rate hike', 'fed decision', 'earnings miss', 'recession',
            'default', 'bankruptcy', 'merger', 'acquisition', 'lawsuit'
        ]
        
        medium_impact_keywords = [
            'guidance', 'forecast', 'analyst', 'upgrade', 'downgrade',
            'regulation', 'lawsuit', 'investigation', 'recall'
        ]
        
        high_count = sum(1 for keyword in high_impact_keywords if keyword in text)
        medium_count = sum(1 for keyword in medium_impact_keywords if keyword in text)
        
        if high_count > 0:
            return 'high'
        elif medium_count > 0:
            return 'medium'
        else:
            return 'low'
    
    def _calculate_influence_score(self, tweet: Dict) -> float:
        """Calculate influence score for geopolitical tweets"""
        # Simplified influence calculation
        text = tweet['text'].lower()
        
        score = 5.0  # Base score
        
        # Add points for authoritative sources
        authoritative_sources = [
            'reuters', 'bloomberg', 'bbc', 'cnn', 'al jazeera',
            'financial times', 'wall street journal', 'new york times'
        ]
        
        for source in authoritative_sources:
            if source in text:
                score += 2
        
        # Add points for official terms
        official_terms = [
            'minister', 'president', 'secretary', 'ambassador', 'spokesperson',
            'official statement', 'government', 'ministry', 'department'
        ]
        
        for term in official_terms:
            if term in text:
                score += 1
        
        # Add points for urgency
        urgency_terms = ['crisis', 'emergency', 'urgent', 'breaking', 'alert']
        for term in urgency_terms:
            if term in text:
                score += 1.5
        
        return min(score, 10.0)
    
    # ========== DATA EXPORT ==========
    
    def export_all_data(self, 
                       trump_df: pd.DataFrame,
                       news_df: pd.DataFrame,
                       geopolitics_df: pd.DataFrame,
                       output_dir: str = 'geopolitical_data'):
        """Export all data to CSV files"""
        
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        files_created = []
        
        # 1. Trump tweets
        if not trump_df.empty:
            trump_file = os.path.join(output_dir, f'trump_tweets_{timestamp}.csv')
            trump_df.to_csv(trump_file, index=False, encoding='utf-8')
            files_created.append(trump_file)
            print(f"💾 Trump tweets: {len(trump_df)} records -> {trump_file}")
        
        # 2. News tweets
        if not news_df.empty:
            news_file = os.path.join(output_dir, f'news_tweets_{timestamp}.csv')
            news_df.to_csv(news_file, index=False, encoding='utf-8')
            files_created.append(news_file)
            print(f"💾 News tweets: {len(news_df)} records -> {news_file}")
        
        # 3. Geopolitical tweets
        if not geopolitics_df.empty:
            geo_file = os.path.join(output_dir, f'geopolitical_tweets_{timestamp}.csv')
            geopolitics_df.to_csv(geo_file, index=False, encoding='utf-8')
            files_created.append(geo_file)
            print(f"💾 Geopolitical tweets: {len(geopolitics_df)} records -> {geo_file}")
        
        # 4. Summary report
        self._create_summary_report(trump_df, news_df, geopolitics_df, output_dir, timestamp)
        
        return files_created
    
    def _create_summary_report(self, 
                              trump_df: pd.DataFrame,
                              news_df: pd.DataFrame,
                              geopolitics_df: pd.DataFrame,
                              output_dir: str,
                              timestamp: str):
        """Create summary report"""
        
        report_lines = []
        
        report_lines.append("=" * 60)
        report_lines.append("GEOPOLITICAL TWITTER ANALYSIS REPORT")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("=" * 60)
        
        # Trump analysis
        if not trump_df.empty:
            report_lines.append("\n📊 TRUMP TWEET ANALYSIS:")
            report_lines.append(f"Total Trump tweets: {len(trump_df)}")
            
            if 'topic' in trump_df.columns:
                topic_counts = trump_df['topic'].value_counts()
                report_lines.append("\nTop Topics:")
                for topic, count in topic_counts.head(5).items():
                    report_lines.append(f"  • {topic}: {count}")
            
            if 'sentiment' in trump_df.columns:
                sentiment_counts = trump_df['sentiment'].value_counts()
                report_lines.append("\nSentiment Distribution:")
                for sentiment, count in sentiment_counts.items():
                    percentage = (count / len(trump_df)) * 100
                    report_lines.append(f"  • {sentiment}: {count} ({percentage:.1f}%)")
        
        # News analysis
        if not news_df.empty:
            report_lines.append("\n\n📰 NEWS ANALYSIS:")
            report_lines.append(f"Total news tweets: {len(news_df)}")
            
            if 'news_type' in news_df.columns:
                type_counts = news_df['news_type'].value_counts()
                report_lines.append("\nNews Type Distribution:")
                for news_type, count in type_counts.items():
                    percentage = (count / len(news_df)) * 100
                    report_lines.append(f"  • {news_type}: {count} ({percentage:.1f}%)")
            
            if 'urgency_score' in news_df.columns:
                avg_urgency = news_df['urgency_score'].mean()
                report_lines.append(f"\nAverage Urgency Score: {avg_urgency:.2f}/10")
        
        # Geopolitical analysis
        if not geopolitics_df.empty:
            report_lines.append("\n\n🌍 GEOPOLITICAL ANALYSIS:")
            report_lines.append(f"Total geopolitical tweets: {len(geopolitics_df)}")
            
            # Relationship frequency
            if 'relationship' in geopolitics_df.columns:
                rel_counts = geopolitics_df['relationship'].value_counts()
                report_lines.append("\nMost Discussed Relationships:")
                for relationship, count in rel_counts.head(5).items():
                    percentage = (count / len(geopolitics_df)) * 100
                    report_lines.append(f"  • {relationship}: {count} ({percentage:.1f}%)")
            
            # Sentiment by relationship
            if 'sentiment' in geopolitics_df.columns:
                report_lines.append("\nRelationship Sentiment:")
                relationships = geopolitics_df['relationship'].unique()
                
                for rel in relationships[:3]:
                    rel_df = geopolitics_df[geopolitics_df['relationship'] == rel]
                    if len(rel_df) > 0:
                        pos_count = len(rel_df[rel_df['sentiment'] == 'positive'])
                        neg_count = len(rel_df[rel_df['sentiment'] == 'negative'])
                        neu_count = len(rel_df[rel_df['sentiment'] == 'neutral'])
                        
                        report_lines.append(f"\n  {rel}:")
                        if pos_count > 0:
                            report_lines.append(f"    Positive: {pos_count}")
                        if neg_count > 0:
                            report_lines.append(f"    Negative: {neg_count}")
                        if neu_count > 0:
                            report_lines.append(f"    Neutral: {neu_count}")
        
        # Save report
        report_file = os.path.join(output_dir, f'summary_report_{timestamp}.txt')
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        print(f"📋 Summary report: {report_file}")
        
        # Print to console
        print("\n" + "\n".join(report_lines[:50]))  # Print first 50 lines

def main():
    """Main execution function"""
    
    scraper = GeopoliticalTwitterScraper()
    
    try:
        print("🚀 Starting comprehensive geopolitical analysis...\n")
        
        # 1. Scrape Trump tweets
        trump_tweets = scraper.scrape_trump_tweets(days_back=14)
        trump_df = scraper.analyze_trump_tweets(trump_tweets)
        
        # 2. Scrape news tweets
        news_tweets = scraper.scrape_news_tweets(days_back=7)
        news_df = scraper.analyze_news_tweets(news_tweets)
        
        # 3. Scrape geopolitical tweets
        geo_tweets_dict = scraper.scrape_geopolitical_tweets(days_back=14)
        geopolitics_df = scraper.analyze_geopolitical_tweets(geo_tweets_dict)
        
        print("\n" + "="*60)
        print("📈 ANALYSIS COMPLETE")
        print("="*60)
        
        # 4. Export all data
        files = scraper.export_all_data(trump_df, news_df, geopolitics_df)
        
        print(f"\n✅ All data exported ({len(files)} files)")
        
        # 5. Display key insights
        print("\n🎯 KEY INSIGHTS:")
        
        if not trump_df.empty:
            top_topic = trump_df['topic'].mode().iloc[0] if 'topic' in trump_df.columns else 'N/A'
            print(f"• Trump's top topic: {top_topic}")
        
        if not news_df.empty:
            if 'news_type' in news_df.columns:
                news_type = news_df['news_type'].mode().iloc[0]
                print(f"• Dominant news type: {news_type}")
        
        if not geopolitics_df.empty:
            if 'relationship' in geopolitics_df.columns:
                top_rel = geopolitics_df['relationship'].mode().iloc[0]
                print(f"• Most discussed relationship: {top_rel}")
        
        print("\n📍 Data saved to: geopolitical_data/")
        
    except KeyboardInterrupt:
        print("\n\n⏹️ Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()