import requests
import json
import time
import re
import csv
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
import logging
import traceback
from config_loader import load_event_config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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


class RedditScraper:
    BASE_URL = "https://www.reddit.com"
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json'
    }
    
    def __init__(self, delay: float = 2.0):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.last_request_time = 0
        
    def _rate_limit(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, params: dict = None) -> Optional[dict]:
        self._rate_limit()
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 429:
                logger.warning("Rate limited! Waiting 60 seconds...")
                time.sleep(60)
                return self._make_request(url, params)
            
            response.raise_for_status()
            
            try:
                return response.json()
            except json.JSONDecodeError:
                text = response.text
                if text.startswith(')]}\''):
                    text = text[4:]
                return json.loads(text)
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None
    
    def _parse_comment_tree(self, comment_data: dict, depth: int = 0) -> List[RedditComment]:
        comments = []
        
        if not isinstance(comment_data, dict):
            return comments
            
        kind = comment_data.get('kind')
        data = comment_data.get('data', {})
        
        if kind == 't1':
            comment = RedditComment(
                comment_id=data.get('id'),
                author=data.get('author', '[deleted]'),
                body=data.get('body', ''),
                score=data.get('score', 0),
                created_utc=data.get('created_utc', 0),
                parent_id=data.get('parent_id', ''),
                permalink=data.get('permalink', ''),
                is_submitter=data.get('is_submitter', False),
                depth=depth
            )
            comments.append(comment)
            
            replies = data.get('replies')
            if replies and isinstance(replies, dict):
                children = replies.get('data', {}).get('children', [])
                for child in children:
                    comments.extend(self._parse_comment_tree(child, depth + 1))
                    
        elif kind == 'more':
            pass
            
        return comments
    
    def get_post(self, post_id: str, subreddit: str, fetch_comments: bool = True) -> Optional[RedditPost]:
        url = f"{self.BASE_URL}/r/{subreddit}/comments/{post_id}/.json"
        data = self._make_request(url)
        
        if not data or len(data) < 1:
            return None
        
        try:
            post_listing = data[0]['data']['children'][0]['data']
            
            post = RedditPost(
                post_id=post_listing['id'],
                title=post_listing['title'],
                author=post_listing.get('author', '[deleted]'),
                selftext=post_listing.get('selftext', ''),
                url=post_listing.get('url', ''),
                permalink=f"https://reddit.com{post_listing.get('permalink', '')}",
                score=post_listing.get('score', 0),
                num_comments=post_listing.get('num_comments', 0),
                created_utc=post_listing.get('created_utc', 0),
                subreddit=post_listing.get('subreddit', ''),
                flair=post_listing.get('link_flair_text')
            )
            
            if fetch_comments and len(data) > 1:
                comments_listing = data[1]['data']['children']
                for comment in comments_listing:
                    post.comments.extend(self._parse_comment_tree(comment))
                    
            return post
            
        except (KeyError, IndexError) as e:
            logger.error(f"Error parsing post data: {e}")
            return None
    
    def get_subreddit_posts(self, subreddit: str, sort: str = 'hot', 
                           limit: int = 25, time_filter: str = 'all', after: str = None) -> tuple[List[RedditPost], Optional[str]]:
        """
        Fetch posts from a subreddit with pagination support.
        Returns: (list of posts, 'after' token for next page)
        """
        url = f"{self.BASE_URL}/r/{subreddit}/{sort}/.json"
        params = {'limit': min(limit, 100)}
        
        if sort == 'top':
            params['t'] = time_filter
        
        if after:
            params['after'] = after
            
        data = self._make_request(url, params)
        
        if not data:
            return [], None
        
        posts = []
        next_after = None
        
        try:
            children = data['data']['children']
            for child in children:
                post_data = child['data']
                post = RedditPost(
                    post_id=post_data['id'],
                    title=post_data['title'],
                    author=post_data.get('author', '[deleted]'),
                    selftext=post_data.get('selftext', ''),
                    url=post_data.get('url', ''),
                    permalink=f"https://reddit.com{post_data.get('permalink', '')}",
                    score=post_data.get('score', 0),
                    num_comments=post_data.get('num_comments', 0),
                    created_utc=post_data.get('created_utc', 0),
                    subreddit=post_data.get('subreddit', ''),
                    flair=post_data.get('link_flair_text')
                )
                posts.append(post)
            
            # Get next page token
            next_after = data['data'].get('after')
                
        except KeyError as e:
            logger.error(f"Error parsing listing: {e}")
            
        return posts, next_after
    
    def search_posts(self, query: str, subreddit: Optional[str] = None, 
                     sort: str = 'relevance', time_filter: str = 'all', 
                     limit: int = 100, after: str = None) -> tuple[List[RedditPost], Optional[str]]:
        """
        Search for posts with pagination support.
        Returns: (list of posts, 'after' token for next page)
        """
        url = f"{self.BASE_URL}/search.json"
        params = {
            'q': query,
            'sort': sort,
            't': time_filter,
            'limit': min(limit, 100),
            'restrict_sr': 'on' if subreddit else 'off'
        }
        
        if subreddit:
            url = f"{self.BASE_URL}/r/{subreddit}/search.json"
        
        if after:
            params['after'] = after
            
        data = self._make_request(url, params)
        
        if not data:
            return [], None
            
        posts = []
        next_after = None
        
        try:
            children = data['data']['children']
            for child in children:
                if child['kind'] == 't3':
                    post_data = child['data']
                    post = RedditPost(
                        post_id=post_data['id'],
                        title=post_data['title'],
                        author=post_data.get('author', '[deleted]'),
                        selftext=post_data.get('selftext', ''),
                        url=post_data.get('url', ''),
                        permalink=f"https://reddit.com{post_data.get('permalink', '')}",
                        score=post_data.get('score', 0),
                        num_comments=post_data.get('num_comments', 0),
                        created_utc=post_data.get('created_utc', 0),
                        subreddit=post_data.get('subreddit', ''),
                        flair=post_data.get('link_flair_text')
                    )
                    posts.append(post)
            
            next_after = data['data'].get('after')
                    
        except KeyError:
            pass
            
        return posts, next_after
    
    def search_posts_by_date_range(self, query: str, subreddit: Optional[str] = None,
                                   sort: str = 'new', time_filter: str = 'all',
                                   start_date: Optional[datetime] = None,
                                   end_date: Optional[datetime] = None,
                                   limit: int = 100) -> List[RedditPost]:
        """
        Search for posts within a specific date range.
        Note: Reddit's public API doesn't support date range filtering directly.
        This function works around that limitation.
        """
        all_posts = []
        current_after = None
        page_count = 0
        
        # Convert dates to timestamps if provided
        start_timestamp = int(start_date.timestamp()) if start_date else None
        end_timestamp = int(end_date.timestamp()) if end_date else None
        
        logger.info(f"Searching posts from {start_date} to {end_date} for query: '{query}'")
        
        while True:
            page_count += 1
            logger.info(f"Fetching page {page_count}...")
            
            posts, next_after = self.search_posts(
                query=query,
                subreddit=subreddit,
                sort=sort,
                time_filter=time_filter,
                limit=limit,
                after=current_after
            )
            
            if not posts:
                logger.info("No more posts found.")
                break
            
            # Filter posts by date range
            filtered_posts = []
            for post in posts:
                post_timestamp = post.created_utc
                
                # Check if post is within date range
                within_range = True
                if start_timestamp and post_timestamp < start_timestamp:
                    within_range = False
                if end_timestamp and post_timestamp > end_timestamp:
                    within_range = False
                
                if within_range:
                    filtered_posts.append(post)
            
            logger.info(f"Found {len(posts)} posts, {len(filtered_posts)} within date range")
            
            all_posts.extend(filtered_posts)
            
            # If we found posts outside our date range, we might need to stop
            if start_timestamp and len(filtered_posts) == 0:
                # Check if all posts are before our start date
                oldest_post = min(posts, key=lambda x: x.created_utc)
                if oldest_post.created_utc < start_timestamp:
                    logger.info(f"Reached posts older than start date ({start_date}), stopping search.")
                    break
            
            if not next_after:
                logger.info("No more pages available.")
                break
            
            current_after = next_after
            
            # Rate limiting
            time.sleep(2)
            
            # Safety break to avoid infinite loops
            if page_count >= 50:  # Maximum 50 pages
                logger.warning("Reached maximum page limit (50 pages)")
                break
        
        return all_posts
    
    def fetch_comments_for_posts(self, posts: List[RedditPost]) -> List[RedditPost]:
        for i, post in enumerate(posts):
            logger.info(f"Fetching comments for post {i+1}/{len(posts)}: {post.post_id}")
            detailed = self.get_post(post.post_id, post.subreddit, fetch_comments=True)
            if detailed:
                posts[i] = detailed
            
            # Rate limiting between comment fetches
            if i < len(posts) - 1:
                time.sleep(1)
        
        return posts


class DataExporter:
    @staticmethod
    def to_json(posts: List[RedditPost], filename: str, mode: str = 'w'):
        """Export to JSON with append support"""
        data = [p.to_dict() for p in posts]
        
        if mode == 'a' and os.path.exists(filename):
            # Read existing data and append
            with open(filename, 'r', encoding='utf-8') as f:
                existing = json.load(f)
            data = existing + data
            
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(posts)} posts to {filename}")
    
    @staticmethod
    def to_csv_posts(posts: List[RedditPost], filename: str, mode: str = 'w'):
        """Export posts to CSV with append support"""
        if not posts:
            return
        
        file_exists = os.path.exists(filename)
        write_header = mode == 'w' or not file_exists
            
        with open(filename, mode, newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(['post_id', 'title', 'author', 'score', 'num_comments', 
                               'created_utc', 'created_date', 'subreddit', 'flair', 'url', 'permalink', 'selftext'])
            
            for post in posts:
                created_date = datetime.fromtimestamp(post.created_utc).strftime('%Y-%m-%d %H:%M:%S')
                writer.writerow([
                    post.post_id, post.title, post.author, post.score,
                    post.num_comments, post.created_utc, created_date, post.subreddit,
                    post.flair, post.url, post.permalink, post.selftext
                ])
        logger.info(f"Saved posts to {filename}")
    
    @staticmethod
    def to_csv_comments(posts: List[RedditPost], filename: str, mode: str = 'w'):
        """Export all comments to CSV with append support"""
        comments = []
        for post in posts:
            for comment in post.comments:
                row = comment.to_dict()
                row['post_id'] = post.post_id
                row['post_title'] = post.title
                comments.append(row)
        
        if not comments:
            return
        
        file_exists = os.path.exists(filename)
        write_header = mode == 'w' or not file_exists
        
        with open(filename, mode, newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(['post_id', 'post_title', 'comment_id', 'author', 'score',
                               'created_utc', 'created_date', 'parent_id', 'depth', 'body'])
            
            for c in comments:
                created_date = datetime.fromtimestamp(c['created_utc']).strftime('%Y-%m-%d %H:%M:%S')
                writer.writerow([
                    c['post_id'], c['post_title'], c['comment_id'], c['author'],
                    c['score'], c['created_utc'], created_date, c['parent_id'], c['depth'], c['body']
                ])
        logger.info(f"Saved {len(comments)} comments to {filename}")


class EventScraper:

    def __init__(self, scraper: RedditScraper, output_dir: str = "reddit_event_data"):
        self.scraper = scraper
        self.output_dir = output_dir
        self.exporter = DataExporter()
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
    def scrape_event_discussions(self, event_date_str: str = "18/01/2026", 
                                 days_before: int = 3, days_after: int = 2,
                                 subreddit: str = "soccer",
                                 event_keywords: List[str] = None,
                                 fetch_comments: bool = True):
        """
        Scrape discussions around a specific event date
        
        Args:
            event_date_str: Date of the event in DD/MM/YYYY format
            days_before: Days to scrape before the event
            days_after: Days to scrape after the event
            subreddit: Subreddit to search in
            event_keywords: Keywords related to the event
            fetch_comments: Whether to fetch comments for posts
        """
        
        # Parse event date
        event_date = datetime.strptime(event_date_str, "%d/%m/%Y")
        start_date = event_date - timedelta(days=days_before)
        end_date = event_date + timedelta(days=days_after)
        
        # Default keywords if none provided
        if event_keywords is None:
            event_keywords = [
                "match", "game", "vs", "highlights", "result",
                "goal", "win", "lose", "draw", "review"
            ]
        
        logger.info("=" * 60)
        logger.info(f"EVENT SCRAPING CONFIGURATION")
        logger.info("=" * 60)
        logger.info(f"Event Date: {event_date.strftime('%d/%m/%Y')}")
        logger.info(f"Scraping Period: {start_date.strftime('%d/%m/%Y')} to {end_date.strftime('%d/%m/%Y')}")
        logger.info(f"Subreddit: r/{subreddit}")
        logger.info(f"Keywords: {', '.join(event_keywords)}")
        logger.info(f"Fetch Comments: {fetch_comments}")
        logger.info("=" * 60)
        
        all_posts = []
        
        # Search for each keyword
        for keyword in event_keywords:
            logger.info(f"\nSearching for keyword: '{keyword}'")
            
            try:
                posts = self.scraper.search_posts_by_date_range(
                    query=keyword,
                    subreddit=subreddit,
                    sort='new',  # Sort by new to get chronological order
                    start_date=start_date,
                    end_date=end_date,
                    limit=100
                )
                
                logger.info(f"Found {len(posts)} posts for keyword '{keyword}'")
                
                if posts:
                    # Remove duplicates (posts might appear in multiple keyword searches)
                    existing_ids = {p.post_id for p in all_posts}
                    new_posts = [p for p in posts if p.post_id not in existing_ids]
                    all_posts.extend(new_posts)
                    
                    logger.info(f"Added {len(new_posts)} new posts from keyword '{keyword}'")
                
                # Rate limiting between keyword searches
                time.sleep(3)
                
            except Exception as e:
                logger.error(f"Error searching for keyword '{keyword}': {e}")
                continue
        
        # Log summary before processing
        logger.info(f"\nTotal unique posts found: {len(all_posts)}")
        
        if not all_posts:
            logger.warning("No posts found for the specified criteria.")
            return None
        
        # Sort posts by date
        all_posts.sort(key=lambda x: x.created_utc)
        
        # Fetch comments if requested
        if fetch_comments and len(all_posts) > 0:
            logger.info(f"\nFetching comments for {len(all_posts)} posts...")
            all_posts = self.scraper.fetch_comments_for_posts(all_posts)
        
        # Generate output filename based on event date
        event_date_formatted = event_date.strftime("%Y%m%d")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save data
        json_filename = os.path.join(self.output_dir, f"event_{event_date_formatted}_{timestamp}.json")
        csv_posts_filename = os.path.join(self.output_dir, f"event_posts_{event_date_formatted}_{timestamp}.csv")
        csv_comments_filename = os.path.join(self.output_dir, f"event_comments_{event_date_formatted}_{timestamp}.csv")
        
        self.exporter.to_json(all_posts, json_filename)
        self.exporter.to_csv_posts(all_posts, csv_posts_filename)
        self.exporter.to_csv_comments(all_posts, csv_comments_filename)
        
        # Generate statistics
        total_comments = sum(len(post.comments) for post in all_posts)
        
        # Analyze date distribution
        from collections import Counter
        date_distribution = Counter()
        for post in all_posts:
            post_date = datetime.fromtimestamp(post.created_utc).strftime('%Y-%m-%d')
            date_distribution[post_date] += 1
        
        logger.info("\n" + "=" * 60)
        logger.info("SCRAPING COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info(f"Total Posts: {len(all_posts)}")
        logger.info(f"Total Comments: {total_comments}")
        logger.info(f"\nDate Distribution:")
        for date, count in sorted(date_distribution.items()):
            logger.info(f"  {date}: {count} posts")
        
        logger.info(f"\nOutput Files:")
        logger.info(f"  JSON: {json_filename}")
        logger.info(f"  CSV Posts: {csv_posts_filename}")
        logger.info(f"  CSV Comments: {csv_comments_filename}")
        
        # Save metadata
        metadata = {
            "event_date": event_date_str,
            "scraping_period": {
                "start": start_date.strftime('%Y-%m-%d'),
                "end": end_date.strftime('%Y-%m-%d')
            },
            "subreddit": subreddit,
            "keywords": event_keywords,
            "total_posts": len(all_posts),
            "total_comments": total_comments,
            "date_distribution": dict(date_distribution),
            "output_files": {
                "json": json_filename,
                "csv_posts": csv_posts_filename,
                "csv_comments": csv_comments_filename
            },
            "completed_at": datetime.now().isoformat()
        }
        
        metadata_filename = os.path.join(self.output_dir, f"metadata_{event_date_formatted}_{timestamp}.json")
        with open(metadata_filename, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        logger.info(f"  Metadata: {metadata_filename}")
        
        return metadata


def main():
    """
    Main function to run the event scraper
    """
    # Charger config externe si présente
    cfg = load_event_config() or {}

    # Configuration for your sports event (from config.json with fallback)
    EVENT_DATE = cfg.get("event_date", "18/01/2026")
    DAYS_BEFORE = int(cfg.get("days_before", 3))
    DAYS_AFTER = int(cfg.get("days_after", 2))
    SUBREDDIT = cfg.get("subreddit", "soccer")
    EVENT_KEYWORDS = cfg.get("keywords", [
        "match", "game", "vs", "highlights", "result",
        "goal", "win", "lose", "draw", "review",
        "analysis", "discussion", "thread", "post-match"
    ])
    DELAY = float(cfg.get("delay", 2.0))
    OUTPUT_DIR = cfg.get("output_dir", f"reddit_data_{EVENT_DATE.replace('/', '')}")

    # Initialize scraper with configured delay
    scraper = RedditScraper(delay=DELAY)

    # Initialize event scraper with configured output directory
    event_scraper = EventScraper(
        scraper=scraper,
        output_dir=OUTPUT_DIR
    )
     
     # Run the scraping
    try:
         metadata = event_scraper.scrape_event_discussions(
             event_date_str=EVENT_DATE,
             days_before=DAYS_BEFORE,
             days_after=DAYS_AFTER,
             subreddit=SUBREDDIT,
             event_keywords=EVENT_KEYWORDS,
             fetch_comments=True
         )
         
         if metadata:
             logger.info(f"\n Scraping completed successfully!")
             logger.info(f" Found {metadata['total_posts']} posts and {metadata['total_comments']} comments")
         else:
             logger.warning(" No data was scraped. Try adjusting your keywords or date range.")
     
    except KeyboardInterrupt:
         logger.info("\n Scraping interrupted by user")
    except Exception as e:
         logger.error(f"\n Error during scraping: {e}")
         traceback.print_exc()
 
 # You can keep the old function if needed, but it's not for your event date
def scrape_soccer_10000_pages():
     """
     Original function - kept for compatibility
     """
     logger.warning("This function is for general scraping, not for specific event dates.")
     logger.info("Use the EventScraper class for event-specific scraping.")


if __name__ == "__main__":
    # Run the event scraper for your sports event
    main()