import requests
import json
import time
import re
import csv
import os
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
import logging

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
                     limit: int = 25, after: str = None) -> tuple[List[RedditPost], Optional[str]]:
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
    
    def fetch_comments_for_posts(self, posts: List[RedditPost]) -> List[RedditPost]:
        for i, post in enumerate(posts):
            logger.info(f"Fetching comments for post {i+1}/{len(posts)}: {post.post_id}")
            detailed = self.get_post(post.post_id, post.subreddit, fetch_comments=True)
            if detailed:
                posts[i] = detailed
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
                               'created_utc', 'subreddit', 'flair', 'url', 'permalink', 'selftext'])
            
            for post in posts:
                writer.writerow([
                    post.post_id, post.title, post.author, post.score,
                    post.num_comments, post.created_utc, post.subreddit,
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
                               'created_utc', 'parent_id', 'depth', 'body'])
            
            for c in comments:
                writer.writerow([
                    c['post_id'], c['post_title'], c['comment_id'], c['author'],
                    c['score'], c['created_utc'], c['parent_id'], c['depth'], c['body']
                ])
        logger.info(f"Saved {len(comments)} comments to {filename}")


class BatchProcessor:
    """
    Gère le traitement par lots de 1000 pages avec sauvegarde progressive
    """
    
    def __init__(self, scraper: RedditScraper, output_dir: str = "reddit_data"):
        self.scraper = scraper
        self.output_dir = output_dir
        self.exporter = DataExporter()
        
        # Créer le dossier de sortie
        os.makedirs(output_dir, exist_ok=True)
        
        # Fichiers de suivi
        self.progress_file = os.path.join(output_dir, "progress.json")
        self.stats_file = os.path.join(output_dir, "stats.json")
        
    def load_progress(self) -> dict:
        """Charge la progression sauvegardée"""
        if os.path.exists(self.progress_file):
            with open(self.progress_file, 'r') as f:
                return json.load(f)
        return {
            "pages_fetched": 0,
            "posts_collected": 0,
            "comments_collected": 0,
            "last_after": None,
            "current_batch": 1,
            "completed_batches": []
        }
    
    def save_progress(self, progress: dict):
        """Sauvegarde la progression"""
        with open(self.progress_file, 'w') as f:
            json.dump(progress, f, indent=2)
    
    def save_stats(self, stats: dict):
        """Sauvegarde les statistiques"""
        with open(self.stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
    
    def get_batch_filename(self, batch_num: int, data_type: str, ext: str) -> str:
        """Génère le nom de fichier pour un lot"""
        timestamp = datetime.now().strftime("%Y%m%d")
        return os.path.join(
            self.output_dir, 
            f"soccer_batch{batch_num:03d}_{data_type}_{timestamp}.{ext}"
        )
    
    def process_batch(self, posts: List[RedditPost], batch_num: int, fetch_comments: bool = True):
        """
        Traite un lot de posts : récupère les commentaires et sauvegarde
        """
        logger.info(f"🔄 Traitement du lot {batch_num}: {len(posts)} posts")
        
        # Récupérer les commentaires si demandé
        if fetch_comments:
            posts = self.scraper.fetch_comments_for_posts(posts)
        
        # Sauvegarder les fichiers du lot
        json_file = self.get_batch_filename(batch_num, "full", "json")
        csv_posts_file = self.get_batch_filename(batch_num, "posts", "csv")
        csv_comments_file = self.get_batch_filename(batch_num, "comments", "csv")
        
        self.exporter.to_json(posts, json_file)
        self.exporter.to_csv_posts(posts, csv_posts_file)
        self.exporter.to_csv_comments(posts, csv_comments_file)
        
        # Calculer les stats
        total_comments = sum(len(p.comments) for p in posts)
        
        return {
            "posts": len(posts),
            "comments": total_comments,
            "files": {
                "json": json_file,
                "csv_posts": csv_posts_file,
                "csv_comments": csv_comments_file
            }
        }
    
    def merge_all_batches(self, total_batches: int):
        """
        Fusionne tous les lots en fichiers finaux
        """
        logger.info("🔄 Fusion de tous les lots...")
        
        all_posts = []
        
        # Charger tous les JSON des lots
        for batch_num in range(1, total_batches + 1):
            json_file = self.get_batch_filename(batch_num, "full", "json")
            if os.path.exists(json_file):
                with open(json_file, 'r', encoding='utf-8') as f:
                    batch_data = json.load(f)
                    all_posts.extend(batch_data)
        
        # Sauvegarder le fichier final complet
        final_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        final_json = os.path.join(self.output_dir, f"soccer_complete_{final_timestamp}.json")
        final_csv_posts = os.path.join(self.output_dir, f"soccer_complete_posts_{final_timestamp}.csv")
        final_csv_comments = os.path.join(self.output_dir, f"soccer_complete_comments_{final_timestamp}.csv")
        
        with open(final_json, 'w', encoding='utf-8') as f:
            json.dump(all_posts, f, indent=2, ensure_ascii=False)
        
        # Reconstruire les objets pour l'export CSV
        posts_objects = []
        for p_data in all_posts:
            post = RedditPost(
                post_id=p_data['post_id'],
                title=p_data['title'],
                author=p_data['author'],
                selftext=p_data['selftext'],
                url=p_data['url'],
                permalink=p_data['permalink'],
                score=p_data['score'],
                num_comments=p_data['num_comments'],
                created_utc=p_data['created_utc'],
                subreddit=p_data['subreddit'],
                flair=p_data['flair'],
                comments=[]
            )
            # Reconstruire les commentaires
            for c_data in p_data.get('comments', []):
                comment = RedditComment(
                    comment_id=c_data['comment_id'],
                    author=c_data['author'],
                    body=c_data['body'],
                    score=c_data['score'],
                    created_utc=c_data['created_utc'],
                    parent_id=c_data['parent_id'],
                    permalink=c_data['permalink'],
                    is_submitter=c_data['is_submitter'],
                    depth=c_data['depth']
                )
                post.comments.append(comment)
            posts_objects.append(post)
        
        self.exporter.to_csv_posts(posts_objects, final_csv_posts)
        self.exporter.to_csv_comments(posts_objects, final_csv_comments)
        
        return {
            "total_posts": len(all_posts),
            "total_comments": sum(len(p['comments']) for p in all_posts),
            "files": {
                "json": final_json,
                "csv_posts": final_csv_posts,
                "csv_comments": final_csv_comments
            }
        }


def scrape_soccer_10000_pages():
    """
    Scrape 10 000 pages de r/soccer par lots de 1 000
    """
    # Configuration
    TARGET_PAGES = 10000
    BATCH_SIZE = 1000  # Nombre de pages par lot
    POSTS_PER_PAGE = 100  # Maximum Reddit par page
    SUBREDDIT = "soccer"
    SORT = "hot"  # ou 'new', 'top'
    TIME_FILTER = "all"  # pour 'top'
    FETCH_COMMENTS = True  # Mettre False pour plus de rapidité (posts seulement)
    
    # Calculer le nombre de lots
    num_batches = TARGET_PAGES // BATCH_SIZE  # = 10 lots
    posts_per_batch = BATCH_SIZE * POSTS_PER_PAGE  # = 100 000 posts par lot (théorique)
    
    logger.info(f" Démarrage du scraping de r/{SUBREDDIT}")
    logger.info(f" Objectif: {TARGET_PAGES} pages ({num_batches} lots de {BATCH_SIZE} pages)")
    logger.info(f" Récupération des commentaires: {'Oui' if FETCH_COMMENTS else 'Non'}")
    
    # Initialiser le scraper et le processeur de lots
    scraper = RedditScraper(delay=2.0)
    processor = BatchProcessor(scraper, output_dir="soccer_data")
    
    # Charger la progression existante
    progress = processor.load_progress()
    
    if progress["pages_fetched"] > 0:
        logger.info(f"  Reprise du scraping: {progress['pages_fetched']} pages déjà récupérées")
        logger.info(f" Lots complétés: {progress['completed_batches']}")
    
    try:
        current_after = progress["last_after"]
        current_batch = progress["current_batch"]
        
        # Traiter chaque lot
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
            
            # Récupérer les pages pour ce lot
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
                
                # Sauvegarder la progression après chaque page
                progress["last_after"] = current_after
                processor.save_progress(progress)
                
                if not current_after:
                    logger.info("🏁 Plus de pages disponibles (after=None)")
                    break
            
            if batch_posts:
                # Traiter et sauvegarder le lot
                stats = processor.process_batch(batch_posts, current_batch, FETCH_COMMENTS)
                
                # Mettre à jour la progression
                progress["posts_collected"] += stats["posts"]
                progress["comments_collected"] += stats["comments"]
                progress["completed_batches"].append(current_batch)
                progress["current_batch"] = current_batch + 1
                progress["last_after"] = current_after
                processor.save_progress(progress)
                
                logger.info(f" Lot {current_batch} terminé: {stats['posts']} posts, {stats['comments']} commentaires")
            
            current_batch += 1
            
            # Pause entre les lots pour éviter le rate limiting
            if current_batch <= num_batches:
                logger.info("  Pause de 30 secondes entre les lots...")
                time.sleep(30)
        
        # Fusion finale
        logger.info(f"\n{'='*60}")
        logger.info(" SCRAPING TERMINÉ - FUSION DES LOTS")
        logger.info(f"{'='*60}")
        
        final_stats = processor.merge_all_batches(num_batches)
        
        logger.info(f"\nSTATISTIQUES FINALES:")
        logger.info(f"   Posts totaux: {final_stats['total_posts']}")
        logger.info(f"   Commentaires totaux: {final_stats['total_comments']}")
        logger.info(f"   Fichiers finaux:")
        logger.info(f"      JSON: {final_stats['files']['json']}")
        logger.info(f"      CSV Posts: {final_stats['files']['csv_posts']}")
        logger.info(f"      CSV Comments: {final_stats['files']['csv_comments']}")
        
        # Sauvegarder les stats finales
        processor.save_stats({
            "target_pages": TARGET_PAGES,
            "completed_batches": progress["completed_batches"],
            "total_posts": final_stats['total_posts'],
            "total_comments": final_stats['total_comments'],
            "files": final_stats['files'],
            "completed_at": datetime.now().isoformat()
        })
        
    except KeyboardInterrupt:
        logger.info("\n Interruption par l'utilisateur")
        logger.info(f" Progression sauvegardée: {progress['pages_fetched']} pages, lot {current_batch}")
    except Exception as e:
        logger.error(f" Erreur: {e}")
        processor.save_progress(progress)
        raise


if __name__ == "__main__":
    scrape_soccer_10000_pages()