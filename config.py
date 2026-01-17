# Configuration settings
import os
from datetime import datetime

class Config:
    # Sources to scrape
    SOURCES = {
        'bbc': {
            'url': 'https://www.bbc.com/sport/football',
            'type': 'news',
            'selectors': {
                'articles': [
                    'a.ssrcss-1mrs5ns-PromoLink',
                    'a[data-testid="internal-link"]',
                    'article a',
                    'h3 a'
                ],
                'title': '.gs-c-promo-heading__title',
                'summary': ['p', 'div[data-testid="card-description"]', '.gs-c-promo-summary'],
                'timestamp': 'time'
            }
        },
        'goal': {
            'url': 'https://www.goal.com/en/news',
            'type': 'news',
            'selectors': {
                'articles': 'article.article-item',
                'title': '.article-item__title',
                'summary': '.article-item__excerpt',
                'timestamp': 'time'
            }
        },
        'sofascore': {
            'url': 'https://www.sofascore.com/football',
            'type': 'live_data',
            'requires_js': True
        }
    }
    
    # Scraping settings
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Referer': 'https://www.google.com/'
    }
    
    # Rate limiting
    DELAY_BETWEEN_REQUESTS = 2  # seconds
    MAX_RETRIES = 3
    
    # Output
    OUTPUT_DIR = 'data'
    OUTPUT_FORMAT = 'json'  # json, csv, or both
    
    # Logging
    LOG_LEVEL = 'INFO'
    LOG_FILE = f'logs/scraper_{datetime.now().strftime("%Y%m%d")}.log'