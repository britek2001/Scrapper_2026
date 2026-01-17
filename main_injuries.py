#!/usr/bin/env python3
"""
Football Player Injuries Scraper - Main Entry Point
"""
from injury_scraper import InjuryScraper

def main():
    """Main execution function"""
    scraper = InjuryScraper()
    
    print("🏥 Football Player Injuries Scraper")
    print("=" * 50)
    
    # Scrape all sources
    results = scraper.scrape_all_injuries()
    
    # Save results
    scraper.save_to_json(results)
    
    print("\n" + "=" * 50)
    print("✅ SCRAPING COMPLETE")
    print("=" * 50)
    print(f"Total Player Injuries: {results['summary']['total_player_injuries']}")
    print(f"Total News Articles: {results['summary']['total_news_articles']}")
    print(f"Data saved to: injuries_data.json")
    print("=" * 50)

if __name__ == "__main__":
    main()
