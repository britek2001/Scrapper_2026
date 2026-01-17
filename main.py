#!/usr/bin/env python3
"""
Football News Scraper - Main Entry Point
"""
from scraper import FootballScraper

def main():
    """Main function to run the scraper"""
    print("⚽ Football News Scraper")
    print("=" * 50)
    
    scraper = FootballScraper()
    
    # Run the scraper
    results = scraper.run()
    
    # Print summary
    print("\n" + "="*50)
    print("📊 SCRAPING SUMMARY")
    print("="*50)
    print(f"✅ Total articles scraped: {results['total']}")
    print(f"💾 Saved to: {results['filename']}")
    
    # Group by source
    if results['total'] > 0:
        sources = {}
        for article in results['articles']:
            source = article['source']
            sources[source] = sources.get(source, 0) + 1
        
        print("\n📈 By Source:")
        for source, count in sources.items():
            print(f"   • {source}: {count} articles")
    
    # Print first 5 articles
    print("\n📰 LATEST HEADLINES:")
    print("-" * 50)
    
    if results['total'] == 0:
        print("❌ No articles found. Please check:")
        print("   1. Your internet connection")
        print("   2. The debug_bbc.html file for HTML structure")
        print("   3. If websites are blocking the scraper")
    else:
        for i, article in enumerate(results['articles'][:5], 1):
            print(f"\n{i}. [{article['source']}] {article['title']}")
            if article['summary']:
                print(f"   📝 {article['summary'][:100]}...")
            print(f"   🔗 {article['url']}")
    
    print("\n" + "="*50)
    print("🎯 Scraping complete!")

if __name__ == "__main__":
    main()