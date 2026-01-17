from player_scraper import PlayerScraper
import json

def test_manchester_city():
    """Test Manchester City scraping"""
    print("\n" + "=" * 60)
    print("🔵 TESTING MANCHESTER CITY")
    print("=" * 60)
    scraper = PlayerScraper()
    players = scraper.scrape_manchester_city_players()
    print(f"\n📊 Result: Found {len(players)} players")
    if players:
        print("\n👤 Sample player:")
        print(json.dumps(players[0], indent=2))
        print(f"\n📝 First 5 players:")
        for i, player in enumerate(players[:5], 1):
            print(f"  {i}. {player['name']} - {player.get('position', 'N/A')}")
    else:
        print("⚠️  No players found - check debug_mancity.html for details")
    print("=" * 60)
    return len(players) > 0

def test_bayern_munich():
    """Test Bayern Munich scraping"""
    print("\n" + "=" * 60)
    print("🔴 TESTING BAYERN MUNICH")
    print("=" * 60)
    scraper = PlayerScraper()
    players = scraper.scrape_bayern_munich_players()
    print(f"\n📊 Result: Found {len(players)} players")
    if players:
        print("\n👤 Sample player:")
        print(json.dumps(players[0], indent=2))
        print(f"\n📝 First 5 players:")
        for i, player in enumerate(players[:5], 1):
            print(f"  {i}. {player['name']} - {player.get('position', 'N/A')}")
    else:
        print("⚠️  No players found - check debug_bayern.html for details")
    print("=" * 60)
    return len(players) > 0

def test_generic_scraper():
    """Test generic URL scraper"""
    print("\n" + "=" * 60)
    print("⚽ TESTING GENERIC SCRAPER")
    print("=" * 60)
    scraper = PlayerScraper()
    # Test avec une URL alternative
    players = scraper.scrape_url(
        "https://www.mancity.com/players/mens",
        "Manchester City (Generic)"
    )
    print(f"\n📊 Result: Found {len(players)} players")
    if players:
        print(f"\n📝 First 5 players:")
        for i, player in enumerate(players[:5], 1):
            print(f"  {i}. {player['name']}")
    print("=" * 60)
    return len(players) > 0

def test_full_scrape():
    """Test full scraping of all clubs"""
    print("\n" + "=" * 60)
    print("🏆 TESTING FULL SCRAPE")
    print("=" * 60)
    scraper = PlayerScraper()
    results = scraper.scrape_all_clubs()
    
    print(f"\n📊 RESULTS:")
    print(f"  Total clubs: {results['total_clubs']}")
    print(f"  Total players: {results['total_players']}")
    print(f"\n📋 Breakdown:")
    for club, count in results['summary'].items():
        status = "✅" if count > 0 else "⚠️ "
        print(f"  {status} {club}: {count} players")
    
    print("=" * 60)
    return results['total_players'] > 0

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("🧪 TESTING PLAYER SCRAPER")
    print("=" * 60)
    print("This will test all scraping functions...\n")
    
    tests = [
        ("Manchester City", test_manchester_city),
        ("Bayern Munich", test_bayern_munich),
        ("Generic Scraper", test_generic_scraper),
        ("Full Scrape", test_full_scrape)
    ]
    
    results = {}
    for name, test_func in tests:
        try:
            print(f"\n▶️  Running test: {name}")
            success = test_func()
            results[name] = "✅ PASS" if success else "⚠️  WARN (no data)"
        except Exception as e:
            results[name] = f"❌ FAIL: {str(e)}"
            print(f"\n❌ Error in {name}: {e}")
    
    print("\n" + "=" * 60)
    print("📋 TEST SUMMARY")
    print("=" * 60)
    for test_name, result in results.items():
        print(f"  {result} - {test_name}")
    print("=" * 60 + "\n")