#!/usr/bin/env python3
"""Test runner for TransderMarkt_Scraper.

Usage:
  python3 scripts/test_tm_scraper.py --url https://www.transfermarkt.com/kylian-mbappe/profil/spieler/342229

The script runs the scraper with Playwright disabled (fast, headless requests-only) and
saves JSON in the scraper's player data directory.
"""
import os
import sys
import argparse

HERE = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.join(HERE, '..'))
sys.path.insert(0, ROOT)

from interation_scraper_fixed import TransderMarkt_Scraper


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--url', '-u', help='Player Transfermarkt profile URL', required=False)
    p.add_argument('--no-write', action='store_true', help="Don't alter scraper settings")
    args = p.parse_args()

    # Default example if none provided
    players = [args.url] if args.url else [
        'https://www.transfermarkt.com/kylian-mbappe/profil/spieler/342229'
    ]

    # Instantiate scraper: disable Playwright to keep testing simple
    # Enable follow_links for test and restrict to agent/berater patterns
    scraper = TransderMarkt_Scraper(
        use_playwright=False,
        use_selenium=False,
        delay=0.5,
        follow_links=True,
        follow_patterns=['agent-support', 'berater', 'statistik/endendevertraege'],
        max_follow_links=10
    )

    print(f"Running Transfermarkt scraper for {len(players)} player(s)...")
    results = scraper.execution_url_agentent(None, players)

    if results:
        for r in results:
            slug = r.get('slug')
            pid = r.get('player_id')
            filepath = os.path.join(scraper.player_data_directory, f"{slug}_{pid}.json")
            exists = os.path.exists(filepath)
            print(f"Player {slug} ({pid}) saved: {exists} -> {filepath}")
    else:
        print('No results returned (check logs).')


if __name__ == '__main__':
    main()
