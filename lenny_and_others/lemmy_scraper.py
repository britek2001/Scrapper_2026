#!/usr/bin/env python3

import time
import random
import json
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from collections import defaultdict

print("="*80)
print(" SCRAPER PLATEFORMES ALTERNATIVES REDDIT")
print("="*80)
print("\nPlateformes: Lemmy, Kbin, Saidit, Raddle, Tildes, Squabbles")
print("Avantage: Pas de login, pas de crash Chrome, 100% stable\n")

# Configuration
topic = None
# tenter de lire le sujet depuis le fichier JSON de config (config/matches.json)
try:
    with open('config/matches.json', 'r', encoding='utf-8') as _f:
        _cfg = json.load(_f)
        topic = _cfg.get('topic') or _cfg.get('search_topic') or _cfg.get('Sujet de recherche')
except Exception:
    topic = None

if not topic:
    topic = input(" Sujet de recherche: ").strip()

print(f"\n Recherche: {topic}")
print(" Lancement de Chrome...")

# Lancer Chrome
options = Options()
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

try:
    driver = webdriver.Chrome(options=options)
    print(" Chrome lancé avec succès!\n")
except Exception as e:
    print(f" Erreur: {e}")
    print(" Installez Selenium: pip3 install selenium")
    exit(1)

# Inserted: embed matches.json content and helper to match titles
matches_json = """
{
  "matches": [
    {
      "name": "AFCON général",
      "terms": [
        "afcon",
        "AFCON 2026",
        "CAN 2026",
        "Africa Cup",
        "Coupe d'Afrique"
      ]
    },
    {
      "name": "Morocco vs Senegal (final)",
      "terms": [
        "Morocco vs Senegal",
        "Maroc Sénégal",
        "Morocco Senegal final",
        "Maroc contre Sénégal",
        "final AFCON 2026"
      ]
    },
    {
      "name": "Joueurs Maroc",
      "terms": [
        "Achraf Hakimi",
        "Hakimi",
        "Hakim Ziyech",
        "Ziyech",
        "Youssef En-Nesyri",
        "En-Nesyri"
      ]
    },
    {
      "name": "Joueurs Sénégal",
      "terms": [
        "Sadio Mane",
        "Mane",
        "Edouard Mendy",
        "Mendy",
        "Kalidou Koulibaly",
        "Koulibaly"
      ]
    },
    {
      "name": "AFCON_Marocco_vs_Senegal_2026",
      "terms": [
        "AFCON",
        "Morocco vs Senegal"
      ]
    }
  ]
}
"""
try:
    matches_config = json.loads(matches_json)
except Exception:
    matches_config = {"matches": []}
_matches = matches_config.get("matches", [])

def match_title(title):
    if not title:
        return []
    t = title.lower()
    matched = set()
    for m in _matches:
        name = m.get("name")
        for term in m.get("terms", []):
            if term and term.lower() in t:
                matched.add(name)
                break
    return list(matched)

all_posts = []

print("\n SCRAPING LEMMY INSTANCES")
print("-" * 50)

lemmy_instances = [
    'https://lemmy.world',
    'https://lemmy.ml',
    'https://lemmy.ca',
    'https://sh.itjust.works'
]

for instance in lemmy_instances:
    try:
        search_url = f"{instance}/search?q={topic.replace(' ', '+')}&type=Posts"
        print(f"{instance}...")
        
        driver.get(search_url)
        time.sleep(random.uniform(3, 5))
        
        # Scroll
        for _ in range(2):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
        
        # Extraction
        posts = driver.find_elements(By.CSS_SELECTOR, "article, div.post, div[class*='post-']")
        count = 0
        
        for post in posts[:15]:
            try:
                title = ""
                try:
                    title_elem = post.find_element(By.CSS_SELECTOR, "h1, h2, h3, a[class*='title']")
                    title = title_elem.text.strip()
                except: pass
                
                if title:
                    all_posts.append({
                        'platform': 'Lemmy',
                        'instance': instance,
                        'title': title,
                        'matches': match_title(title),
                        'scraped_at': datetime.now().isoformat()
                    })
                    count += 1
            except: pass
        
        print(f"  ✓ {count} posts collectés")
        time.sleep(random.uniform(3, 5))
        
    except Exception as e:
        print(f"    Erreur: {str(e)[:50]}")

# ============== KBIN ==============
print("\n SCRAPING KBIN INSTANCES")
print("-" * 50)

kbin_instances = ['https://kbin.social', 'https://fedia.io']

for instance in kbin_instances:
    try:
        search_url = f"{instance}/search?q={topic.replace(' ', '+')}"
        print(f" {instance}...")
        
        driver.get(search_url)
        time.sleep(random.uniform(3, 5))
        
        for _ in range(2):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
        
        posts = driver.find_elements(By.CSS_SELECTOR, "article, div.entry")
        count = 0
        
        for post in posts[:15]:
            try:
                title = ""
                try:
                    title_elem = post.find_element(By.CSS_SELECTOR, "h2, h3")
                    title = title_elem.text.strip()
                except: pass
                
                if title:
                    all_posts.append({
                        'platform': 'Kbin',
                        'instance': instance,
                        'title': title,
                        'matches': match_title(title),
                        'scraped_at': datetime.now().isoformat()
                    })
                    count += 1
            except: pass
        
        print(f"  ✓ {count} posts collectés")
        time.sleep(random.uniform(3, 5))
        
    except Exception as e:
        print(f"  Erreur: {str(e)[:50]}")

# ============== SAIDIT ==============
print("\nSCRAPING SAIDIT.NET")
print("-" * 50)

try:
    search_url = f"https://saidit.net/search?q={topic.replace(' ', '+')}"
    
    driver.get(search_url)
    time.sleep(random.uniform(3, 5))
    
    posts = driver.find_elements(By.CSS_SELECTOR, "div.thing")
    count = 0
    
    for post in posts[:20]:
        try:
            title = ""
            try:
                title_elem = post.find_element(By.CSS_SELECTOR, "a.title")
                title = title_elem.text.strip()
            except: pass
            
            if title:
                all_posts.append({
                    'platform': 'Saidit',
                    'instance': 'saidit.net',
                    'title': title,
                    'matches': match_title(title),
                    'scraped_at': datetime.now().isoformat()
                })
                count += 1
        except: pass
    
    print(f"  ✓ {count} posts collectés")
    time.sleep(random.uniform(3, 5))
    
except Exception as e:
    print(f"  Erreur: {str(e)[:50]}")

print("\n SCRAPING RADDLE.ME")
print("-" * 50)

try:
    search_url = f"https://raddle.me/search?q={topic.replace(' ', '+')}"
    
    driver.get(search_url)
    time.sleep(random.uniform(3, 5))
    
    posts = driver.find_elements(By.CSS_SELECTOR, "article.submission")
    count = 0
    
    for post in posts[:20]:
        try:
            title = ""
            try:
                title_elem = post.find_element(By.CSS_SELECTOR, "h2 a")
                title = title_elem.text.strip()
            except: pass
            
            if title:
                all_posts.append({
                    'platform': 'Raddle',
                    'instance': 'raddle.me',
                    'title': title,
                    'matches': match_title(title),
                    'scraped_at': datetime.now().isoformat()
                })
                count += 1
        except: pass
    
    print(f"  ✓ {count} posts collectés")
    
except Exception as e:
    print(f"    Erreur: {str(e)[:50]}")

# Fermer Chrome
driver.quit()
print("\n Chrome fermé")

print("\n" + "="*80)
print(" SAUVEGARDE DES RÉSULTATS")
print("="*80)

if not all_posts:
    print("  Aucun post collecté")
    exit(0)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# CSV
csv_file = f"reddit_alternatives_simple_{timestamp}.csv"
df = pd.DataFrame(all_posts)
df.to_csv(csv_file, index=False, encoding='utf-8-sig')
print(f" CSV: {csv_file}")

# JSON
json_file = f"reddit_alternatives_simple_{timestamp}.json"
with open(json_file, 'w', encoding='utf-8') as f:
    json.dump(all_posts, f, indent=2, ensure_ascii=False)
print(f" JSON: {json_file}")

# Stats
print("\n" + "="*80)
print(" STATISTIQUES")
print("="*80)
print(f"Total: {len(all_posts)} posts")

stats = defaultdict(int)
for post in all_posts:
    stats[post['platform']] += 1

for platform, count in sorted(stats.items(), key=lambda x: x[1], reverse=True):
    print(f"  {platform}: {count} posts")

print("="*80)
print(" TERMINÉ!")
