#!/usr/bin/env python3
# Bing URL Scraper – Bing France, multiple queries

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

import time
import csv
from datetime import datetime
import urllib.parse
#

# ---------- DRIVER SETUP ----------
def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")

    # 🇫🇷 Force Bing France
    options.add_argument("--lang=fr-FR")
    options.add_argument("accept-language=fr-FR,fr;q=0.9,en;q=0.8")

    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
    )

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.implicitly_wait(5)
    return driver


# ---------- BING SCRAPER FOR SINGLE QUERY ----------
def scrape_bing_for_query(query, category, max_pages=2):
    """Scrape Bing for a single query across multiple pages"""
    driver = setup_driver()
    query_results = []
    seen_urls = set()
    
    try:
        q = urllib.parse.quote(query)
        
        for page in range(max_pages):
            start = page * 50
            url = (
                f"https://www.bing.com/search"
                f"?q={q}"
                f"&count=50"
                f"&first={start}"
                f"&setlang=fr"
                f"&mkt=fr-FR"
                f"&cc=FR"
            )

            driver.delete_all_cookies()
            driver.get(url)
            time.sleep(2)

            # Try multiple selectors for Bing
            links = driver.find_elements(By.CSS_SELECTOR, "li.b_algo h2 a")
            # Alternative selector if primary doesn't work
            if not links:
                links = driver.find_elements(By.CSS_SELECTOR, "h2 a")
            
            for a in links:
                href = a.get_attribute("href")
                if href and href.startswith("http") and href not in seen_urls:
                    seen_urls.add(href)
                    query_results.append({
                        "query": query,
                        "category": category,
                        "url": href
                    })
            
            print(f"  → Query: '{query}' | Page {page + 1}: Found {len(links)} links, {len(query_results)} unique URLs")
            time.sleep(1)
    
    except Exception as e:
        print(f"  → Error for query '{query}': {str(e)}")
    
    finally:
        driver.quit()
    
    return query_results


# ---------- BING SCRAPER FOR ALL QUERIES ----------
def scrape_bing_urls(queries, max_pages=2):
    """Scrape Bing for all queries"""
    all_results = []
    
    print(f"Starting scrape of {len(queries)} queries...")
    print("=" * 60)
    
    for i, (query, category) in enumerate(queries, 1):
        print(f"\n[{i}/{len(queries)}] Processing: '{query}' (Category: {category})")
        
        query_results = scrape_bing_for_query(query, category, max_pages)
        
        print(f"  ✓ Collected {len(query_results)} URLs for this query")
        all_results.extend(query_results)
        
        # Small delay between queries to avoid rate limiting
        if i < len(queries):
            time.sleep(1)
    
    print("\n" + "=" * 60)
    print(f"Total unique URLs collected: {len(all_results)}")
    return all_results


# ---------- SAVE ----------
def save_results(data):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    txt = f"bing_urls_fr_{ts}.txt"
    csvf = f"bing_urls_fr_{ts}.csv"

    # Save as text file (URLs only)
    with open(txt, "w", encoding="utf-8") as f:
        for d in data:
            f.write(d["url"] + "\n")

    # Save as CSV with full details
    with open(csvf, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ID", "Query", "Category", "URL"])
        for i, d in enumerate(data, 1):
            writer.writerow([i, d["query"], d["category"], d["url"]])

    return txt, csvf


# ---------- MAIN ----------
def main():
    base_queries = [
        # General/Standard Queries
        ("AFCON 2025", "general"),
        ("AFCON 2026", "general_2026"),
        ("CAF 2025", "caf"),
        ("Africa Cup of Nations 2026", "general_full"),
        ("CAN 2026", "can_french"),
        
        # Country Specific
        ("Morocco AFCON 2026", "morocco"),
        ("Atlas Lions AFCON", "morocco"),
        ("Walid Regragui AFCON", "morocco_coach"),
        ("Morocco AFCON host", "morocco_host"),
        ("Rabat stadium AFCON final", "morocco_stadium"),
        
        ("Senegal AFCON 2026", "senegal"),
        ("Lions of Teranga AFCON", "senegal"),
        ("Pape Thiaw AFCON", "senegal_coach"),
        ("Senegal AFCON champions", "senegal_champs"),
        
        # FINAL MATCH - MAIN EVENT
        ("Morocco Senegal final AFCON", "final"),
        ("AFCON final 2026", "final"),
        ("Morocco vs Senegal AFCON", "final_match"),
        ("AFCON final controversy", "final_controversy"),
        ("AFCON final chaos", "final_chaos"),
        ("AFCON final walk-off", "final_walkoff"),
        ("AFCON final protest", "final_protest"),
        ("AFCON final scandal", "final_scandal"),
        
        # PLAYER FOCUS - KEY INDIVIDUALS
        ("Sadio Mané AFCON", "player_senegal"),
        ("Achraf Hakimi AFCON", "player_morocco"),
        ("Brahim Díaz AFCON", "player_morocco"),
        ("Kalidou Koulibaly AFCON", "player_senegal"),
        ("Yassine Bounou AFCON", "player_morocco"),
        ("Édouard Mendy AFCON", "player_senegal"),
        ("Nicolas Jackson AFCON", "player_senegal"),
        ("Noussair Mazraoui AFCON", "player_morocco"),
        ("Pape Gueye AFCON", "player_senegal"),
        ("Ismaël Saibari AFCON", "player_morocco"),
        ("Amine Adli AFCON", "player_morocco"),
        ("Lamine Camara AFCON", "player_senegal"),
        ("Amara Diouf AFCON", "player_senegal"),
        
        # 🚨 POLEMICAL/DRAMATIC EVENTS (NUEVO/EXPANDIDO) 🚨
        
        # 1. BRUTAL PENALTY DRAMA
        ("Brahim Díaz penalty miss AFCON final", "event_penalty_miss"),
        ("Díaz panenka fail AFCON", "event_panenka_fail"),
        ("Brahim Díaz penalty saved Mendy", "event_penalty_save"),
        ("Last minute penalty AFCON final", "event_lastmin_pen"),
        ("VAR penalty AFCON final Morocco", "event_var_penalty"),
        ("AFCON final penalty controversy", "event_pen_controversy"),
        ("Díaz misses crucial penalty", "event_crucial_miss"),
        
        # 2. HAKIMI INJURY & SUSPENSION DRAMA
        ("Achraf Hakimi injury AFCON final", "event_hakimi_injury"),
        ("Hakimi injured AFCON", "event_hakimi_hurt"),
        ("Hakimi suspension AFCON", "event_hakimi_suspend"),
        ("Hakimi red card AFCON", "event_hakimi_red"),
        ("Hakimi banned AFCON", "event_hakimi_ban"),
        ("Hakimi World Cup suspension", "event_hakimi_wc"),
        ("PSG Hakimi AFCON injury", "event_hakimi_psg"),
        
        # 3. SENEGAL WALK-OFF PROTEST
        ("Senegal walk off protest AFCON final", "event_walkoff"),
        ("Senegal players protest AFCON", "event_protest"),
        ("Senegal boycott AFCON final", "event_boycott"),
        ("Senegal leaves pitch AFCON", "event_leaves_pitch"),
        ("Referee protest AFCON", "event_ref_protest"),
        ("Game stopped AFCON final", "event_game_stopped"),
        ("15 minute delay AFCON final", "event_15min_delay"),
        
        # 4. COACH DRAMA & SANCTIONS
        ("Pape Thiaw suspended AFCON", "event_coach_suspend"),
        ("Senegal coach fined AFCON", "event_coach_fined"),
        ("Regragui complaint AFCON", "event_coach_complaint"),
        ("Morocco protest rejected CAF", "event_protest_rejected"),
        ("CAF disciplinary board AFCON", "event_caf_discipline"),
        
        # 5. CONTROVERSIAL GOALS/SAVES
        ("Pape Gueye goal AFCON final", "event_goal"),
        ("Mendy penalty save AFCON final", "event_save"),
        ("Édouard Mendy hero AFCON", "event_mendy_hero"),
        ("Controversial goal AFCON final", "event_goal_controversy"),
        ("Offside goal AFCON final", "event_offside"),
        ("VAR disallowed goal AFCON", "event_var_disallow"),
        
        # 6. FAN & STADIUM CONTROVERSY
        ("Fan invasion AFCON final", "event_fan_invasion"),
        ("Rabat stadium chaos", "event_stadium_chaos"),
        ("Morocco fans protest AFCON", "event_fan_protest"),
        ("Objects thrown pitch AFCON", "event_objects_thrown"),
        ("Security breach AFCON final", "event_security_breach"),
        ("Prince Moulay Abdellah stadium incidents", "event_stadium_incidents"),
        
        # 7. REFEREEING CONTROVERSIES
        ("Referee mistake AFCON final", "event_ref_mistake"),
        ("VAR controversy AFCON final", "event_var_controversy"),
        ("CAF referee scandal AFCON", "event_ref_scandal"),
        ("Penalty decision controversy AFCON", "event_pen_decision"),
        ("Added time controversy AFCON", "event_added_time"),
        
        # 8. FINANCIAL & ORGANIZATIONAL DRAMA
        ("CAF fines AFCON final", "event_caf_fines"),
        ("Million dollar fine AFCON", "event_million_fine"),
        ("Prize money controversy AFCON", "event_prize_money"),
        ("AFCON corruption allegations", "event_corruption"),
        ("Hosting costs Morocco AFCON", "event_hosting_costs"),
        
        # 9. PLAYER CONFLICTS & FIGHTS
        ("Player fight AFCON final", "event_player_fight"),
        ("Morocco Senegal brawl", "event_brawl"),
        ("Koulibaly confrontation AFCON", "event_koulibaly_fight"),
        ("Jackson red card AFCON", "event_jackson_red"),
        ("Pushing shoving AFCON final", "event_pushing"),
        
        # 10. POST-MATCH FALLOUT
        ("Senegal celebrations controversy", "event_celebration_controversy"),
        ("Morocco players cry AFCON final", "event_morocco_cry"),
        ("Díaz emotional AFCON miss", "event_diaz_emotional"),
        ("Hakimi angry AFCON final", "event_hakimi_angry"),
        ("Regragui resigns AFCON", "event_regragui_resign"),
        
        # 11. MEDIA & SOCIAL MEDIA DRAMA
        ("AFCON final social media outrage", "event_social_media"),
        ("Twitter AFCON controversy", "event_twitter"),
        ("African media AFCON scandal", "event_media_scandal"),
        ("International press AFCON chaos", "event_intl_press"),
        ("CNN AFCON chaotic final", "event_cnn_coverage"),
        ("Sky Sports AFCON walk-off", "event_skysports"),
        
        # 12. POLITICAL & DIPLOMATIC ANGLE
        ("Morocco Senegal diplomatic tension", "event_diplomatic"),
        ("Government reaction AFCON", "event_gov_reaction"),
        ("King Mohammed VI AFCON", "event_king_reaction"),
        ("Senegal president AFCON final", "event_president"),
        ("African Union AFCON controversy", "event_au"),
        
        # CLUB vs COUNTRY CONFLICTS
        ("Real Madrid Brahim Díaz AFCON", "club_real_madrid"),
        ("Chelsea AFCON Senegal", "club_chelsea"),
        ("PSG Hakimi AFCON injury", "club_psg"),
        ("Bayern Munich AFCON", "club_bayern"),
        ("Al Hilal AFCON", "club_alhilal"),
        ("Manchester City AFCON players", "club_mancity"),
        ("Liverpool Mané AFCON", "club_liverpool"),
        ("Barcelona AFCON players", "club_barcelona"),
        
        # FRENCH LANGUAGE SPECIFIC (for Francophone sites)
        ("CAN 2026 finale chaos", "french_final_chaos"),
        ("Sénégal marche CAN finale", "french_walkoff"),
        ("Pénalty raté Brahim Díaz", "french_penalty_miss"),
        ("Hakimi blessé CAN", "french_hakimi_injured"),
        ("Controverse finale CAN", "french_controversy"),
        ("Protestation joueurs Sénégal", "french_protest"),
        ("CAF sanctions finale CAN", "french_sanctions"),
        
        # ARABIC/FRENCH FOR MOROCCAN SITES
        ("المغرب السنغال نهائي كان 2026", "arabic_final"),
        ("ركلات الترجيح كان 2026", "arabic_penalties"),
        ("إصابة أخرف الحكيمي", "arabic_hakimi_injury"),
        ("احتجاج السنغال", "arabic_protest"),
        ("فضيحة نهائي كان", "arabic_scandal"),
        ("الفيار كان", "arabic_var"),
        
        # COMPARISONS & HISTORICAL CONTEXT
        ("Worst AFCON final ever", "historical_worst"),
        ("Most chaotic AFCON final", "historical_chaotic"),
        ("AFCON final compared to", "historical_compare"),
        ("Previous AFCON controversies", "historical_controversies"),
        ("1998 AFCON walk-off", "historical_1998"),
        ("African football shame", "historical_shame"),
        
        # LEGAL & OFFICIAL ACTIONS
        ("CAF investigation AFCON final", "legal_investigation"),
        ("FIFA involvement AFCON", "legal_fifa"),
        ("Appeal Morocco AFCON", "legal_appeal"),
        ("Court case AFCON final", "legal_court"),
        ("Disciplinary hearing AFCON", "legal_hearing"),
        
        # FAN REACTIONS WORLDWIDE
        ("Dakar celebration AFCON", "fan_dakar"),
        ("Casablanca riot AFCON", "fan_casablanca"),
        ("African diaspora reaction AFCON", "fan_diaspora"),
        ("European fans AFCON final", "fan_europe"),
        ("Social media trend AFCON", "fan_trending"),
    ]

    pages = 8  # 2 pages per query
    print(f"Configuration: {len(base_queries)} queries, {pages} pages per query")
    
    data = scrape_bing_urls(base_queries, pages)

    if data:
        txt, csvf = save_results(data)
        print(f"\n✔ FINAL RESULTS")
        print(f"Total unique URLs: {len(data)}")
        print(f"Text file: {txt}")
        print(f"CSV file: {csvf}")
        
        # Show breakdown by query
        print("\nBreakdown by query:")
        query_counts = {}
        for item in data:
            query_counts[item["query"]] = query_counts.get(item["query"], 0) + 1
        
        for query, count in sorted(query_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {query}: {count} URLs")
    else:
        print("❌ No URLs found")


if __name__ == "__main__":
    main()