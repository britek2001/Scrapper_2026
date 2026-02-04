
import json
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict

# Charger les données JSON
with open('achraf-hakimi_398073_complete.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Extraire les données de blessures
injuries_data = data['pages']['verletzungen']

def analyze_injuries():
    """Analyse complète des blessures d'Hakimi"""
    
    injuries_list = injuries_data['injuries_list']
    season_totals = injuries_data['season_totals']
    
    print("="*80)
    print("ANALYSE DES BLESSURES - ACHRAF HAKIMI")
    print("="*80)
    
    # 1. Statistiques générales
    print("\n📊 STATISTIQUES GÉNÉRALES:")
    print(f"Nombre total de blessures: {len(injuries_list)}")
    
    total_days_out = 0
    total_games_missed = 0
    
    # Calculer les totaux globaux
    for season in season_totals.values():
        days_str = season['total_days'].replace(' jours', '').strip()
        total_days_out += int(days_str)
        total_games_missed += int(season['games_missed'])
    
    print(f"Jours d'indisponibilité totaux: {total_days_out} jours")
    print(f"Matchs manqués totaux: {total_games_missed}")
    
    # 2. Blessures par saison
    print("\n📅 BLESSURES PAR SAISON:")
    
    seasons_sorted = sorted(season_totals.items(), key=lambda x: x[0], reverse=True)
    
    for season, stats in seasons_sorted:
        print(f"\n  Saison {season}:")
        print(f"    • Jours d'indisponibilité: {stats['total_days']}")
        print(f"    • Nombre de blessures: {stats['injuries_count']}")
        print(f"    • Matchs manqués: {stats['games_missed']}")
    
    # 3. Détail des blessures
    print("\n🩺 DÉTAIL DES BLESSURES:")
    
    # Grouper par type de blessure
    injury_types = defaultdict(list)
    
    for injury in injuries_list:
        injury_type = injury['injury_type']
        injury_types[injury_type].append(injury)
    
    print(f"\nTypes de blessures différents: {len(injury_types)}")
    
    for injury_type, injuries in injury_types.items():
        total_days = sum(int(i['days_out'].replace(' jours', '').strip()) for i in injuries)
        avg_days = total_days // len(injuries)
        print(f"\n  {injury_type}:")
        print(f"    • Nombre d'occurrences: {len(injuries)}")
        print(f"    • Total jours d'indisponibilité: {total_days} jours")
        print(f"    • Moyenne par blessure: {avg_days} jours")
    
    # 4. Blessures récentes (saison 25/26)
    print("\n⚠️ BLESSURE RÉCENTE - SAISON 25/26:")
    
    recent_injury = None
    for injury in injuries_list:
        if injury['season'] == '25/26':
            recent_injury = injury
            break
    
    if recent_injury:
        print(f"  Type: {recent_injury['injury_type']}")
        print(f"  Période: du {recent_injury['from_date']} au {recent_injury['until_date']}")
        print(f"  Durée: {recent_injury['days_out']}")
        print(f"  Matchs manqués: {recent_injury['games_missed']}")
        
        # Analyser l'impact sur l'AFCON
        injury_end = datetime(2025, 12, 27)  # 27 déc 2025
        afcon_start = datetime(2026, 1, 10)  # Début estimé AFCON
        afcon_final = datetime(2026, 1, 18)  # Finale
        
        days_between = (afcon_start - injury_end).days
        weeks_between = days_between // 7
        
        print(f"\n  📍 IMPACT SUR L'AFCON 2026:")
        print(f"    • Fin de blessure: {injury_end.strftime('%d/%m/%Y')}")
        print(f"    • Début AFCON estimé: {afcon_start.strftime('%d/%m/%Y')}")
        print(f"    • Jours de récupération avant AFCON: {days_between} jours ({weeks_between} semaines)")
        print(f"    • Finale AFCON: {afcon_final.strftime('%d/%m/%Y')}")
        
        if days_between > 30:
            print(f"    ✅ TEMPS DE RÉCUPÉRATION SUFFISANT")
            print(f"       Hakimi aura environ {weeks_between} semaines pour retrouver la forme")
        elif days_between > 14:
            print(f"    ⚠️ TEMPS DE RÉCUPÉRATION LIMITÉ")
            print(f"       Seulement {weeks_between} semaines avant le tournoi")
        else:
            print(f"    ❌ TEMPS DE RÉCUPÉRATION INSUFFISANT")
            print(f"       Risque de rater le début de l'AFCON")
    
    # 5. Tendances historiques
    print("\n📈 TENDANCES HISTORIQUES:")
    
    # Fréquence des blessures
    seasons_with_injuries = len(season_totals)
    total_seasons = 10  # Depuis 2016/17 environ
    injury_frequency = (seasons_with_injuries / total_seasons) * 100
    
    print(f"  • Saisons avec blessures: {seasons_with_injuries}/{total_seasons} ({injury_frequency:.1f}%)")
    
    # Saisons critiques
    critical_seasons = []
    for season, stats in season_totals.items():
        days = int(stats['total_days'].replace(' jours', '').strip())
        if days > 50:
            critical_seasons.append((season, days))
    
    if critical_seasons:
        print(f"  • Saisons critiques (>50 jours):")
        for season, days in critical_seasons:
            print(f"      - {season}: {days} jours")
    
    # 6. Analyse par zone corporelle
    print("\n🏥 ANALYSE PAR ZONE CORPORELLE:")
    
    body_parts = {
        'cheville': ['cheville', 'Entorse à la cheville', 'Problèmes à la cheville'],
        'ischio': ['ischio', 'Blessure à l\'ischio'],
        'pied': ['métatarse', 'Fracture du métatarse'],
        'virus': ['Coronavirus', 'Quarantaine']
    }
    
    for part, keywords in body_parts.items():
        part_injuries = []
        total_days_part = 0
        
        for injury in injuries_list:
            if any(keyword in injury['injury_type'] for keyword in keywords):
                part_injuries.append(injury)
                days = int(injury['days_out'].replace(' jours', '').strip())
                total_days_part += days
        
        if part_injuries:
            avg_days = total_days_part // len(part_injuries)
            print(f"  • {part.upper()}:")
            print(f"      - Nombre de blessures: {len(part_injuries)}")
            print(f"      - Total jours: {total_days_part}")
            print(f"      - Moyenne: {avg_days} jours")
    
    # 7. Recommandations pour l'AFCON 2026
    print("\n💡 RECOMMANDATIONS POUR L'AFCON 2026:")
    
    if recent_injury and recent_injury['season'] == '25/26':
        print("  1. PRÉVENTION RENFORCÉE:")
        print("     • Surveillance accrue de la cheville")
        print("     • Travail proprioceptif quotidien")
        print("     • Adaptation des charges d'entraînement")
        
        print("\n  2. PROGRAMME DE RÉCUPÉRATION:")
        print("     • Phase 1 (jusqu'à fin déc 2025): Récupération complète")
        print("     • Phase 2 (janv 2026): Réathlétisation progressive")
        print("     • Phase 3 (AFCON): Gestion minutieuse des minutes")
        
        print("\n  3. STRATÉGIE DE JEU:")
        print("     • Rotation possible en phase de groupes")
        print("     • Éviter les matchs consécutifs si possible")
        print("     • Communication constante avec le staff médical")
    
    # 8. Score de risque pour l'AFCON
    print("\n⚠️ SCORE DE RISQUE AFCON 2026:")
    
    risk_factors = {
        'blessure_récente': 30 if recent_injury and recent_injury['season'] == '25/26' else 0,
        'durée_blessure': 20 if recent_injury and int(recent_injury['days_out'].replace(' jours', '')) > 30 else 10,
        'historique_cheville': 25 if any('cheville' in i['injury_type'].lower() for i in injuries_list[-3:]) else 5,
        'matchs_manqués': 15 if recent_injury and int(recent_injury['games_missed']) > 10 else 5,
        'tps_récupération': 10 if days_between > 30 else 20
    }
    
    total_risk = sum(risk_factors.values())
    
    print(f"  Facteurs de risque:")
    for factor, score in risk_factors.items():
        print(f"    • {factor}: {score}/25")
    
    print(f"\n  Score total: {total_risk}/100")
    
    if total_risk < 30:
        print(f"  🟢 RISQUE FAIBLE - Disponibilité quasi-certaine")
    elif total_risk < 60:
        print(f"  🟡 RISQUE MODÉRÉ - Surveillance nécessaire")
    else:
        print(f"  🔴 RISQUE ÉLEVÉ - Plan de contingence requis")
    
    # 9. Timeline des blessures
    print("\n📅 TIMELINE DES BLESSURES:")
    
    # Créer une timeline depuis 2016
    timeline_years = range(2016, 2027)
    
    for year in timeline_years:
        season_start = f"{year}/{(year+1)%100:02d}"
        season_end = f"{year-1}/{year%100:02d}" if year > 2016 else ""
        
        injuries_this_year = []
        for injury in injuries_list:
            season = injury['season']
            if season.startswith(str(year%100)) or (season_end and season.startswith(str((year-1)%100))):
                injuries_this_year.append(injury)
        
        if injuries_this_year:
            total_days = sum(int(i['days_out'].replace(' jours', '').strip()) for i in injuries_this_year)
            print(f"  {year}: {len(injuries_this_year)} blessures ({total_days} jours)")
        else:
            print(f"  {year}: Aucune blessure majeure")
    
    # 10. Impact sur les performances
    print("\n⚽ IMPACT SUR LES PERFORMANCES:")
    
    # Analyser les données de performance autour des blessures
    performance_data = data['pages']['leistungsdatendetails']['table_1']['rows']
    
    # Regrouper par saison
    seasons_performance = defaultdict(list)
    for row in performance_data:
        if row['Saison']:
            seasons_performance[row['Saison']].append(row)
    
    # Comparer saisons avec/sans blessures
    print("  Comparaison saisons avec blessures:")
    
    for season in ['25/26', '22/23', '21/22', '20/21', '18/19']:
        if season in season_totals:
            injury_days = int(season_totals[season]['total_days'].replace(' jours', ''))
            
            # Trouver les stats pour cette saison
            stats_this_season = []
            for row in performance_data:
                if row['Saison'] == season:
                    stats_this_season.append(row)
            
            if stats_this_season:
                # Calculer minutes totales
                total_minutes = 0
                for stat in stats_this_season:
                    if stat['col_9'] and stat['col_9'] != '-':
                        minutes_str = stat['col_9'].replace('\'', '').replace(' ', '')
                        if minutes_str.isdigit():
                            total_minutes += int(minutes_str)
                
                if injury_days > 0:
                    availability = max(0, 100 - (injury_days / 365 * 100))
                    print(f"    • Saison {season}: {injury_days} jours blessé")
                    print(f"      Disponibilité: {availability:.1f}%")
                    print(f"      Minutes jouées: {total_minutes}")
    return injuries_list, season_totals


def save_analysis_to_files(injuries_list, season_totals, posts):
    """Sauvegarde l'analyse dans différents formats"""
    
    # 1. CSV des blessures
    injuries_df = pd.DataFrame(injuries_list)
    injuries_df.to_csv('hakimi_injuries_analysis.csv', index=False, encoding='utf-8-sig')
    
    # 2. CSV des posts générés
    posts_df = pd.DataFrame(posts)
    posts_df.to_csv('hakimi_injury_posts.csv', index=False, encoding='utf-8-sig')
    
    # 3. Rapport texte
    with open('hakimi_injury_report.txt', 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("RAPPORT COMPLET - BLESSURES ACHRAF HAKIMI\n")
        f.write("="*80 + "\n\n")
        
        f.write("📊 STATISTIQUES GÉNÉRALES:\n")
        f.write(f"Nombre total de blessures: {len(injuries_list)}\n")
        
        total_days = sum(int(season['total_days'].replace(' jours', '').strip()) 
                        for season in season_totals.values())
        f.write(f"Jours d'indisponibilité totaux: {total_days} jours\n\n")
        
        f.write("🩺 BLESSURE LA PLUS RÉCENTE (25/26):\n")
        recent = next((inj for inj in injuries_list if inj['season'] == '25/26'), None)
        if recent:
            f.write(f"Type: {recent['injury_type']}\n")
            f.write(f"Période: {recent['from_date']} - {recent['until_date']}\n")
            f.write(f"Durée: {recent['days_out']}\n")
            f.write(f"Matchs manqués: {recent['games_missed']}\n\n")
        
        f.write("⚠️ RECOMMANDATIONS POUR L'AFCON 2026:\n")
        f.write("1. Surveillance renforcée de la cheville\n")
        f.write("2. Programme de récupération adapté\n")
        f.write("3. Gestion minutieuse des minutes de jeu\n")
    
    # 4. JSON complet
    analysis_data = {
        'summary': {
            'total_injuries': len(injuries_list),
            'total_days_out': total_days,
            'recent_injury': recent,
            'afcon_risk_assessment': {
                'risk_level': 'MODERATE' if total_days > 100 else 'LOW',
                'recommendations': [
                    'Extended warm-up and cool-down routines',
                    'Regular physiotherapy sessions',
                    'Limited back-to-back matches',
                    'Close monitoring during training'
                ]
            }
        },
        'injuries_by_season': season_totals,
        'sample_posts': posts[:10]  # Premier 10 posts comme échantillon
    }
    
    with open('hakimi_injury_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(analysis_data, f, indent=2, ensure_ascii=False)
    
    print("\n💾 FICHIERS SAUVEGARDÉS:")
    print("  • hakimi_injuries_analysis.csv - Données brutes des blessures")
    print("  • hakimi_injury_posts.csv - Posts Facebook générés")
    print("  • hakimi_injury_report.txt - Rapport d'analyse")
    print("  • hakimi_injury_analysis.json - Analyse complète en JSON")

def main():
    """Fonction principale"""
    
    print("🔍 ANALYSE DES BLESSURES D'ACHRAF HAKIMI")
    print("   Préparation AFCON 2026 - Finale Maroc vs Sénégal")
    print("="*80)
    
    # Analyser les blessures
    injuries_list, season_totals = analyze_injuries()
    
    # Générer des posts Facebook
    print("\n" + "="*80)
    print("📱 GÉNÉRATION DE POSTS FACEBOOK")
    print("="*80)
    

if __name__ == "__main__":
    main()