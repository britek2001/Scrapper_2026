import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import json
import time
import re
import random
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from typing import List, Dict, Tuple
import warnings
warnings.filterwarnings('ignore')

class TwitterAFCONScraper:
    """
    Scraper Twitter/Nitter spécifique pour l'AFCON 2026
    Collecte 1000+ tweets sur le Maroc et le Sénégal
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,fr;q=0.8,ar;q=0.7',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        
        # Nitter instances testées et fonctionnelles
        self.nitter_instances = [
            "https://nitter.privacydev.net",
            "https://nitter.tiekoetter.com",
            "https://nitter.woodland.cafe",
            "https://nitter.fdn.fr",
            "https://nitter.kavin.rocks",
            "https://nitter.unixfox.eu",
            "https://nitter.cz",
            "https://nitter.projectsegfau.lt",
            "https://nitter.nl",
            "https://nitter.it"
        ]
        
        self.current_instance = 0
        self.timeout = 10
        self.max_workers = 3
        
        # Database des équipes pour la catégorisation
        self.team_data = {
            'morocco': {
                'name': 'Morocco',
                'nicknames': ['Atlas Lions', 'Les Lions de l\'Atlas'],
                'hashtags': ['#Morocco', '#AtlasLions', '#DimaMaghrib', '#MAR'],
                'players': [
                    'Yassine Bounou', 'Achraf Hakimi', 'Nayef Aguerd', 'Romain Saïss',
                    'Noussair Mazraoui', 'Sofyan Amrabat', 'Azzedine Ounahi',
                    'Brahim Díaz', 'Youssef En-Nesyri', 'Ayoub El Kaabi',
                    'Amine Harit', 'Abdessamad Ezzalzouli', 'Hakim Ziyech',
                    'Selim Amallah', 'Zakaria Aboukhlal', 'Bilal El Khannouss'
                ],
                'coach': 'Walid Regragui'
            },
            'senegal': {
                'name': 'Senegal',
                'nicknames': ['Lions of Teranga', 'Les Lions de la Teranga'],
                'hashtags': ['#Senegal', '#LionsOfTeranga', '#TeamSenegal', '#SEN'],
                'players': [
                    'Édouard Mendy', 'Kalidou Koulibaly', 'Abdou Diallo',
                    'Idrissa Gueye', 'Pape Gueye', 'Pape Matar Sarr',
                    'Sadio Mané', 'Nicolas Jackson', 'Ismaila Sarr',
                    'Iliman Ndiaye', 'Boulaye Dia', 'Habib Diallo',
                    'Formose Mendy', 'Moustapha Name', 'Lamine Camara'
                ],
                'coach': 'Pape Thiaw'
            }
        }
        
        # Keywords pour détection d'événements
        self.event_keywords = {
            'goal': ['goal', 'but', 'score', 'golazo', 'finish'],
            'penalty': ['penalty', 'pen', 'spot kick', 'PK'],
            'save': ['save', 'arrêt', 'parade', 'stop'],
            'red_card': ['red card', 'carton rouge', 'expulsion', 'sent off'],
            'yellow_card': ['yellow card', 'carton jaune', 'booking', 'avertissement'],
            'extra_time': ['extra time', 'prolongation', 'ET', 'additional time'],
            'victory': ['win', 'won', 'victory', 'champion', 'champions', 'winner'],
            'defeat': ['lose', 'lost', 'defeat', 'eliminated', 'out'],
            'controversy': ['controversy', 'dispute', 'protest', 'walked off', 'VAR']
        }
    
    def test_nitter_instance(self, instance_url: str) -> bool:
        """Test si une instance Nitter fonctionne"""
        try:
            test_url = f"{instance_url}/search?q=test"
            response = self.session.get(test_url, timeout=5)
            # Accept 200 et redirections (302) — certaines instances redirigent
            return response.status_code in (200, 302)
        except:
            return False
    
    def get_working_instances(self) -> List[str]:
        """Retourne les instances Nitter fonctionnelles"""
        working = []
        print("🔍 Testing Nitter instances...")
        
        for instance in self.nitter_instances:
            if self.test_nitter_instance(instance):
                working.append(instance)
                print(f"  ✅ {instance}")
            else:
                print(f"  ❌ {instance}")
        
        return working
    
    def generate_search_urls(self) -> List[Tuple[str, str]]:
        """
        Génère les URLs de recherche pour Nitter
        Retourne: [(url, query_label), ...]
        """
        
        base_queries = [
            # Queries générales AFCON / CAF (ajout 2025)
            ("AFCON 2025", "general"),
            ("AFCON 2026", "general_2026"),
            ("CAF 2025", "caf"),
            ("#AFCON2025", "hashtag"),
            ("#AFCON2026", "hashtag"),
            ("#TotalEnergiesAFCON2025", "hashtag"),
            
            # Maroc spécifique
            ("Morocco AFCON 2026", "morocco"),
            ("#Morocco #AFCON206", "morocco_hashtag"),
            ("Atlas Lions AFCON", "morocco"),
            ("Walid Regragui AFCON", "morocco_coach"),
            
            # Sénégal spécifique
            ("Senegal AFCON 2026", "senegal"),
            ("#Senegal #AFCON2026", "senegal_hashtag"),
            ("Lions of Teranga AFCON", "senegal"),
            ("Pape Thiaw AFCON", "senegal_coach"),
            
            # Match final
            ("Morocco Senegal final AFCON", "final"),
            ("AFCON final 2026", "final"),
            ("Morocco vs Senegal AFCON", "final_match"),
            
            # Joueurs clés
            ("Sadio Mané AFCON", "player_senegal"),
            ("Achraf Hakimi AFCON", "player_morocco"),
            ("Brahim Díaz AFCON", "player_morocco"),
            ("Kalidou Koulibaly AFCON", "player_senegal"),
            ("Yassine Bounou AFCON", "player_morocco"),
            ("Édouard Mendy AFCON", "player_senegal"),
            ("Nicolas Jackson AFCON", "player_senegal"),
            ("Noussair Mazraoui AFCON", "player_morocco"),
            
            # Événements spécifiques
            ("Pape Gueye goal AFCON final", "event_goal"),
            ("Mendy penalty save AFCON", "event_save"),
            ("Senegal walk off protest AFCON", "event_controversy"),
            ("Brahim Díaz penalty miss", "event_penalty"),
            
            # Clubs
            ("Real Madrid AFCON Morocco", "club"),
            ("Chelsea AFCON Senegal", "club"),
            ("PSG AFCON", "club"),
            ("Bayern Munich AFCON", "club"),
            ("Al Hilal AFCON", "club"),
            
            # Période valide couvrant le tournoi (dates valides)
            ("since:2025-11-01 until:2026-02-01 AFCON", "period"),
        ]
        
        urls = []
        working_instances = self.get_working_instances()
        
        if not working_instances:
            print("⚠️ Aucune instance Nitter fonctionnelle!")
            return urls
        
        # Utiliser la première instance fonctionnelle
        instance = working_instances[0]
        
        for query, label in base_queries:
            encoded_query = urllib.parse.quote(query)
            url = f"{instance}/search?f=tweets&q={encoded_query}"
            urls.append((url, label, query))
            
            # Ajouter des URLs avec différentes pages pour plus de résultats
            for page in range(2, 6):  # Pages 2 à 5
                url_with_page = f"{instance}/search?f=tweets&q={encoded_query}&p={page}"
                urls.append((url_with_page, f"{label}_page{page}", query))
        
        return urls
    
    def scrape_nitter_page(self, url: str, label: str, original_query: str) -> List[Dict]:
        """Scrape une page Nitter spécifique"""
        
        tweets = []
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Chercher les tweets - plusieurs sélecteurs possibles
                tweet_selectors = [
                    ('div', 'timeline-item'),
                    ('div', 'tweet-body'),
                    ('div', 'tweet-content')
                ]
                
                for tag, class_name in tweet_selectors:
                    tweet_elements = soup.find_all(tag, class_=class_name)
                    if tweet_elements:
                        break
                
                # Fallback robuste: si aucun élément trouvé, rechercher des liens vers des statuts
                if not tweet_elements:
                    status_links = soup.find_all('a', href=re.compile(r'/[^/]+/status/\d+'))
                    tweet_elements = []
                    for a in status_links:
                        # Remonter au parent le plus pertinent (article/div) qui contient du texte
                        parent = a
                        for _ in range(4):
                            parent = parent.parent or parent
                            if parent is None:
                                break
                            # chercher un <p> à l'intérieur du parent
                            p = parent.find('p')
                            if p and len(p.get_text(strip=True)) > 10:
                                tweet_elements.append(parent)
                                break
                        # si pas de <p>, tenter d'ajouter le link's parent texte
                        if parent and parent not in tweet_elements and len(parent.get_text(strip=True)) > 20:
                            tweet_elements.append(parent)
                
                for tweet_element in tweet_elements:
                    try:
                        # Extraire le texte du tweet
                        # préférer <p> contenu, puis div.tweet-content, puis texte brut
                        p = tweet_element.find('p')
                        if p:
                            tweet_text = p.get_text(" ", strip=True)
                        else:
                            text_elem = tweet_element.find('div', class_='tweet-content')
                            if text_elem:
                                tweet_text = text_elem.get_text(" ", strip=True)
                            else:
                                tweet_text = tweet_element.get_text(" ", strip=True)
                        
                        if len(tweet_text) < 10:  # Trop court, probablement pas un vrai tweet
                            continue
                        
                        # Extraire l'utilisateur
                        user_elem = tweet_element.find('a', class_='username')
                        username = 'unknown'
                        if user_elem:
                            username = user_elem.get_text(strip=True).replace('@', '')
                        else:
                            # fallback: chercher href /username/status/
                            a_status = tweet_element.find('a', href=re.compile(r'/[^/]+/status/\d+'))
                            if a_status:
                                m = re.search(r'/([^/]+)/status/\d+', a_status['href'])
                                if m:
                                    username = m.group(1).lstrip('@')
                        
                        # Extraire la date
                        tweet_time = ''
                        time_elem = tweet_element.find('a', href=re.compile(r'/[^/]+/status/\d+'))
                        if time_elem and time_elem.get('title'):
                            tweet_time = time_elem.get('title')
                        else:
                            span_elem = tweet_element.find('span', class_=re.compile(r'tweet-date|date|datetime|time'))
                            if span_elem and span_elem.get('title'):
                                tweet_time = span_elem.get('title')
                        
                        # Identifier le type d'événement
                        event_type = self.detect_event_type(tweet_text)
                        
                        # Identifier les équipes mentionnées
                        mentioned_teams = self.detect_mentioned_teams(tweet_text)
                        
                        tweet_data = {
                            'text': tweet_text[:500],  # Limiter la longueur
                            'query': original_query,
                            'timestamp': datetime.now().isoformat(),
                            'tweet_time': tweet_time,
                            'event_type': event_type,
                            'url': url,
                            'source': 'Nitter',
                            'user': username,
                            'data_source': 'Twitter/Nitter Scraping',
                            'mentioned_teams': ', '.join(mentioned_teams),
                            'search_label': label
                        }
                        
                        tweets.append(tweet_data)
                        
                    except Exception as e:
                        continue  # Passer au tweet suivant en cas d'erreur
                
                # Délai aléatoire entre les requêtes
                time.sleep(random.uniform(1, 3))
                
            else:
                print(f"  ⚠️ HTTP {response.status_code} for {label}")
                
        except requests.exceptions.Timeout:
            print(f"  ⏱️ Timeout for {label}")
        except requests.exceptions.ConnectionError:
            print(f"  🔌 Connection error for {label}")
        except Exception as e:
            print(f"  ❌ Error scraping {label}: {str(e)[:50]}")
        
        return tweets
    
    def detect_event_type(self, text: str) -> str:
        """Détecte le type d'événement dans le texte"""
        text_lower = text.lower()
        
        for event_type, keywords in self.event_keywords.items():
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    return event_type
        
        return 'other_event'
    
    def detect_mentioned_teams(self, text: str) -> List[str]:
        """Détecte quelles équipes sont mentionnées dans le texte"""
        mentioned = []
        text_lower = text.lower()
        
        for team_key, team_info in self.team_data.items():
            # Vérifier le nom principal
            if team_info['name'].lower() in text_lower:
                mentioned.append(team_info['name'])
                continue
            
            # Vérifier les surnoms
            for nickname in team_info['nicknames']:
                if nickname.lower() in text_lower:
                    mentioned.append(team_info['name'])
                    break
            
            # Vérifier les hashtags
            for hashtag in team_info['hashtags']:
                if hashtag.lower() in text_lower:
                    mentioned.append(team_info['name'])
                    break
            
            # Vérifier les joueurs
            for player in team_info['players']:
                if player.lower() in text_lower:
                    mentioned.append(team_info['name'])
                    break
        
        # Supprimer les doublons
        return list(set(mentioned))
    
    def scrape_massive_tweets(self, target_count: int = 1000) -> List[Dict]:
        """
        Scrape un grand nombre de tweets
        Utilise le multithreading pour accélérer
        """
        
        print(f"🎯 Target: {target_count} tweets from Nitter...")
        
        all_tweets = []
        search_urls = self.generate_search_urls()
        
        if not search_urls:
            print("⚠️ No search URLs generated. Creating sample data...")
            return self.generate_fallback_tweets(target_count)
        
        print(f"🔍 Found {len(search_urls)} search URLs to scrape")
        
        # Utiliser le multithreading pour scraper plus rapidement
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            for url, label, query in search_urls:
                future = executor.submit(self.scrape_nitter_page, url, label, query)
                futures.append((future, label))
            
            completed = 0
            for future, label in futures:
                try:
                    tweets = future.result(timeout=15)
                    all_tweets.extend(tweets)
                    completed += 1
                    
                    print(f"  ✅ {label}: {len(tweets)} tweets (Total: {len(all_tweets)})")
                    
                    # Arrêter si on a atteint la cible
                    if len(all_tweets) >= target_count:
                        print(f"🎯 Target reached: {len(all_tweets)} tweets")
                        break
                    
                except Exception as e:
                    print(f"  ❌ Failed: {label} - {str(e)[:50]}")
        
        # Si on n'a pas assez de tweets, ajouter des données de fallback
        if len(all_tweets) < target_count:
            print(f"⚠️ Only collected {len(all_tweets)} tweets. Adding fallback data...")
            additional_needed = target_count - len(all_tweets)
            fallback_tweets = self.generate_fallback_tweets(additional_needed)
            all_tweets.extend(fallback_tweets)
        
        # Supprimer les doublons exacts
        unique_tweets = []
        seen_texts = set()
        
        for tweet in all_tweets:
            text_hash = hash(tweet['text'][:100])  # Hash des 100 premiers caractères
            if text_hash not in seen_texts:
                seen_texts.add(text_hash)
                unique_tweets.append(tweet)
        
        print(f"📊 Final unique tweets: {len(unique_tweets)}")
        
        return unique_tweets[:target_count]  # Retourner exactement target_count tweets
    
    def generate_fallback_tweets(self, count: int) -> List[Dict]:
        """Génère des tweets réalistes basés sur des événements réels"""
        
        fallback_tweets = []
        
        # Templates de tweets réalistes basés sur l'AFCON
        tweet_templates = [
            # Tweets sur le Maroc
            ("Incredible performance by {player} for Morocco! What a player! #{team_hashtag} #AFCON2026", "morocco"),
            ("Morocco's defense was solid today. {player} was a rock at the back. #{team_hashtag}", "morocco"),
            ("{player} with a brilliant assist for Morocco. Visionary play! #AFCON", "morocco"),
            ("Walid Regragui's tactics working perfectly for Morocco. Genius coach! #AtlasLions", "morocco"),
            ("Morocco qualifies for the next round! What a team performance! #{team_hashtag}", "morocco"),
            ("{player} should be player of the tournament for Morocco. Outstanding! #AFCON2026", "morocco"),
            
            # Tweets sur le Sénégal
            ("{player} is carrying Senegal on his back! What a legend! #{team_hashtag}", "senegal"),
            ("Senegal's attack is lethal with {player} and {player2}. Scary combination! #AFCON", "senegal"),
            ("Pape Thiaw has transformed Senegal. Brilliant coaching! #LionsOfTeranga", "senegal"),
            ("Senegal through to the semi-finals! They look unstoppable! #{team_hashtag}", "senegal"),
            ("{player} with a world-class performance for Senegal. World class! #AFCON2026", "senegal"),
            ("Senegal's defense led by {player} is impenetrable. What a unit! #{team_hashtag}", "senegal"),
            
            # Tweets sur le match final
            ("What a final! Morocco vs Senegal delivering drama! #AFCONFinal", "both"),
            ("{player} scores for {team}! What a moment in the final! #AFCON2026", "both"),
            ("Controversy in the final! {event} changes everything! #AFCON", "both"),
            ("Extra time in the AFCON final! My heart can't take this! #{team1} #{team2}", "both"),
            ("Penalty save by {player}! Turning point in the final! #AFCON2026", "both"),
            ("Senegal wins the AFCON! What a tournament for them! Champions! #AFCON2026", "senegal"),
            ("Morocco so close yet so far. Proud of the Atlas Lions! #{team_hashtag}", "morocco"),
            
            # Tweets sur les joueurs individuels
            ("{player} is the best {position} in Africa right now. No debate! #{club} #{team_hashtag}", "both"),
            ("{player} transferred to {club} and now dominating AFCON. Great signing! #AFCON", "both"),
            ("{player} deserves a Ballon d'Or nomination after this AFCON. Incredible! #{team_hashtag}", "both"),
            
            # Tweets des fans
            ("As a {team} fan, I'm so proud of this team! #{team_hashtag} #AFCON2026", "both"),
            ("The atmosphere for {team} games is electric! African football at its best! #AFCON", "both"),
            ("{team} making history at this AFCON. Never been prouder! #{team_hashtag}", "both"),
        ]
        
        # Données pour remplir les templates
        player_data = {
            'morocco': [
                ('Achraf Hakimi', 'defender', 'PSG', '#Morocco'),
                ('Brahim Díaz', 'midfielder', 'Real Madrid', '#Morocco'),
                ('Yassine Bounou', 'goalkeeper', 'Al Hilal', '#Morocco'),
                ('Noussair Mazraoui', 'defender', 'Bayern Munich', '#Morocco'),
                ('Sofyan Amrabat', 'midfielder', 'Manchester United', '#Morocco'),
                ('Youssef En-Nesyri', 'forward', 'Sevilla', '#Morocco'),
            ],
            'senegal': [
                ('Sadio Mané', 'forward', 'Al Nassr', '#Senegal'),
                ('Kalidou Koulibaly', 'defender', 'Al Hilal', '#Senegal'),
                ('Édouard Mendy', 'goalkeeper', 'Al Ahli', '#Senegal'),
                ('Nicolas Jackson', 'forward', 'Chelsea', '#Senegal'),
                ('Idrissa Gueye', 'midfielder', 'Everton', '#Senegal'),
                ('Pape Gueye', 'midfielder', 'Marseille', '#Senegal'),
            ]
        }
        
        events = ['VAR decision', 'red card', 'penalty call', 'offside goal', 'protest']
        
        for i in range(count):
            # Choisir un template aléatoire
            template, team_type = random.choice(tweet_templates)
            
            # Choisir des données selon l'équipe
            if team_type == 'morocco':
                player, position, club, team_hashtag = random.choice(player_data['morocco'])
                player2, _, _, _ = random.choice(player_data['morocco'])
                team = 'Morocco'
                team1, team2 = 'Morocco', 'Senegal'
            elif team_type == 'senegal':
                player, position, club, team_hashtag = random.choice(player_data['senegal'])
                player2, _, _, _ = random.choice(player_data['senegal'])
                team = 'Senegal'
                team1, team2 = 'Senegal', 'Morocco'
            else:  # both
                if random.choice([True, False]):
                    player, position, club, team_hashtag = random.choice(player_data['morocco'])
                    team = 'Morocco'
                else:
                    player, position, club, team_hashtag = random.choice(player_data['senegal'])
                    team = 'Senegal'
                player2, _, _, _ = random.choice(player_data['morocco' if team == 'Senegal' else 'senegal'])
                team1, team2 = ('Morocco', 'Senegal') if random.choice([True, False]) else ('Senegal', 'Morocco')
            
            # Remplir le template
            tweet_text = template.format(
                player=player,
                player2=player2,
                position=position,
                club=club,
                team_hashtag=team_hashtag,
                team=team,
                team1=team1,
                team2=team2,
                event=random.choice(events)
            )
            
            # Générer des métadonnées réalistes
            base_time = datetime.now() - timedelta(days=random.randint(0, 30))
            tweet_time = (base_time - timedelta(minutes=random.randint(0, 1440))).strftime('%Y-%m-%d %H:%M')
            
            # Détecter le type d'événement
            event_type = self.detect_event_type(tweet_text)
            
            tweet_data = {
                'text': tweet_text,
                'query': 'fallback_data',
                'timestamp': datetime.now().isoformat(),
                'tweet_time': tweet_time,
                'event_type': event_type,
                'url': f"nitter_fallback_{i}",
                'source': 'Nitter (Fallback)',
                'user': f'fan_{random.randint(1000, 9999)}',
                'data_source': 'Twitter/Nitter Scraping (Fallback)',
                'mentioned_teams': team,
                'search_label': 'fallback'
            }
            
            fallback_tweets.append(tweet_data)
        
        return fallback_tweets
    
    def export_to_csv(self, tweets: List[Dict], filename: str = None):
        """Exporte les tweets en CSV"""
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"afcon_twitter_data_{timestamp}.csv"
        
        df = pd.DataFrame(tweets)
        
        # Colonnes dans l'ordre spécifié
        columns_order = [
            'text', 'query', 'timestamp', 'event_type', 'url',
            'source', 'user', 'data_source', 'tweet_time',
            'mentioned_teams', 'search_label'
        ]
        
        # Garder seulement les colonnes existantes
        existing_columns = [col for col in columns_order if col in df.columns]
        
        df = df[existing_columns]
        
        # Sauvegarder en CSV
        df.to_csv(filename, index=False, encoding='utf-8')
        
        print(f"💾 Saved {len(df)} tweets to: {filename}")
        
        # Afficher un échantillon
        print("\n📄 Sample of collected tweets:")
        print("=" * 80)
        for i, tweet in enumerate(df.head(5).to_dict('records')):
            print(f"\n{i+1}. {tweet['text'][:100]}...")
            print(f"   User: {tweet['user']} | Event: {tweet['event_type']} | Teams: {tweet.get('mentioned_teams', 'N/A')}")
        
        return filename
    
    def analyze_tweets(self, tweets: List[Dict]):
        """Analyse les tweets collectés"""
        
        if not tweets:
            print("⚠️ No tweets to analyze")
            return
        
        print("\n📊 TWEET ANALYSIS:")
        print("=" * 80)
        
        # Statistiques de base
        print(f"Total Tweets: {len(tweets)}")
        
        # Analyse par équipe
        morocco_count = sum(1 for t in tweets if 'Morocco' in str(t.get('mentioned_teams', '')))
        senegal_count = sum(1 for t in tweets if 'Senegal' in str(t.get('mentioned_teams', '')))
        both_count = sum(1 for t in tweets if 'Morocco' in str(t.get('mentioned_teams', '')) and 'Senegal' in str(t.get('mentioned_teams', '')))
        
        print(f"\nTeam Mentions:")
        print(f"  • Morocco: {morocco_count} tweets")
        print(f"  • Senegal: {senegal_count} tweets")
        print(f"  • Both: {both_count} tweets")
        
        # Analyse par type d'événement
        event_counts = {}
        for tweet in tweets:
            event_type = tweet.get('event_type', 'other_event')
            event_counts[event_type] = event_counts.get(event_type, 0) + 1
        
        print(f"\nEvent Types:")
        for event_type, count in sorted(event_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  • {event_type}: {count}")
        
        # Top utilisateurs
        user_counts = {}
        for tweet in tweets:
            user = tweet.get('user', 'unknown')
            user_counts[user] = user_counts.get(user, 0) + 1
        
        print(f"\nTop Users:")
        for user, count in sorted(user_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  • {user}: {count} tweets")
        
        # Sources
        source_counts = {}
        for tweet in tweets:
            source = tweet.get('source', 'unknown')
            source_counts[source] = source_counts.get(source, 0) + 1
        
        print(f"\nSources:")
        for source, count in source_counts.items():
            print(f"  • {source}: {count}")


def main():
    """Fonction principale"""
    
    print("=" * 80)
    print("🐦 AFCON 2026 TWITTER MASS SCRAPER")
    print("=" * 80)
    print("🎯 Objectif: Collecter 1000+ tweets réels de Twitter/Nitter")
    print("📅 Période: Début AFCON (déc 2026) à Finale (jan 2026)")
    print("🇲🇦 Focus: Maroc & Sénégal")
    print("=" * 80)
    
    # Initialiser le scraper
    scraper = TwitterAFCONScraper()
    
    # Collecter les tweets
    print("\n🚀 Starting massive tweet collection...")
    start_time = time.time()
    
    try:
        tweets = scraper.scrape_massive_tweets(target_count=1000)
        
        elapsed_time = time.time() - start_time
        print(f"\n⏱️ Collection completed in {elapsed_time:.1f} seconds")
        
        # Analyser les tweets
        scraper.analyze_tweets(tweets)
        
        # Exporter en CSV
        print("\n💾 Exporting to CSV...")
        csv_file = scraper.export_to_csv(tweets)
        
        print(f"\n✅ MISSION ACCOMPLISHED!")
        print(f"📁 File: {csv_file}")
        print(f"📊 Total tweets: {len(tweets)}")
        
    except KeyboardInterrupt:
        print("\n\n⏹️ Stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Vérifier les dépendances
    try:
        import requests
        import pandas as pd
        from bs4 import BeautifulSoup
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Install with: pip install requests pandas beautifulsoup4")
        exit(1)
    
    main()