from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
import time
import json
import os
import random
import pickle
from datetime import datetime
from bs4 import BeautifulSoup


from config_loader import load_search_terms

class FacebookInfiniteScroller:
    def __init__(self):
        self.chrome_debug_port = 9222
        self.checkpoint_file = "facebook_scroll_checkpoint.pkl"
        self.data_folder = "facebook_data"
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Créer dossier de données
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)
        
        # Données collectées
        self.all_posts = []
        self.processed_post_ids = set()
        self.current_term_index = 0
        self.current_scroll_position = 0
        
        # Charger checkpoint
        self.load_checkpoint()
        
        # Charger les termes de recherche depuis un fichier externe (config/matches.json)
        # Remplacer l'import direct par l'import depuis le module twitter_scraper
        try:
            match_name = os.environ.get("RESEARCH_MATCH")  # optionnel : si défini, on cible un match précis
            self.search_terms = load_search_terms(match_name=match_name)
            if not self.search_terms:
                raise ValueError("Aucun terme chargé depuis la config")
            print(f" {len(self.search_terms)} termes de recherche (chargés depuis config)")
        except Exception as e:
            print(f"  Erreur chargement config: {e}")
            # Fallback minimal si la config manque
            self.search_terms = ["afcon", "Morocco vs Senegal"]
            print(f" Utilisation d'un fallback: {len(self.search_terms)} termes")
        
        print(f" Facebook Infinite Scroller initialisé")
        print(f" {len(self.search_terms)} termes de recherche")
        print(f" Objectif: 500 MB de données")
        
        # Initialiser Chrome
        self.setup_chrome()
    
    def setup_chrome(self):
        """Configure et ouvre Chrome automatiquement"""
        chrome_options = Options()
        
        # Configuration pour garder la session
        chrome_options.add_argument("--user-data-dir=/tmp/chrome_facebook_session")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        
        try:
            print(" Ouverture de Chrome...")
            self.driver = webdriver.Chrome(options=chrome_options)
            
            # Masquer l'automatisation
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
            
            self.wait = WebDriverWait(self.driver, 15)
            print(" Chrome ouvert avec succès")
            
            # Aller sur Facebook
            print(" Navigation vers Facebook...")
            self.driver.get("https://www.facebook.com")
            time.sleep(3)
            
            # Vérifier si déjà connecté
            print("\n Vérification de la connexion...")
            time.sleep(2)
            
            # Attendre que l'utilisateur se connecte
            print("\n" + "="*70)
            print(" CONNEXION REQUISE")
            print("="*70)
            print("Si vous n'êtes pas connecté à Facebook:")
            print("  1. Connectez-vous maintenant dans la fenêtre Chrome ouverte")
            print("  2. Attendez d'être sur la page d'accueil de Facebook")
            print("  3. Revenez ici et appuyez sur Entrée")
            print("\nSi vous êtes déjà connecté, appuyez simplement sur Entrée")
            print("="*70)
            
            input("\n Appuyez sur Entrée quand vous êtes connecté à Facebook... ")
            
            print("\n Prêt à commencer le scraping!")
            time.sleep(2)
            
        except Exception as e:
            print(f" Erreur ouverture Chrome: {e}")
            raise
    
    def save_checkpoint(self):
        """Sauvegarde la progression"""
        checkpoint = {
            'current_term_index': self.current_term_index,
            'current_scroll_position': self.current_scroll_position,
            'processed_post_ids': self.processed_post_ids,
            'total_collected': len(self.all_posts),
            'timestamp': datetime.now().isoformat()
        }
        with open(self.checkpoint_file, 'wb') as f:
            pickle.dump(checkpoint, f)
    
    def load_checkpoint(self):
        """Charge la progression précédente"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'rb') as f:
                    checkpoint = pickle.load(f)
                self.current_term_index = checkpoint.get('current_term_index', 0)
                self.current_scroll_position = checkpoint.get('current_scroll_position', 0)
                self.processed_post_ids = checkpoint.get('processed_post_ids', set())
                print(f" Checkpoint chargé:")
                print(f"   Terme actuel: {self.current_term_index}")
                print(f"   Posts déjà collectés: {checkpoint.get('total_collected', 0)}")
            except:
                print("  Checkpoint corrompu, recommence à zéro")
    
    def save_data_incremental(self):
        """Sauvegarde incrémentale des données - fichier global + fichiers individuels"""
        if not self.all_posts:
            return 0
        
        if self.all_posts:
            last_post = self.all_posts[-1]
            post_number = len(self.all_posts)
            individual_file = os.path.join(self.data_folder, f"post_{post_number:05d}_{self.timestamp}.json")
            
            with open(individual_file, 'w', encoding='utf-8') as f:
                json.dump(last_post, f, ensure_ascii=False, indent=2)
        
        # 2. Sauvegarder le fichier global (tous les posts)
        json_file = os.path.join(self.data_folder, f"facebook_posts_{self.timestamp}.json")
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(self.all_posts, f, ensure_ascii=False, indent=2)
        
        # Calculer taille
        size_mb = os.path.getsize(json_file) / (1024 * 1024)
        
        if len(self.all_posts) % 10 == 0:
            total_comments = sum(len(p.get('comments', [])) for p in self.all_posts)
            print(f"\n Checkpoint: {len(self.all_posts)} posts | {size_mb:.2f} MB | {total_comments} commentaires")
            print(f" Fichiers: global + {len(self.all_posts)} fichiers individuels")
        
        return size_mb
    
    def human_scroll(self, aggressive=False):
        """Scroll naturel comme un humain avec interactions"""
        if aggressive:
            # Scroll agressif jusqu'en bas de la page
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(3, 5))
        else:
            # Scroll progressif AVEC smooth behavior
            scroll_amount = random.randint(1000, 2000)
            self.driver.execute_script(f"""
                window.scrollBy({{
                    top: {scroll_amount},
                    left: 0,
                    behavior: 'smooth'
                }});
            """)
            
            # Pause naturelle
            time.sleep(random.uniform(2, 3))
            
            # Parfois scroll back un peu (comportement humain)
            if random.random() < 0.1:
                back_scroll = random.randint(100, 300)
                self.driver.execute_script(f"window.scrollBy(0, -{back_scroll});")
                time.sleep(random.uniform(1, 2))
    
    def wait_for_content_load(self):
        """Attendre que le contenu charge avec vérification"""
        time.sleep(random.uniform(3, 5))
    
    def extract_post_id(self, post_element):
        """Extrait un ID unique du post"""
        try:
            # Essayer plusieurs méthodes pour obtenir un ID unique
            post_html = post_element.get_attribute('outerHTML')
            
            # Chercher data-id ou similaire
            if 'data-id' in post_html:
                return post_html.split('data-id="')[1].split('"')[0]
            
            # Chercher dans le href
            links = post_element.find_elements(By.TAG_NAME, "a")
            for link in links:
                href = link.get_attribute('href')
                if href and '/posts/' in href:
                    return href
            
            # Utiliser hash du contenu comme fallback
            return str(hash(post_html[:200]))
        except:
            return None
    
    def click_see_more(self, post_element):
        """Clique sur 'Voir plus' pour le contenu complet"""
        try:
            see_more_buttons = post_element.find_elements(By.XPATH,
                ".//div[contains(text(), 'Voir plus') or contains(text(), 'See more')]")
            if see_more_buttons:
                see_more_buttons[0].click()
                time.sleep(random.uniform(0.5, 1.5))
        except:
            pass
    
    def extract_post_content_simple(self, post_element):
        """Extraction SIMPLE: copie tout le texte visible du post"""
        post_data = {
            'scraped_at': datetime.now().isoformat()
        }
        
        try:
            # Cliquer sur "Voir plus" pour tout afficher
            self.click_see_more(post_element)
            time.sleep(0.5)
            
            # COPIER TOUT LE TEXTE VISIBLE (comme un copier-coller)
            full_text = post_element.text.strip()
            
            # Séparer en lignes
            lines = [line.strip() for line in full_text.split('\n') if line.strip()]
            
            # Le premier élément avec du texte substantiel = probablement l'auteur
            author = "Unknown"
            for line in lines[:5]:
                if len(line) > 3 and len(line) < 100 and not any(x in line.lower() for x in ['voir', 'like', 'comment', 'share', 'grupo']):
                    author = line
                    break
            
            post_data['author'] = author
            post_data['full_text'] = full_text
            post_data['text_lines'] = lines
            
            # Chercher URL dans tous les liens
            post_url = ""
            try:
                links = post_element.find_elements(By.TAG_NAME, "a")
                for link in links:
                    href = link.get_attribute('href')
                    if href and ('/posts/' in href or 'story_fbid' in href or 'photo' in href):
                        post_url = href.split('?')[0]
                        break
            except:
                pass
            
            post_data['post_url'] = post_url
            
            # Compter approximativement
            text_lower = full_text.lower()
            
            # Réactions (chercher des chiffres avant "reactions" ou "like")
            reactions = "0"
            import re
            reaction_patterns = [
                r'(\d+[kKmM]?)\s*(?:reaction|like|j\'aime)',
                r'(\d+[kKmM]?)\s*person',
            ]
            for pattern in reaction_patterns:
                match = re.search(pattern, full_text, re.IGNORECASE)
                if match:
                    reactions = match.group(1)
                    break
            post_data['reactions'] = reactions
            
            # Commentaires
            comments_count = "0"
            comment_patterns = [
                r'(\d+[kKmM]?)\s*(?:comment|comentario)',
                r'(\d+[kKmM]?)\s*(?:réponse|response)',
            ]
            for pattern in comment_patterns:
                match = re.search(pattern, full_text, re.IGNORECASE)
                if match:
                    comments_count = match.group(1)
                    break
            post_data['comments_count'] = comments_count
            
            # Partages
            shares = "0"
            share_patterns = [
                r'(\d+[kKmM]?)\s*(?:share|partage|compartir)',
            ]
            for pattern in share_patterns:
                match = re.search(pattern, full_text, re.IGNORECASE)
                if match:
                    shares = match.group(1)
                    break
            post_data['shares'] = shares
            
        except Exception as e:
            print(f" Erreur extraction: {e}")
            post_data['full_text'] = ""
            post_data['author'] = "Unknown"
            post_data['post_url'] = ""
            post_data['reactions'] = "0"
            post_data['comments_count'] = "0"
            post_data['shares'] = "0"
        
        return post_data
        """Extrait le contenu complet d'un post avec sélecteurs améliorés"""
        post_data = {
            'scraped_at': datetime.now().isoformat()
        }
        
        try:
            # Cliquer sur "Voir plus" si existe
            self.click_see_more(post_element)
            
            # MÉTHODE 1: Extraire le texte du post (multiples stratégies)
            try:
                # Stratégie A: Chercher tous les div avec texte
                text_elements = post_element.find_elements(By.CSS_SELECTOR, "div[dir='auto']")
                if text_elements:
                    texts = [elem.text.strip() for elem in text_elements if elem.text.strip() and len(elem.text.strip()) > 10]
                    post_data['content'] = ' '.join(texts[:5]) if texts else ""
                
                # Stratégie B: Si vide, prendre tout le texte du post
                if not post_data.get('content'):
                    post_data['content'] = post_element.text.strip()[:500]  # Max 500 chars
            except:
                post_data['content'] = ""
            
            # MÉTHODE 2: Extraire l'auteur (multiples stratégies)
            try:
                # Stratégie A: Chercher <strong> dans un lien
                author_elem = post_element.find_element(By.CSS_SELECTOR, "a[role='link'] strong, a strong, strong span")
                post_data['author'] = author_elem.text.strip()
                
                # URL de l'auteur
                try:
                    author_link = post_element.find_element(By.CSS_SELECTOR, "a[role='link']")
                    post_data['author_url'] = author_link.get_attribute('href')
                except:
                    post_data['author_url'] = ""
            except:
                # Stratégie B: Chercher le premier lien
                try:
                    links = post_element.find_elements(By.TAG_NAME, "a")
                    for link in links[:3]:
                        text = link.text.strip()
                        if text and len(text) > 2 and len(text) < 50:
                            post_data['author'] = text
                            post_data['author_url'] = link.get_attribute('href')
                            break
                except:
                    pass
                
                if not post_data.get('author'):
                    post_data['author'] = "Unknown"
                    post_data['author_url'] = ""
            
            # MÉTHODE 3: Extraire timestamp et URL du post
            try:
                # Chercher les liens avec timestamp
                time_links = post_element.find_elements(By.CSS_SELECTOR, "a[href*='/posts/'], a[href*='story_fbid']")
                for link in time_links:
                    href = link.get_attribute('href')
                    if href and ('/posts/' in href or 'story_fbid' in href):
                        post_data['post_url'] = href.split('?')[0]  # Enlever les params
                        # Essayer de prendre le texte comme timestamp
                        time_text = link.text.strip()
                        if time_text and len(time_text) < 50:
                            post_data['timestamp'] = time_text
                        break
            except:
                pass
            
            if not post_data.get('timestamp'):
                post_data['timestamp'] = ""
            if not post_data.get('post_url'):
                post_data['post_url'] = ""
            
            # MÉTHODE 4: Extraire réactions (texte ou aria-label)
            try:
                # Chercher les spans avec aria-label contenant des chiffres
                reaction_elems = post_element.find_elements(By.CSS_SELECTOR, "span[aria-label]")
                for elem in reaction_elems:
                    aria = elem.get_attribute('aria-label')
                    if aria and any(char.isdigit() for char in aria):
                        post_data['reactions'] = aria
                        break
                
                # Si pas trouvé, chercher du texte avec "reaction"
                if not post_data.get('reactions'):
                    all_text = post_element.text.lower()
                    if 'reaction' in all_text or 'like' in all_text:
                        # Extraire les chiffres
                        import re
                        numbers = re.findall(r'\d+[kKmM]?', post_element.text)
                        if numbers:
                            post_data['reactions'] = numbers[0]
            except:
                pass
            
            if not post_data.get('reactions'):
                post_data['reactions'] = "0"
            
            # MÉTHODE 5: Extraire nombre de commentaires
            try:
                comments_elems = post_element.find_elements(By.XPATH,
                    ".//*[contains(text(), 'comment') or contains(text(), 'commentaire') or contains(text(), 'Comment')]")
                for elem in comments_elems:
                    text = elem.text.strip()
                    if text:
                        post_data['comments_count'] = text
                        break
            except:
                pass
            
            if not post_data.get('comments_count'):
                post_data['comments_count'] = "0"
            
            # MÉTHODE 6: Extraire partages
            try:
                shares_elems = post_element.find_elements(By.XPATH,
                    ".//*[contains(text(), 'share') or contains(text(), 'partage') or contains(text(), 'Share')]")
                for elem in shares_elems:
                    text = elem.text.strip()
                    if text:
                        post_data['shares'] = text
                        break
            except:
                pass
            
            if not post_data.get('shares'):
                post_data['shares'] = "0"
            
        except Exception as e:
            print(f"   Erreur extraction post: {e}")
        
        return post_data
    
    def expand_comments(self, max_expansions=5):
        """Clique sur 'Voir plus de commentaires' plusieurs fois"""
        expansions = 0
        while expansions < max_expansions:
            try:
                see_more_comments = self.driver.find_elements(By.XPATH,
                    "//span[contains(text(), 'Voir plus de commentaires') or contains(text(), 'View more comments') or contains(text(), 'See more comments')]")
                
                if not see_more_comments:
                    break
                
                # Scroll vers le bouton
                self.driver.execute_script("arguments[0].scrollIntoView(true);", see_more_comments[0])
                time.sleep(0.5)
                
                # Cliquer
                see_more_comments[0].click()
                time.sleep(random.uniform(2, 4))
                expansions += 1
            except:
                break
    
    def extract_comments_inline(self, post_element):
        """Extrait les commentaires directement depuis le post (sans navigation)"""
        comments = []
        
        try:
            # Scroll vers le post
            self.driver.execute_script("arguments[0].scrollIntoView(true);", post_element)
            time.sleep(1)
            
            # Chercher le bouton de commentaires pour les ouvrir
            try:
                # Cliquer sur "X commentaires" pour ouvrir
                comments_button = post_element.find_elements(By.XPATH,
                    ".//span[contains(text(), 'comment') or contains(text(), 'commentaire')]")
                if comments_button:
                    comments_button[0].click()
                    time.sleep(random.uniform(2, 3))
            except:
                pass
            
            # Élargir les commentaires (cliquer "Voir plus de commentaires")
            for _ in range(3):
                try:
                    see_more = post_element.find_elements(By.XPATH,
                        ".//span[contains(text(), 'Voir plus de commentaires') or contains(text(), 'View more comments')]")
                    if see_more:
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", see_more[0])
                        time.sleep(0.5)
                        see_more[0].click()
                        time.sleep(random.uniform(2, 3))
                    else:
                        break
                except:
                    break
            
            # Extraire les commentaires visibles dans ce post
            comment_elements = post_element.find_elements(By.CSS_SELECTOR, "div[role='article']")
            
            for elem in comment_elements[:50]:  # Max 50 commentaires par post
                try:
                    # Cliquer sur "Voir plus" dans le commentaire
                    try:
                        see_more = elem.find_elements(By.XPATH,
                            ".//div[contains(text(), 'Voir plus') or contains(text(), 'See more')]")
                        if see_more:
                            see_more[0].click()
                            time.sleep(0.3)
                    except:
                        pass
                    
                    # Texte du commentaire
                    text_elems = elem.find_elements(By.CSS_SELECTOR, "div[dir='auto']")
                    comment_text = ' '.join([t.text.strip() for t in text_elems if t.text.strip()])
                    
                    if not comment_text or len(comment_text) < 3:
                        continue
                    
                    # Auteur du commentaire
                    try:
                        author_elem = elem.find_element(By.CSS_SELECTOR, "a[role='link'] span")
                        author = author_elem.text.strip()
                    except:
                        author = "Unknown"
                    
                    # Timestamp
                    try:
                        time_elem = elem.find_element(By.CSS_SELECTOR, "span[id^='jsc_'] a")
                        timestamp = time_elem.get_attribute('aria-label') or time_elem.text
                    except:
                        timestamp = ""
                    
                    # Réactions sur le commentaire
                    try:
                        reactions = elem.find_elements(By.CSS_SELECTOR, "span[aria-label*='reaction']")
                        comment_reactions = reactions[0].get_attribute('aria-label') if reactions else "0"
                    except:
                        comment_reactions = "0"
                    
                    comments.append({
                        'author': author,
                        'text': comment_text,
                        'timestamp': timestamp,
                        'reactions': comment_reactions
                    })
                    
                except:
                    continue
            
        except Exception as e:
            print(f"       Erreur extraction commentaires inline: {e}")
        
        return comments
        """Extrait tous les commentaires d'un post"""
        comments = []
        
        if not post_url:
            return comments
        
        try:
            # Aller sur le post
            self.driver.get(post_url)
            time.sleep(random.uniform(3, 5))
            
            # Élargir les commentaires
            self.expand_comments(max_expansions=5)
            
            # Scroll pour charger plus de commentaires
            for _ in range(3):
                self.driver.execute_script("window.scrollBy(0, 500);")
                time.sleep(random.uniform(1, 2))
            
            # Extraire les commentaires
            comment_elements = self.driver.find_elements(By.CSS_SELECTOR, "div[role='article']")
            
            for elem in comment_elements[:100]:  # Max 100 commentaires par post
                try:
                    # Cliquer sur "Voir plus" dans le commentaire
                    try:
                        see_more = elem.find_elements(By.XPATH,
                            ".//div[contains(text(), 'Voir plus') or contains(text(), 'See more')]")
                        if see_more:
                            see_more[0].click()
                            time.sleep(0.3)
                    except:
                        pass
                    
                    # Texte du commentaire
                    text_elems = elem.find_elements(By.CSS_SELECTOR, "div[dir='auto']")
                    comment_text = ' '.join([t.text.strip() for t in text_elems if t.text.strip()])
                    
                    if not comment_text or len(comment_text) < 5:
                        continue
                    
                    # Auteur du commentaire
                    try:
                        author_elem = elem.find_element(By.CSS_SELECTOR, "a[role='link'] span")
                        author = author_elem.text.strip()
                    except:
                        author = "Unknown"
                    
                    # Timestamp
                    try:
                        time_elem = elem.find_element(By.CSS_SELECTOR, "span[id^='jsc_'] a")
                        timestamp = time_elem.get_attribute('aria-label') or time_elem.text
                    except:
                        timestamp = ""
                    
                    # Réactions sur le commentaire
                    try:
                        reactions = elem.find_elements(By.CSS_SELECTOR, "span[aria-label*='reaction']")
                        comment_reactions = reactions[0].get_attribute('aria-label') if reactions else "0"
                    except:
                        comment_reactions = "0"
                    
                    comments.append({
                        'author': author,
                        'text': comment_text,
                        'timestamp': timestamp,
                        'reactions': comment_reactions
                    })
                    
                except Exception as e:
                    continue
            
            # Retourner à la page de recherche
            self.driver.back()
            time.sleep(random.uniform(2, 3))
            
        except Exception as e:
            print(f"     Erreur extraction commentaires: {e}")
            try:
                self.driver.back()
                time.sleep(2)
            except:
                pass
        
        return comments
    
    def scroll_until_end(self, search_term):
        """Scroll jusqu'à la fin des résultats pour un terme"""
        print(f"\n{'='*70}")
        print(f"🔍 Recherche: '{search_term}'")
        print(f"{'='*70}")
        
        # Construire URL
        search_url = f"https://www.facebook.com/search/top/?q={search_term}"
        
        # Aller sur la page de recherche
        self.driver.get(search_url)
        self.wait_for_content_load()
        
        # IMPORTANT: Attendre plus longtemps au chargement initial
        print("   ⏳ Attente chargement initial...")
        time.sleep(random.uniform(3, 5))
        
        posts_this_term = 0
        no_new_posts_count = 0
        scroll_count = 0
        total_scrolls_without_new = 0
        last_extracted_count = 0
        
        while True:
            # Faire UN SEUL scroll à la fois (plus humain)
            self.human_scroll()
            scroll_count += 1
            total_scrolls_without_new += 1
            
            # Vérifier après CHAQUE scroll (pas tous les 3)
            try:
                post_elements = self.driver.find_elements(By.CSS_SELECTOR, "div[role='article']")
            except:
                print("     Impossible de trouver les posts, passage au suivant")
                break
            
            # NE PAS afficher le nombre de posts visibles (il fluctue à cause du virtual scroll)
            # Afficher seulement tous les 10 scrolls pour ne pas polluer
            if scroll_count % 10 == 0:
                print(f"    Scroll #{scroll_count} - Posts collectés ce terme: {posts_this_term} | Total: {len(self.all_posts)}")
            
            # Extraire les nouveaux posts
            new_posts_count = 0
            for post_elem in post_elements:
                try:
                    # Obtenir ID unique
                    post_id = self.extract_post_id(post_elem)
                    
                    if not post_id or post_id in self.processed_post_ids:
                        continue
                    
                    # Marquer comme traitéy
                    self.processed_post_ids.add(post_id)
                    
                    # Extraire données du post (MÉTHODE SIMPLE)
                    print(f"   Extraction post {posts_this_term + 1}...")
                    post_data = self.extract_post_content_simple(post_elem)
                    post_data['search_term'] = search_term
                    post_data['post_id'] = post_id
                    
                    # FILTRER LES FAUX POSTS
                    full_text = post_data.get('full_text', '')
                    
                    # 1. Ignorer si trop court (moins de 30 caractères)
                    if len(full_text) < 30:
                        print(f"       Ignoré (texte trop court: {len(full_text)} chars)")
                        continue
                    
                    # 2. Ignorer les groupes/boutons
                    ignore_keywords = ['Unirte', 'Join group', 'Ver todos', 'See all', 'publicaciones al día']
                    if any(keyword in full_text for keyword in ignore_keywords):
                        print(f"        Ignoré (contenu de groupe/bouton)")
                        continue
                    
                    # 3. Pour être valide, avoir une URL de post OU beaucoup de texte
                    has_url = bool(post_data.get('post_url'))
                    has_content = len(full_text) > 100
                    
                    if not has_url and not has_content:
                        print(f"        Ignoré (pas d'URL et peu de contenu)")
                        continue
                    
                    # AFFICHER CE QUI A ÉTÉ EXTRAIT
                    print(f"        Auteur: {post_data['author']}")
                    
                    text_preview = full_text[:150].replace('\n', ' ') + "..." if len(full_text) > 150 else full_text.replace('\n', ' ')
                    print(f"       Texte ({len(full_text)} chars): {text_preview}")
                    
                    if post_data['post_url']:
                        print(f"      URL: {post_data['post_url'][:60]}...")
                    
                    print(f"       Réactions: {post_data['reactions']}")
                    print(f"       Commentaires: {post_data['comments_count']}")
                    print(f"       Partages: {post_data['shares']}")
                    
                    # Extraire commentaires
                    print(f"       Extraction commentaires...")
                    post_data['comments'] = self.extract_comments_inline(post_elem)
                    print(f"       {len(post_data['comments'])} commentaires collectés")
                    
                    if post_data['comments']:
                        for comment in post_data['comments'][:2]:
                            comment_preview = comment['text'][:60].replace('\n', ' ') + "..." if len(comment['text']) > 60 else comment['text']
                            print(f"         └─ {comment['author']}: {comment_preview}")
                    
                    # Ajouter aux données
                    self.all_posts.append(post_data)
                    posts_this_term += 1
                    new_posts_count += 1
                    
                    print(f"       POST AJOUTÉ! Total: {len(self.all_posts)}")
                    
                    # SAUVEGARDER APRÈS CHAQUE POST (sécurité maximale)
                    size_mb = self.save_data_incremental()
                    self.save_checkpoint()
                    
                    # Vérifier si on a atteint 500 MB
                    if size_mb >= 500:
                        print(f"\n OBJECTIF ATTEINT: {size_mb:.2f} MB collectés!")
                        return True
                    
                    print()
                    
                except StaleElementReferenceException:
                    continue
                except Exception as e:
                    print(f"    Erreur post: {e}")
                    continue
            
            # Mise à jour du compteur
            if new_posts_count > 0:
                print(f"    {new_posts_count} nouveaux posts extraits! Total: {posts_this_term}")
                total_scrolls_without_new = 0
                last_extracted_count = posts_this_term
            
            # Vérifier si vraiment bloqué (aucun nouveau post extrait après 50 scrolls)
            if total_scrolls_without_new > 50:
                no_new_posts_count += 1
                print(f" Aucun nouveau post depuis 50 scrolls ({no_new_posts_count}/5)")
                
                # Essayer des actions de déblocage
                if no_new_posts_count <= 4:
                    print(f" Tentative {no_new_posts_count} de déblocage (scroll up/down)...")
                    # Scroll tout en haut puis tout en bas
                    self.driver.execute_script("window.scrollTo(0, 0);")
                    time.sleep(3)
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(5)
                    total_scrolls_without_new = 0  # Réinitialiser
                    print(f"   ♻️  Reprise du scroll...")
                
                if no_new_posts_count >= 5:
                    print(f"  Fin: {posts_this_term} posts collectés pour '{search_term}'")
                    print(f"  Total scrolls effectués: {scroll_count}")
                    break
            
            # Pause naturelle avant prochain scroll
            time.sleep(random.uniform(1.5, 3))
        
        print(f"\n Terme '{search_term}' terminé: {posts_this_term} posts collectés")
        return False
    
    def run(self):
        """Lance le scraping complet"""
        print("\n" + "="*70)
        print("FACEBOOK INFINITE SCROLL SCRAPER")
        print("="*70)
        print(f" Début: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f" Termes à traiter: {len(self.search_terms)}")
        print(f" Reprise à partir du terme: {self.current_term_index}")
        print(f" Objectif: 500 MB de données")
        print("="*70 + "\n")
        
        try:
            for idx in range(self.current_term_index, len(self.search_terms)):
                self.current_term_index = idx
                term = self.search_terms[idx]
                
                print(f"\n[{idx + 1}/{len(self.search_terms)}] Traitement: '{term}'")
                
                # Scroll jusqu'à la fin
                goal_reached = self.scroll_until_end(term)
                
                # Sauvegarde après chaque terme
                size_mb = self.save_data_incremental()
                self.save_checkpoint()
                
                print(f"\n PROGRESSION:")
                print(f"   Total posts: {len(self.all_posts)}")
                print(f"   Taille: {size_mb:.2f} MB / 500 MB")
                print(f"   Progression: {(size_mb/500)*100:.1f}%")
                
                # Vérifier si objectif atteint
                if goal_reached:
                    break
                
                # Pause entre termes
                time.sleep(random.uniform(5, 10))
            
            # Sauvegarde finale
            size_mb = self.save_data_incremental()
            
            print("\n" + "="*70)
            print(" SCRAPING TERMINÉ!")
            print("="*70)
            print(f" Total posts collectés: {len(self.all_posts)}")
            print(f" Total commentaires: {sum(len(p.get('comments', [])) for p in self.all_posts)}")
            print(f" Taille finale: {size_mb:.2f} MB")
            print(f" Fichier global: facebook_data/facebook_posts_{self.timestamp}.json")
            print(f" Fichiers individuels: {len(self.all_posts)} posts dans facebook_data/post_XXXXX_*.json")
            print("="*70)
            
        except KeyboardInterrupt:
            print("\n\n  INTERRUPTION PAR L'UTILISATEUR")
            self.save_data_incremental()
            self.save_checkpoint()
            print(" Données sauvegardées, vous pouvez reprendre plus tard")
        
        except Exception as e:
            print(f"\n ERREUR: {e}")
            self.save_data_incremental()
            self.save_checkpoint()
            print(" Données sauvegardées avant crash")
            raise
        
        finally:
            try:
                self.driver.quit()
            except:
                pass

def main():
    
    print("\n Le script va:")
    print("  1. Ouvrir Chrome automatiquement")
    print("  2. Vous demander de vous connecter à Facebook")
    print("  3. Commencer le scraping automatique")
    print("  4. Sauvegarder progressivement (tous les 10 posts)")
    print("\n  Vous pouvez interrompre (Ctrl+C) à tout moment, le script reprendra automatiquement\n")
    
    input("Appuyez sur Entrée pour commencer... ")
    
    scraper = FacebookInfiniteScroller()
    scraper.run()

if __name__ == "__main__":
    main()
