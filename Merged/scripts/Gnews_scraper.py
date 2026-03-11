import pandas as pd
from gnews import GNews
from newspaper import Article, Config
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By

def setup_driver():
    options = ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    # 为了看清抓取过程，目前没有开启无头模式
    options.add_argument("--start-maximized")
    
    driver = webdriver.Chrome(options=options)
    return driver

def fetch_article_with_selenium(driver, google_url):
    #使用真实浏览器突破 Google 墙并渲染 JS 动态网页
   
    try:
        #访问 Google News 跳转链接
        driver.get(google_url)
        time.sleep(1) 

        #检测并点击 Google 的 Cookie 同意按钮 (针对欧洲区)
        if "consent.google.com" in driver.current_url or "google.com" in driver.current_url:
            try:
                buttons = driver.find_elements(By.TAG_NAME, "button")
                for btn in buttons:
                    txt = btn.text.lower()
                    if "accept" in txt or "accepter" in txt or "tout accepter" in txt:
                        driver.execute_script("arguments[0].click();", btn)
                        time.sleep(2.5) #等待跳转
                        break
            except:
                pass

        #等待重定向到真实的新闻网站
        for _ in range(10): 
            if "google.com" not in driver.current_url and "consent.google.com" not in driver.current_url:
                break
            time.sleep(0.5)
            
        real_url = driver.current_url
        
        #如果依然卡在 google.com，说明被死锁了，放弃
        if "google.com" in real_url:
            return None, real_url 

        #重点：给新闻网站 3 秒钟时间加载动态内容 (文字、图片等)
        time.sleep(3)
        
        #直接抽取浏览器已经渲染好的最终 HTML 源码
        html_source = driver.page_source

        #将含有动态内容的 HTML 喂给 newspaper3k 解析
        config = Config()
        config.browser_user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        
        article = Article(url=real_url, config=config)
        article.download(input_html=html_source) #直接解析提取的完整源码
        article.parse()
        
        return article.text, real_url
        
    except Exception as e:
        return None, google_url

def main():
    print("Robot d'exploration Web à thème d'actualités")
    
    topic = input("Veuillez saisir le sujet que vous souhaitez rechercher. (Ex: Artificial Intelligence): ")
    if not topic.strip():
        print("Le sujet ne peut pas être vide ! Le programme se termine.")
        return
        
    lang = input("Veuillez saisir la langue de recherche (par exemple : zh-Hans=中文, en=英文, fr=法文) [par défaut en]: ") or "en"
    country = input("Veuillez saisir le code du pays (par exemple : US=美国, CN=中国, FR=法国) [par défaut US]: ") or "US"
    max_num_str = input("Veuillez saisir le nombre maximum d'articles à récupérer [par défaut 30]: ") or "30"
    max_num = int(max_num_str)

    print(f"\nDémarrer la recherche: [{topic}] (Lanaguage: {lang}, Pays: {country}, Nombre maximal de liens récupérés: {max_num})")
    print("Démarrage de votre navigateur, veuillez patienter...\n")

    driver = setup_driver()
    all_data = []
    total_count = 0
    seen_titles = set()
    
    try:
        google_news = GNews(
            language=lang, 
            country=country, 
            max_results=max_num
        )
        
        results = google_news.get_news(topic)
        
        if not results:
            print("Aucune actualité pertinente n'a été trouvée. Veuillez essayer de modifier vos mots-clés ou votre langue.")
            return

        print(f"Récupération réussie des liens d'actualités liés à{len(results)} début de l'extraction du texte...\n")

        for item in results:
            title = item.get('title')
            if title in seen_titles: continue
            
            google_url = item.get('url')
            if google_url:
                #调用全新的渲染函数
                full_text, real_url = fetch_article_with_selenium(driver, google_url)
                
                seen_titles.add(title)
                
                #质量控制：正文大于 150 字符才算成功
                if full_text and len(full_text) > 150: 
                    all_data.append({
                        "topic": topic,
                        "publisher": item.get('publisher', {}).get('title'),
                        "date": item.get('published date'),
                        "title": title,
                        "full_text": full_text, 
                        "url": real_url
                    })
                    total_count += 1
                    print(f"[{total_count}/{len(results)}] succès: {title[:40]}...")
                else:
                    print(f"[Passer] Blocage de contenu ou abonnement payant requis : {title[:40]}...")
                    
    except Exception as e:
        print(f"    !!! Une erreur s'est produite : {e}")
            
    finally:
        driver.quit()

    if all_data:
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=['full_text']) 
        
        safe_topic = "".join([c if c.isalnum() else "_" for c in topic])
        filename = f"Gnews_data_{safe_topic}.csv"
        
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        print("\n" + "="*50)
        print(f"Récupération terminée ! Total articles récupérés : {len(df)}")
        print(f"Les données ont été sauvegardées automatiquement dans : {filename}")
        print("="*50)
    else:
        print("\n  Impossible de récupérer les données du texte, un contrôle anti-robot très strict a été détecté.")

if __name__ == "__main__":
    main()
