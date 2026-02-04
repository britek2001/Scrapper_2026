import pandas as pd
from gnews import GNews
from newspaper import Article, Config
import time
import random
from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions

# ================= 配置区域 (数据核弹版) =================
# 目标：地毯式轰炸
MAX_RESULTS_PER_KEYWORD = 100 

# 设定时间范围：2026年1月10日 到 2026年2月10日 (覆盖决赛前后一个月)
# GNews 格式: (年, 月, 日)
START_DATE = (2026, 1, 10)
END_DATE = (2026, 2, 10)

TASKS = [
    # --- 1. 塞内加尔 Top 10 (Senegal Best Pages) ---
    {
        "language": "fr",
        "country": "SN",
        "label": "Senegal_Top10",
        "keywords": [
            # 塞内加尔主流媒体域名 + 关键词
            "site:seneweb.com CAN 2025 blessure",  # 伤病
            "site:wiwsport.com CAN 2025",
            "site:dakaractu.com CAN 2025",
            "site:sudquotidien.sn CAN 2025",
            "site:lequotidien.sn CAN 2025",
            "site:aps.sn CAN 2025",
            "site:igfm.sn CAN 2025",
            "site:senego.com CAN 2025",
            "site:pressafrik.com CAN 2025",
            "site:leral.net CAN 2025",
            "Sadio Mané blessure CAN 2025", # 专门搜球星伤病
        ]
    },
    # --- 2. 摩洛哥 Top 10 (Morocco Best Pages) ---
    {
        "language": "fr",
        "country": "MA",
        "label": "Morocco_Top10",
        "keywords": [
            "site:hespress.com CAN 2025",
            "site:le360.ma CAN 2025",
            "site:mapnews.ma CAN 2025",
            "site:lematin.ma CAN 2025",
            "site:yabiladi.com CAN 2025",
            "site:telquel.ma CAN 2025",
            "site:medias24.com CAN 2025",
            "site:lionsdelatlas.ma CAN 2025", # 摩洛哥足球专页
            "site:elbotola.com CAN 2025",    # 体育专页
            "Hakimi injury AFCON 2025",      # 伤病英文搜一下
            "Ziyech blessure CAN 2025",      # 伤病法语搜一下
        ]
    },
    # --- 3. 法国 Top 10 (French Best Pages) ---
    {
        "language": "fr",
        "country": "FR",
        "label": "France_Top10",
        "keywords": [
            "site:lequipe.fr CAN 2025",      # 队报 (最重要)
            "site:france24.com CAN 2025",
            "site:lemonde.fr CAN 2025",
            "site:lefigaro.fr CAN 2025",
            "site:liberation.fr CAN 2025",
            "site:bfmtv.com CAN 2025",
            "site:eurosport.fr CAN 2025",
            "site:rmcsport.bfmtv.com CAN 2025",
            "site:sofoot.com CAN 2025",
            "site:20minutes.fr CAN 2025",
        ]
    },
    # --- 4. 美国 Top 10 (USA Best Pages) ---
    {
        "language": "en",
        "country": "US",
        "label": "USA_Top10",
        "keywords": [
            "site:espn.com AFCON 2025",
            "site:cnn.com AFCON 2025",
            "site:nytimes.com AFCON 2025",
            "site:washingtonpost.com AFCON 2025",
            "site:bleacherreport.com AFCON 2025",
            "site:cbssports.com AFCON 2025",
            "site:si.com AFCON 2025",       # Sports Illustrated
            "site:foxsports.com AFCON 2025",
            "site:usatoday.com AFCON 2025",
            "site:theathletic.com AFCON 2025",
            "Pulisic AFCON reaction", # 即使没关系，搜一下美国视角
        ]
    }
]

# ================= 函数定义 (保持不变，功能最强) =================

def setup_driver():
    print(">>> 正在启动 Edge 浏览器 (用于解析真实链接)...")
    options = EdgeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    # 启用无头模式可以让后台跑得更快，如果你想看过程可以注释掉下面这行
    # options.add_argument("--headless") 
    
    service = EdgeService(executable_path="./msedgedriver.exe")
    driver = webdriver.Edge(service=service, options=options)
    return driver

def resolve_real_url(driver, google_url):
    try:
        driver.get(google_url)
        # 等待跳转
        for _ in range(10): 
            current_url = driver.current_url
            if "google.com" not in current_url and "consent.google.com" not in current_url:
                return current_url
            time.sleep(0.5)
        return driver.current_url 
    except:
        return google_url

def get_content_from_url(url):
    try:
        config = Config()
        config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        config.request_timeout = 15
        
        article = Article(url, config=config)
        article.download()
        article.parse()
        return article.text
    except:
        return None

def main():
    driver = setup_driver()
    all_data = []
    total_count = 0
    seen_titles = set()
    
    try:
        print(f">>> 🚀 启动终极爬虫: The Mother of Datas...")
        print(f">>> 锁定时间范围: {START_DATE} 至 {END_DATE}")

        for task in TASKS:
            lang = task['language']
            country = task['country']
            label = task['label']
            
            # 【关键修改】：这里加入了 start_date 和 end_date
            google_news = GNews(
                language=lang, 
                country=country, 
                max_results=MAX_RESULTS_PER_KEYWORD,
                start_date=START_DATE,
                end_date=END_DATE
            )
            
            for keyword in task['keywords']:
                print(f"\n>>> [{label}] 正在搜索: {keyword}")
                
                try:
                    results = google_news.get_news(keyword)
                    print(f"    -> 找到 {len(results)} 条线索")
                    
                    for item in results:
                        title = item.get('title')
                        if title in seen_titles: continue
                        
                        google_url = item.get('url')
                        if google_url:
                            real_url = resolve_real_url(driver, google_url)
                            if "google.com" in real_url: continue
                                
                            seen_titles.add(title)

                            full_text = get_content_from_url(real_url)
                            
                            if full_text and len(full_text) > 200: 
                                all_data.append({
                                    "source_label": label,
                                    "publisher": item.get('publisher', {}).get('title'),
                                    "date": item.get('published date'),
                                    "title": title,
                                    "full_text": full_text, 
                                    "url": real_url
                                })
                                total_count += 1
                                print(f"    ✅ [{total_count}] {label}: {title[:20]}...")
                            else:
                                pass
                except Exception as e:
                    print(f"    !!! 出错: {e}")
            
    finally:
        driver.quit()

    if all_data:
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=['full_text']) # 深度去重
        filename = "mother_of_all_data_2026.csv"
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        print("\n" + "="*60)
        print(f"🎉 任务完成！数据核弹已生成: {filename}")
        print(f"📊 总数据量: {len(df)} 篇")
        print("="*60)
    else:
        print("\n😭 没抓到数据，请检查网络或关键词。")

if __name__ == "__main__":
    main()