import pandas as pd
from gnews import GNews
from newspaper import Article, Config
import time
import random
# 引入 Selenium 组件
from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions

# ================= 配置区域 =================
# 目标：凑够 1000 条
MAX_RESULTS_PER_KEYWORD = 50 

TASKS = [
    # --- 法语区 ---
    {
        "language": "fr",
        "country": "FR",
        "label": "French_Source",
        "keywords": [
            "CAN 2025 polémique",   
            "CAN 2025 Maroc Sénégal",
            "site:france24.com CAN 2025", 
            "site:bfmtv.com CAN 2025",
            "site:francetvinfo.fr CAN 2025",
            "site:rmcsport.bfmtv.com CAN 2025",
        ]
    },
    # --- 英语区 ---
    {
        "language": "en",
        "country": "US",
        "label": "English_Source",
        "keywords": [
            "AFCON 2025 controversy",
            "AFCON 2025 Morocco Senegal",
            "AFCON 2025 corruption",
            "site:bbc.com AFCON 2025",
        ]
    }
]

# ================= 函数定义 =================

def setup_driver():
    """启动 Edge 浏览器"""
    print(">>> 正在启动浏览器 (用于解析真实链接)...")
    options = EdgeOptions()
    # 启用无头模式 (不显示界面)，跑得快一点。如果你想看过程，可以把下面这行注释掉
    # options.add_argument("--headless") 
    options.add_argument("--disable-blink-features=AutomationControlled")
    # 指向你之前下载的 msedgedriver.exe
    service = EdgeService(executable_path="./msedgedriver.exe")
    driver = webdriver.Edge(service=service, options=options)
    return driver

def resolve_real_url(driver, google_url):
    """
    用浏览器访问 Google 链接，等待它跳转到真实的新闻网站
    """
    try:
        driver.get(google_url)
        # 等待跳转，最多等 5 秒
        for _ in range(10): 
            current_url = driver.current_url
            # 如果链接里已经没有 "google.com" 了，说明跳转成功！
            if "google.com" not in current_url:
                return current_url
            time.sleep(0.5)
        return driver.current_url # 超时了，就返回当前的吧
    except:
        return google_url

def get_content_from_url(url):
    """使用 newspaper3k 下载正文"""
    try:
        config = Config()
        config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ...'
        config.request_timeout = 10
        
        article = Article(url, config=config)
        article.download()
        article.parse()
        return article.text, article.top_image
    except:
        return None, None

def main():
    # 1. 先启动浏览器
    driver = setup_driver()
    
    all_data = []
    total_count = 0
    
    try:
        print(">>> 🚀 加强版爬虫启动 (Selenium + Newspaper3k)...")

        for task in TASKS:
            lang = task['language']
            country = task['country']
            label = task['label']
            
            # 初始化 GNews
            google_news = GNews(language=lang, country=country, max_results=MAX_RESULTS_PER_KEYWORD)
            
            for keyword in task['keywords']:
                print(f"\n>>> [{label}] 搜索: {keyword}")
                
                try:
                    # 1. 获取 Google 列表
                    results = google_news.get_news(keyword)
                    print(f"    -> 找到 {len(results)} 条线索")
                    
                    for item in results:
                        google_url = item.get('url')
                        title = item.get('title')
                        
                        if google_url:
                            # === 关键步骤：用浏览器把 Google 链接变成真实链接 ===
                            real_url = resolve_real_url(driver, google_url)
                            
                            # 如果还是 google 的链接，可能就是失败了，跳过
                            if "google.com" in real_url:
                                continue

                            # === 下载正文 ===
                            full_text, image_url = get_content_from_url(real_url)
                            
                            # 过滤掉内容太短的 (通常是 cookie 提示或报错)
                            if full_text and len(full_text) > 200: 
                                all_data.append({
                                    "source_label": label,
                                    "publisher": item.get('publisher', {}).get('title'),
                                    "date": item.get('published date'),
                                    "title": title,
                                    "full_text": full_text, # 这次会是真正的文章！
                                    "real_url": real_url,
                                    "google_url": google_url
                                })
                                total_count += 1
                                print(f"    ✅ [{total_count}] 成功: {item.get('publisher', {}).get('title')} - {title[:15]}...")
                            else:
                                # print(f"    ⚠️  内容无效或太短")
                                pass
                                
                except Exception as e:
                    print(f"    !!! 错误: {e}")

    finally:
        # 无论如何，最后要关闭浏览器
        driver.quit()

    # ================= 保存 =================
    if all_data:
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=['title'])
        filename = "real_full_news_dataset.csv"
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        
        print("\n" + "="*50)
        print(f"🎉 成功抓取 {len(df)} 条【真实】新闻正文！")
        print(f"💾 结果已保存为: {filename}")
        print("="*50)
    else:
        print("\n😭 未抓取到数据。")

if __name__ == "__main__":
    main()