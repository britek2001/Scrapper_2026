from GoogleNews import GoogleNews
import pandas as pd

def scrape_afcon_news():

    googlenews = GoogleNews(lang='en', region='US') # 也可以改成 lang='fr', region='FR'
    
    keywords = ["AFCON 2025 controversy", "AFCON 2025 referee", "AFCON 2025 scandal"]
    
    all_news = []
    print("=== 开始抓取 Google News ===")

    for key in keywords:
        print(f"正在搜索: {key} ...")
        googlenews.clear()
        googlenews.search(key)
        
        # 获取结果
        results = googlenews.result()
        
        if results:
            print(f" -> 找到 {len(results)} 条新闻")
            for item in results:
                data = {
                    'keyword': key,
                    'title': item['title'],
                    'date': item['date'],
                    'media': item['media'],
                    'desc': item['desc'],
                    'link': item['link']
                }
                all_news.append(data)
        else:
            print(" -> 未找到新闻")

    # 保存
    if all_news:
        df = pd.DataFrame(all_news)
        filename = 'afcon_news_context.csv'
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\n成功！数据已保存为 {filename}")
    else:
        print("\n很遗憾，没抓到数据。")

if __name__ == "__main__":
    scrape_afcon_news()