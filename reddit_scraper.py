import requests
import pandas as pd
import time
import json
from datetime import datetime

def scrape_reddit_timeline():
    # 1. 设置请求头（非常重要！Reddit 严查 User-Agent，必须伪装成浏览器）
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # 2. 我们先在 r/soccer 搜索这场比赛的帖子
    # 关键词：Senegal Morocco Match Thread (为了匹配 AFCON 决赛)
    search_query = "Senegal Morocco AFCON Match Thread"
    print(f"=== 正在 r/soccer 搜索: {search_query} ===")
    
    search_url = f"https://www.reddit.com/r/soccer/search.json?q={search_query}&restrict_sr=1&sort=relevance&limit=1"
    
    try:
        response = requests.get(search_url, headers=headers)
        if response.status_code != 200:
            print(f"搜索失败，状态码: {response.status_code}")
            return

        data = response.json()
        posts = data['data']['children']
        
        if not posts:
            print("未找到相关比赛贴。")
            return

        # 获取第一个搜索结果
        top_post = posts[0]['data']
        post_title = top_post['title']
        post_url = top_post['url']
        # 这里最关键：帖子正文（selftext）通常包含时间线（Timeline）！
        post_timeline_text = top_post['selftext'] 
        
        print(f" -> 找到帖子: {post_title}")
        print(f" -> 链接: {post_url}")
        
        # 3. 保存比赛的关键时间线数据 (Decisive Happenings)
        # 这对应你项目书里的 "Objective 3: assessment of decisive happenings"
        timeline_data = [{
            'type': 'Match_Timeline_Text',
            'content': post_timeline_text,
            'source_url': post_url,
            'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }]
        
        pd.DataFrame(timeline_data).to_csv('reddit_match_timeline.csv', index=False)
        print(" -> 比赛时间线文本已保存为 'reddit_match_timeline.csv'")

        # 4. 抓取下方的用户评论 (用于情感分析或第三方视角)
        print(" -> 正在抓取用户评论...")
        # 在链接后加 .json 获取评论
        comments_url = post_url + ".json"
        # 去掉最后的斜杠避免错误
        if comments_url.endswith("/.json"): comments_url = comments_url.replace("/.json", ".json")

        time.sleep(2) # 礼貌性等待
        comm_resp = requests.get(comments_url, headers=headers)
        comm_data = comm_resp.json()
        
        # Reddit JSON 返回是个列表，第0项是帖子信息，第1项是评论
        comments_list = comm_data[1]['data']['children']
        
        parsed_comments = []
        for comment in comments_list:
            if 'body' in comment['data']:
                c_data = comment['data']
                parsed_comments.append({
                    'author': c_data.get('author', 'deleted'),
                    'text': c_data.get('body', ''),
                    'score': c_data.get('score', 0), # 点赞数
                    'created_utc': datetime.fromtimestamp(c_data.get('created_utc', 0)),
                    'permalink': f"https://www.reddit.com{c_data.get('permalink', '')}"
                })
        
        # 保存评论
        if parsed_comments:
            df = pd.DataFrame(parsed_comments)
            df.to_csv('reddit_comments.csv', index=False)
            print(f" -> 成功抓取 {len(parsed_comments)} 条高赞评论，已保存为 'reddit_comments.csv'")
        else:
            print(" -> 未抓取到评论。")

    except Exception as e:
        print(f"抓取出错: {e}")

if __name__ == "__main__":
    scrape_reddit_timeline()