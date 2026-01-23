# coding=utf-8
"""
Reddit爬虫 - 第一阶段：使用PullPush API收集帖子URL
"""
import json
import logging
import os
import re
import random
import requests
import time
import datetime
from pathlib import Path


def setup_logger(debug=False):
    level = logging.DEBUG if debug else logging.INFO
    format_str = '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    
    logging.basicConfig(
        level=level,
        format=format_str,
        handlers=[
            logging.FileHandler("reddit_crawler_stage1.log", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


class URLCollector:
    """第一阶段：收集Reddit帖子URL"""

    def __init__(self, subreddit_url, max_posts=100, before_timestamp=None, api_delay=None, api_page_size=100):
        self.subreddit_url = subreddit_url
        self.max_posts = max_posts
        self.before_timestamp = before_timestamp or int(time.time())
        self.api_delay = api_delay or {'min': 2, 'max': 5}
        self.default_page_size = api_page_size

        # 提取subreddit名称并创建目录
        self.subreddit_name = self._extract_subreddit_name(subreddit_url)
        self.subreddit_dir = f".\\outputs\\{self.subreddit_name}"
        Path(self.subreddit_dir).mkdir(parents=True, exist_ok=True)
        
        # URL收集状态文件路径
        self.state_file = os.path.join(self.subreddit_dir, f"{self.subreddit_name}_urls.json")

    def _extract_subreddit_name(self, url):
        """从Reddit URL中提取subreddit名称"""
        match = re.search(r'/r/([^/]+)', url)
        return match.group(1) if match else "unknown_subreddit"

    def _extract_post_id(self, url):
        """提取帖子ID"""
        match = re.search(r'/comments/([a-zA-Z0-9]+)/', url)
        return match.group(1) if match else None

    def _atomic_write_json(self, file_path, data, indent=2):
        """原子写入JSON文件，避免中断时文件被截断"""
        temp_file = file_path + ".tmp"
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=indent)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_file, file_path)
        except Exception as e:
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass
            raise e

    def save_progress(self, collected_urls, before_timestamp, scanned_post_ids):
        """保存URL收集进度"""
        try:
            is_complete = len(collected_urls) >= self.max_posts
            
            state_data = {
                "subreddit_name": self.subreddit_name,
                "max_posts": self.max_posts,
                "total_collected": len(collected_urls),
                "is_complete": is_complete,
                "before_timestamp": before_timestamp,
                "collected_urls": collected_urls,
                "scanned_post_ids": list(scanned_post_ids),
                "last_updated": datetime.datetime.now().isoformat()
            }
            
            self._atomic_write_json(self.state_file, state_data)
            
            status = "完成" if is_complete else "进行中"
            logging.info(f"URL收集进度已保存: {len(collected_urls)}/{self.max_posts} ({status})")
            
        except Exception as e:
            logging.error(f"保存进度失败: {e}")

    def load_progress(self) -> tuple:
        """加载URL收集进度"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state_data = json.load(f)
                
                collected_urls = state_data.get('collected_urls', [])
                before_timestamp = state_data.get('before_timestamp', self.before_timestamp)
                scanned_post_ids = set(state_data.get('scanned_post_ids', []))
                is_complete = state_data.get('is_complete', False)
                
                if is_complete:
                    logging.info(f"URL收集已完成: {len(collected_urls)} 个帖子")
                else:
                    logging.info(f"恢复URL收集进度: {len(collected_urls)}/{self.max_posts}")
                
                return collected_urls, before_timestamp, scanned_post_ids, is_complete
                
        except Exception as e:
            logging.warning(f"加载进度失败: {e}")
        
        return [], self.before_timestamp, set(), False

    def _initialize_collection_state(self, collected_urls, before_timestamp, scanned_post_ids):
        """初始化URL收集状态"""
        # 如果没有已扫描的ID，从已有URL中提取
        if not scanned_post_ids and collected_urls:
            for url_data in collected_urls:
                post_id = self._extract_post_id(url_data.get('url', ''))
                if post_id:
                    scanned_post_ids.add(post_id)
            logging.info(f"从URL列表提取了 {len(scanned_post_ids)} 个帖子ID")
        
        if collected_urls:
            logging.info(f"继续收集: 已有 {len(collected_urls)} 个 URL，"
                        f"从时间戳 {before_timestamp} 继续")
        
        return collected_urls, scanned_post_ids, before_timestamp

    def _is_deleted_post(self, post):
        """检查帖子是否已被删除"""
        # 检查作者是否已删除
        author = post.get("author", "")
        if author in ("[deleted]", "[removed]"):
            return True
        
        # 检查是否被版主/管理员删除
        removed_by = post.get("removed_by_category")
        if removed_by:  # 如果有值，说明被删除了
            return True
        
        # 检查帖子内容是否被删除（对于 self post）
        selftext = post.get("selftext", "")
        if selftext in ("[deleted]", "[removed]"):
            return True
        
        return False

    def _process_api_response(self, new_posts, collected_urls, scanned_post_ids, max_posts):
        """处理API响应数据"""
        new_count = 0
        duplicate_count = 0
        deleted_count = 0
        
        for post in new_posts:
            if len(collected_urls) >= max_posts:
                break
            
            post_id = post.get("id")
            permalink = post.get("permalink")
            created_utc = post.get("created_utc")
            
            if not (post_id and permalink and created_utc):
                continue
                
            if post_id in scanned_post_ids:
                duplicate_count += 1
                continue
            
            # 过滤已删除的帖子
            if self._is_deleted_post(post):
                deleted_count += 1
                scanned_post_ids.add(post_id)  # 记录ID避免重复检查
                continue
            
            post_url = f"https://www.reddit.com{permalink}"
            collected_urls.append({
                "url": post_url,
                "created_utc": created_utc
            })
            scanned_post_ids.add(post_id)
            new_count += 1
        
        return new_count, duplicate_count, deleted_count

    def collect_post_urls(self, existing_urls=None, before_timestamp=None, existing_scanned_ids=None):
        """收集帖子URL - 使用Pullpush API，支持断点续爬"""
        target_url = self.subreddit_url
        
        if "/comments/" in target_url:
            return [{"url": target_url}]
        
        # 提取subreddit名称
        subreddit_name = self._extract_subreddit_name(target_url)
        if not subreddit_name or subreddit_name == "unknown_subreddit":
            logging.error(f"无法从URL提取subreddit名称: {target_url}")
            return []
        
        # 初始化收集状态
        collected_urls = existing_urls or []
        scanned_post_ids = existing_scanned_ids or set()
        before = before_timestamp or self.before_timestamp
        
        collected_urls, scanned_post_ids, before = self._initialize_collection_state(
            collected_urls, before, scanned_post_ids)
        
        if not existing_urls:
            logging.info(f"开始收集 r/{subreddit_name} 的帖子，目标数量: {self.max_posts}")
        
        consecutive_errors = 0
        consecutive_zero_new = 0  # 连续获取0个新URL的次数
        last_save_count = len(collected_urls)
        
        try:
            while len(collected_urls) < self.max_posts:
                try:
                    # 构造并发送API请求
                    api_url = "https://api.pullpush.io/reddit/search/submission/"
                    params = {
                        "subreddit": subreddit_name,
                        "size": min(self.default_page_size, self.max_posts - len(collected_urls) + 1),
                        "sort": "desc",
                        "sort_type": "created_utc"
                    }
                    if before:
                        params["before"] = before
                    
                    logging.info(f"请求API: {len(collected_urls)}/{self.max_posts} 个帖子")
                    response = requests.get(api_url, params=params, timeout=30)
                    
                    if response.status_code != 200:
                        logging.error(f"API请求失败: HTTP {response.status_code}")
                        consecutive_errors += 1
                        if consecutive_errors >= 3:
                            logging.error("连续请求失败，停止收集")
                            break
                        time.sleep(5)
                        continue
                    
                    data = response.json()
                    new_posts = data.get("data", [])
                    
                    if not new_posts:
                        logging.info("无更多数据，URL收集完成")
                        break
                    
                    # 处理新获取的帖子
                    new_count, duplicate_count, deleted_count = self._process_api_response(
                        new_posts, collected_urls, scanned_post_ids, self.max_posts)
                    
                    # 检测连续多次获取0个新URL且0个被删除的帖子，说明全是重复数据
                    if new_count == 0 and deleted_count == 0:
                        consecutive_zero_new += 1
                        if consecutive_zero_new >= 3:
                            logging.info("连续多次未获取到新URL，可能已无更多数据，URL收集完成")
                            break
                    else:
                        consecutive_zero_new = 0  # 重置计数器
                    
                    # 更新时间戳用于下一页
                    if new_posts:
                        before = new_posts[-1]["created_utc"]
                    
                    consecutive_errors = 0  # 成功请求，重置错误计数
                    logging.info(f"获取 {new_count} 个新URL，跳过 {duplicate_count} 个重复，过滤 {deleted_count} 个已删除，总计 {len(collected_urls)}")
                    
                    # 定期保存进度
                    if (len(collected_urls) - last_save_count >= 50 or 
                        len(collected_urls) % 250 == 0):
                        self.save_progress(collected_urls, before, scanned_post_ids)
                        last_save_count = len(collected_urls)
                    
                    # 随机延迟避免被封
                    delay = random.uniform(self.api_delay['min'], self.api_delay['max'])
                    logging.debug(f"等待 {delay:.1f} 秒后继续...")
                    time.sleep(delay)
                    
                except (requests.exceptions.Timeout, 
                        requests.exceptions.RequestException, 
                        json.JSONDecodeError) as e:
                    consecutive_errors += 1
                    logging.error(f"请求错误: {e}")
                    time.sleep(3)
                    if consecutive_errors >= 5:
                        logging.error("连续错误过多，保存进度并退出")
                        break
                    continue
                except KeyboardInterrupt:
                    logging.info("用户中断，保存进度...")
                    raise
                except Exception as e:
                    consecutive_errors += 1
                    logging.error(f"收集URL时出错: {e}")
                    if consecutive_errors >= 3:
                        break
                    time.sleep(2)
        
        except KeyboardInterrupt:
            logging.info("收集被中断，保存进度...")
            self.save_progress(collected_urls, before, scanned_post_ids)
            raise
        
        # 最终保存进度
        if len(collected_urls) != last_save_count or len(collected_urls) >= self.max_posts:
            self.save_progress(collected_urls, before, scanned_post_ids)
            status = "完成" if len(collected_urls) >= self.max_posts else "未完成"
            logging.info(f"URL收集{status}: {len(collected_urls)}/{self.max_posts}")
        
        return collected_urls[:self.max_posts]

    def run(self):
        """运行URL收集"""
        try:
            # 加载进度
            collected_urls, before_timestamp, scanned_post_ids, is_complete = self.load_progress()
            
            if is_complete:
                logging.info(f"URL收集已完成，共 {len(collected_urls)} 个帖子")
                logging.info("如需重新收集，请删除状态文件后重试")
                return collected_urls
            
            # 继续或开始收集
            url_list = self.collect_post_urls(collected_urls, before_timestamp, scanned_post_ids)
            
            if not url_list:
                logging.error("未收集到任何帖子链接")
                return []
            
            logging.info(f"URL收集完成，共 {len(url_list)} 个帖子")
            return url_list
            
        except KeyboardInterrupt:
            logging.info("用户中断，进度已保存")
            return []
        except Exception as e:
            logging.error(f"收集过程出错: {e}")
            return []


def main():
    """主函数"""
    setup_logger()
    
    # 配置参数
    target_url = "https://www.reddit.com/r/dogs/"
    max_posts = 50000
    
    collector = URLCollector(
        subreddit_url=target_url,
        max_posts=max_posts,
        before_timestamp=int(datetime.datetime(2026, 1, 22).timestamp()),
        api_delay={'min': 1, 'max': 5},
        api_page_size=100
    )
    
    url_list = collector.run()


if __name__ == "__main__":
    main()
