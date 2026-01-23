# coding=utf-8
"""
Reddit爬虫 - 第二阶段：使用Playwright爬取帖子详细内容
"""
import json
import logging
import random
import asyncio
import os
import re
import datetime
import sqlite3
from pathlib import Path
from playwright.async_api import async_playwright


def setup_logger(debug=False):
    level = logging.DEBUG if debug else logging.INFO
    format_str = '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    
    logging.basicConfig(
        level=level,
        format=format_str,
        handlers=[
            logging.FileHandler("reddit_crawler_stage2.log", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


class PostCrawler:
    """第二阶段：使用Playwright爬取帖子详细内容"""

    def __init__(self, subreddit_url, headless=False, 
                 use_system_browser='chrome', delays=None,
                 start_index=None, end_index=None):

        self.subreddit_url = subreddit_url
        self.headless = headless
        self.use_system_browser = use_system_browser
        
        # 区间爬取参数（0-based索引，包含边界）
        self.start_index = start_index  # None表示从头开始
        self.end_index = end_index      # None表示爬到末尾
        
        self.delays = delays or {
            'page_min': 2000, 'page_max': 5000,
            'action_min': 500, 'action_max': 1500,
            'scroll_min': 3000, 'scroll_max': 6000,
            'api_min': 1000, 'api_max': 2000
        }
        
        # 提取subreddit名称并创建目录
        self.subreddit_name = self._extract_subreddit_name(subreddit_url)
        self.subreddit_dir = f".\\outputs\\{self.subreddit_name}"
        Path(self.subreddit_dir).mkdir(parents=True, exist_ok=True)
        
        # URL列表文件（第一阶段生成，第二阶段只读）
        self.urls_file = os.path.join(self.subreddit_dir, f"{self.subreddit_name}_urls.json")
        
        # 结果文件路径和进度文件路径（根据区间动态设置，在load_url_list后确定）
        self.output_file = None
        self.progress_file = None
        
        # 浏览器数据目录
        self.user_data_dir = self._get_browser_data_dir()
        
        # 存储数据
        self.all_posts_data = []
        self.total_crawled_count = 0
        
        # 数据收集来源标识
        self.collect_source = "pullpush"
        
        # SQLite数据库路径（全局唯一）
        self.db_path = "./outputs/reddit_posts.sqlite"
        Path("./outputs").mkdir(parents=True, exist_ok=True)
        self._init_database()
        
        # Playwright对象
        self.playwright = None
        self.browser = None
        self.page = None

    def _init_database(self):
        """初始化SQLite数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='posts'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            # 创建新表
            cursor.execute('''
                CREATE TABLE posts (
                    index_in_list INTEGER,
                    post_id TEXT PRIMARY KEY,
                    subreddit TEXT,
                    collect_source TEXT,
                    url TEXT,
                    title TEXT,
                    body TEXT,
                    author TEXT,
                    created_time TEXT,
                    score INTEGER,
                    upvote_ratio REAL,
                    num_comments INTEGER,
                    num_crossposts INTEGER,
                    num_comments_filtered INTEGER,
                    total_awards_received INTEGER,
                    pinned INTEGER,
                    distinguished TEXT,
                    flair_text TEXT,
                    content_categories TEXT,
                    category TEXT,
                    pwls INTEGER,
                    wls INTEGER,
                    user_reports TEXT,
                    mod_reports TEXT,
                    author_patreon_flair INTEGER,
                    comments TEXT,
                    crawled_at TEXT,
                    is_valid INTEGER DEFAULT 1
                )
            ''')
            logging.info("创建新的posts表")
        else:
            # 检查并添加缺失的列
            cursor.execute("PRAGMA table_info(posts)")
            existing_columns = [col[1] for col in cursor.fetchall()]
            
            # 添加index_in_list列（如果不存在）
            if 'index_in_list' not in existing_columns:
                try:
                    cursor.execute('ALTER TABLE posts ADD COLUMN index_in_list INTEGER')
                    logging.info("添加index_in_list列到posts表")
                except sqlite3.OperationalError as e:
                    logging.warning(f"添加index_in_list列失败: {e}")
            
            # 添加is_valid列（如果不存在）
            if 'is_valid' not in existing_columns:
                try:
                    cursor.execute('ALTER TABLE posts ADD COLUMN is_valid INTEGER DEFAULT 1')
                    logging.info("添加is_valid列到posts表")
                except sqlite3.OperationalError as e:
                    logging.warning(f"添加is_valid列失败: {e}")
        
        # 创建索引以提高查询效率
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_subreddit ON posts(subreddit)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_collect_source ON posts(collect_source)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_index_in_list ON posts(index_in_list)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_is_valid ON posts(is_valid)')
        
        conn.commit()
        conn.close()
        logging.info(f"SQLite数据库初始化完成: {self.db_path}")

    def _extract_subreddit_name(self, url):
        """从Reddit URL中提取subreddit名称"""
        match = re.search(r'/r/([^/]+)', url)
        return match.group(1) if match else "unknown_subreddit"
    
    def _get_browser_data_dir(self):
        """获取浏览器数据目录"""
        if self.use_system_browser:
            data_dir = f"./browser_data_{self.use_system_browser}"
        else:
            data_dir = "./browser_data"
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        return data_dir

    async def init_browser(self):
        """初始化浏览器"""
        self.playwright = await async_playwright().start()
        
        # 确定浏览器channel
        channel = None
        if self.use_system_browser:
            if self.use_system_browser.lower() == 'chrome':
                channel = 'chrome'
            elif self.use_system_browser.lower() in ['edge', 'msedge']:
                channel = 'msedge'
        
        self.browser = await self.playwright.chromium.launch_persistent_context(
            user_data_dir=self.user_data_dir,
            headless=self.headless,
            channel=channel,
            viewport={'width': 1920, 'height': 1080},
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-first-run',
                '--disable-notifications',
                '--disable-infobars',
            ]
        )
        
        # 反自动化检测
        await self.browser.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            delete navigator.__proto__.webdriver;
        """)
        
        self.page = await self.browser.new_page()

    async def _check_and_handle_captcha_or_login(self, current_url=None):
        """检测CAPTCHA或登录验证并等待手动处理"""
        try:
            # 检测常见的CAPTCHA元素
            captcha_selectors = [
                '[data-testid="captcha"]',  # Reddit specific
                '.g-recaptcha',  # Google reCAPTCHA
                '#recaptcha',
                '[class*="captcha"]',
                '[id*="captcha"]',
                'iframe[src*="recaptcha"]',
                'iframe[src*="captcha"]',
                '[aria-label*="captcha"]',
                '[aria-label*="verification"]'
            ]
            captcha_found = False
            for selector in captcha_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    if elements:
                        # 检查元素是否可见
                        for element in elements:
                            is_visible = await element.is_visible()
                            if is_visible:
                                captcha_found = True
                                break
                    if captcha_found:
                        break
                except Exception:
                    # 如果页面上下文失效，跳过这个检测
                    continue

            # 也检查页面文本中是否包含验证以及登录相关内容
            if not captcha_found:
                # 检查页面是否包含正常的Reddit数据结构，如果有就跳过验证检测
                json_element = await self.page.query_selector("pre")
                json_string = await json_element.text_content()
                if "title" in  json_string \
                    and "author" in json_string \
                    and "selftext" in json_string:
                    return
                try:
                    page_text = await self.page.locator('body').text_content()
                    if page_text:
                        captcha_keywords = ['captcha', 'verification', 'prove you are human', 'robot check', 'login', 'sign in', '登录']
                        page_text_lower = page_text.lower()
                        for keyword in captcha_keywords:
                            if keyword in page_text_lower:
                                captcha_found = True
                                break
                except Exception:
                    pass
            
            if captcha_found:
                logging.warning("检测到CAPTCHA或登录验证！程序已暂停，请手动完成验证或登录")
                logging.warning("完成后，请在此命令行输入 'c' 然后按回车键继续程序")
                # 等待用户输入
                while True:
                    try:
                        user_input = input("请输入 'c' 继续: ").strip().lower()
                        if user_input == 'c':
                            logging.info("继续执行程序...")
                            break
                        else:
                            print("请输入 'c' 来继续程序")
                    except KeyboardInterrupt:
                        logging.info("用户中断程序")
                        raise
                    except Exception as e:
                        logging.warning(f"输入处理错误: {e}")
                        continue
                
                # 用户处理完验证后，重新导航到当前页面以恢复上下文
                if current_url:
                    try:
                        logging.info("重新加载页面以恢复上下文...")
                        await self.page.goto(current_url, wait_until='domcontentloaded', timeout=30000)
                        await self.page.wait_for_timeout(random.randint(self.delays['page_min'], self.delays['page_max']))
                    except Exception as e:
                        logging.warning(f"重新加载页面失败: {e}")
                        
        except Exception as e:
            logging.error(f"CAPTCHA检测时出错: {e}")

    async def _simulate_human_browse_a_post_behavior(self, random_rate=0.2):
        """模拟人类浏览行为 - 随机点击帖子并浏览"""
        if random.random() >= random_rate:
            return
        logging.info("触发模拟浏览行为")
        try:
            # 一次性筛选出视口内有效的链接
            links = await self.page.query_selector_all('a[href*="/comments/"]:not([data-testid*="ad"]):not([data-adtype])')
            viewport_links = []
            
            for link in links:
                try:
                    href = await link.get_attribute("href")
                    if not href or "/user/" in href:
                        continue
                    
                    # 补全相对URL并验证格式
                    if href.startswith('/'):
                        href = "https://www.reddit.com" + href
                    if not re.match(r'https://www\.reddit\.com/r/[^/]+/comments/[a-zA-Z0-9]+/[^/]+/?$', href):
                        continue
                    
                    # 检查是否在视口内且可见
                    if await link.is_visible():
                        in_viewport = await link.evaluate('''
                            element => {
                                const rect = element.getBoundingClientRect();
                                return rect.bottom > 0 && rect.top < window.innerHeight && 
                                       rect.right > 0 && rect.left < window.innerWidth;
                            }
                        ''')
                        if in_viewport:
                            viewport_links.append((link, href))
                except Exception:
                    continue
            
            if not viewport_links:
                return
            
            # 选择并点击链接
            chosen_link, href = viewport_links[-1]
            logging.info(f"选择链接进行模拟浏览{href}")
            
            if not await chosen_link.is_enabled():
                logging.info("选择的链接不可点击")
                return
            
            # 执行点击、浏览、返回流程
            await chosen_link.scroll_into_view_if_needed()
            await self.page.wait_for_timeout(self.delays['action_min'])
            await chosen_link.click(force=True)
            await self.page.wait_for_load_state('domcontentloaded', timeout=10000)
            await self.page.wait_for_timeout(random.randint(self.delays['page_min'], self.delays['page_max']))
            
            if "/comments/" not in self.page.url:
                logging.info(f"点击后未跳转到正确页面: {self.page.url}")
                return
            
            # 模拟浏览
            for _ in range(random.randint(1, 3)):
                scroll_distance = random.randint(200, 5000)
                direction = 1 if random.random() < 0.8 else -1
                await self.page.evaluate(f"window.scrollBy(0, {scroll_distance * direction});")
                await self.page.wait_for_timeout(random.randint(self.delays['action_min'], self.delays['action_max']))
            
            # 返回列表页
            logging.info("返回列表页")
            await self.page.go_back()
            await self.page.wait_for_load_state('domcontentloaded')
            await self.page.wait_for_timeout(random.randint(self.delays['page_min'], self.delays['page_max']))
            
        except Exception as e:
            logging.info(f"模拟浏览行为时出错: {e}")
            try:
                if '/comments/' in self.page.url:
                    await self.page.go_back()
                    await self.page.wait_for_load_state('domcontentloaded')
            except:
                pass

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

    def load_url_list(self):
        """加载URL列表（第一阶段生成的索引文件，只读）并设置区间"""
        if not os.path.exists(self.urls_file):
            logging.error(f"URL索引文件不存在: {self.urls_file}")
            logging.error("请先运行第一阶段收集URL")
            return None
        
        try:
            with open(self.urls_file, 'r', encoding='utf-8') as f:
                urls_data = json.load(f)
            
            if not urls_data.get('is_complete', False):
                logging.error("URL收集未完成，请先完成第一阶段")
                return None
            
            url_list = urls_data.get('collected_urls', [])
            if not url_list:
                logging.error("URL列表为空")
                return None
            
            total_count = len(url_list)
            
            # 设置区间边界（0-based索引）
            if self.start_index is None:
                self.start_index = 0
            if self.end_index is None:
                self.end_index = total_count - 1
            
            # 边界检查
            if self.start_index < 0:
                self.start_index = 0
            if self.end_index >= total_count:
                self.end_index = total_count - 1
            if self.start_index > self.end_index:
                logging.error(f"无效区间: start={self.start_index}, end={self.end_index}")
                return None
            
            # 设置区间相关的文件路径
            self._setup_range_files()
            
            logging.info(f"已加载URL索引: 共 {total_count} 个帖子，本次爬取区间 [{self.start_index}, {self.end_index}]")
            return url_list
            
        except Exception as e:
            logging.error(f"读取URL索引文件失败: {e}")
            return None
    
    def _setup_range_files(self):
        """根据区间设置结果文件和进度文件路径"""
        range_suffix = f"{self.start_index}_{self.end_index}"
        self.output_file = os.path.join(
            self.subreddit_dir, 
            f"{self.subreddit_name}_data_{range_suffix}.json"
        )
        self.progress_file = os.path.join(
            self.subreddit_dir, 
            f"{self.subreddit_name}_crawl_progress_{range_suffix}.json"
        )

    def load_crawl_progress(self):
        """加载爬取进度（当前爬到第几个）"""
        if not os.path.exists(self.progress_file):
            return self.start_index, 0  # 从区间起点开始
        
        try:
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                progress = json.load(f)
            
            # 验证进度文件的区间是否匹配
            saved_start = progress.get('range_start')
            saved_end = progress.get('range_end')
            if saved_start != self.start_index or saved_end != self.end_index:
                logging.warning(f"进度文件区间 [{saved_start}, {saved_end}] 与当前区间 [{self.start_index}, {self.end_index}] 不匹配，从区间起点开始")
                return self.start_index, 0
            
            current_index = progress.get('current_index', self.start_index)
            total_crawled = progress.get('total_crawled', 0)
            
            # 恢复时从下一个索引开始，避免重复爬取
            next_index = current_index + 1
            if next_index > self.end_index:
                next_index = self.end_index + 1  # 已完成所有爬取
            
            logging.info(f"恢复爬取进度: 上次处理到索引 {current_index}，从索引 {next_index} 继续，已爬取 {total_crawled} 条")
            return next_index, total_crawled
            
        except Exception as e:
            logging.warning(f"读取进度文件失败: {e}，从区间起点开始")
            return self.start_index, 0

    def save_crawl_progress(self, current_index):
        """保存爬取进度"""
        try:
            progress = {
                "subreddit": self.subreddit_name,
                "range_start": self.start_index,
                "range_end": self.end_index,
                "current_index": current_index,
                "total_crawled": self.total_crawled_count,
                "last_updated": datetime.datetime.now().isoformat()
            }
            self._atomic_write_json(self.progress_file, progress)
        except Exception as e:
            logging.error(f"保存进度失败: {e}")

    def save_data(self, current_index):
        """保存数据到JSON文件和SQLite数据库"""
        try:
            # 如果没有新数据需要保存，直接返回
            if not self.all_posts_data:
                return
            
            # ========== 保存到JSON文件 ==========
            # 读取现有数据
            existing_data = []
            if os.path.exists(self.output_file):
                try:
                    with open(self.output_file, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                except Exception:
                    pass
            
            # 合并数据
            all_data = existing_data + self.all_posts_data
            
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=4)
            
            # ========== 保存到SQLite数据库 ==========
            self._save_to_sqlite(self.all_posts_data)
            
            # 更新总计数器
            self.total_crawled_count += len(self.all_posts_data)
            
            logging.info(f"保存数据成功，新增 {len(self.all_posts_data)} 条，JSON文件总计 {len(all_data)} 条，本次运行已爬取 {self.total_crawled_count} 条帖子")
            
            # 清空已保存的数据，避免重复保存
            self.all_posts_data.clear()

            # 保存爬取进度
            self.save_crawl_progress(current_index)
            
        except Exception as e:
            logging.error(f"保存数据失败: {e}")

    def _save_to_sqlite(self, posts_data):
        """保存帖子数据到SQLite数据库（重复post_id时覆盖）"""
        if not posts_data:
            return
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        crawled_at = datetime.datetime.now().isoformat()
        inserted_count = 0
        updated_count = 0
        error_count = 0
        
        for post in posts_data:
            try:
                # 先检查是否存在
                post_id = post.get("post_id")
                cursor.execute('SELECT 1 FROM posts WHERE post_id = ?', (post_id,))
                exists = cursor.fetchone() is not None
                
                # 准备数据，确保所有字段都有默认值
                insert_data = (
                    post.get("index", 0),  # index_in_list
                    post_id,  # post_id
                    post.get("subreddit", self.subreddit_name),  # subreddit
                    self.collect_source,  # collect_source
                    post.get("url", ""),  # url
                    post.get("title", ""),  # title
                    post.get("body", ""),  # body
                    post.get("author", ""),  # author
                    post.get("created_time", ""),  # created_time
                    post.get("score", 0),  # score
                    post.get("upvote_ratio", 0.0),  # upvote_ratio
                    post.get("num_comments", 0),  # num_comments
                    post.get("num_crossposts", 0),  # num_crossposts
                    post.get("num_comments_filtered", 0),  # num_comments_filtered
                    post.get("total_awards_received", 0),  # total_awards_received
                    1 if post.get("pinned") else 0,  # pinned
                    post.get("distinguished", ""),  # distinguished
                    post.get("flair_text", ""),  # flair_text
                    json.dumps(post.get("content_categories", []), ensure_ascii=False),  # content_categories
                    post.get("category", ""),  # category
                    post.get("pwls", -1),  # pwls
                    post.get("wls", -1),  # wls
                    json.dumps(post.get("user_reports", []), ensure_ascii=False),  # user_reports
                    json.dumps(post.get("mod_reports", []), ensure_ascii=False),  # mod_reports
                    post.get("author_patreon_flair", 0),  # author_patreon_flair
                    json.dumps(post.get("comments", []), ensure_ascii=False),  # comments
                    crawled_at,  # crawled_at
                    1 if post.get("is_valid", True) else 0  # is_valid
                )
                
                cursor.execute('''
                    INSERT OR REPLACE INTO posts (
                        index_in_list, post_id, subreddit, collect_source, url, title, body, author,
                        created_time, score, upvote_ratio, num_comments, num_crossposts,
                        num_comments_filtered, total_awards_received, pinned, distinguished, flair_text,
                        content_categories, category, pwls, wls, user_reports,
                        mod_reports, author_patreon_flair, comments, crawled_at, is_valid
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', insert_data)
                
                if exists:
                    updated_count += 1
                else:
                    inserted_count += 1
                    
            except Exception as e:
                error_count += 1
                logging.error(f"保存帖子到SQLite失败 (post_id={post.get('post_id', 'Unknown')}): {e}")
                logging.debug(f"失败的帖子数据: {post}")
        
        try:
            conn.commit()
        except Exception as e:
            logging.error(f"SQLite事务提交失败: {e}")
            conn.rollback()
        finally:
            conn.close()
        
        # 输出统计信息
        if error_count > 0:
            logging.warning(f"SQLite: 新增 {inserted_count} 条，更新 {updated_count} 条，失败 {error_count} 条")
        elif updated_count > 0:
            logging.info(f"SQLite: 新增 {inserted_count} 条，更新 {updated_count} 条已存在记录")
        else:
            logging.info(f"SQLite: 新增 {inserted_count} 条")

    async def fetch_post_json(self, post_url, url_index):
        """获取单个帖子的JSON数据"""
        try:
            # 构造JSON API URL
            base_url = post_url.split('?')[0]
            json_url = base_url.rstrip('/') + ".json"
            
            await self.page.goto(json_url, wait_until='domcontentloaded', timeout=15000)
            await self.page.wait_for_timeout(random.randint(self.delays['api_min'], self.delays['api_max']))
            
            # 检查CAPTCHA或登录验证
            await self._check_and_handle_captcha_or_login(json_url)
            
            # 获取JSON内容
            pre_element = await self.page.query_selector("pre")
            if not pre_element:
                return None
                
            json_content = await pre_element.text_content()
            if not json_content:
                return None
            
            # 解析JSON
            raw_data = json.loads(json_content)
            post_info = raw_data[0]['data']['children'][0]['data']
            comments_tree = raw_data[1]['data']['children']
            
            # 提取帖子数据
            post_data = {
                "index": url_index,
                "post_id": self._extract_post_id(post_url),
                "url": post_url,
                "subreddit": post_info.get("subreddit", ""),
                "title": post_info.get("title", "N/A"),
                "body": post_info.get("selftext", ""),
                "author": post_info.get("author", "[deleted]"),
                "created_time": self._convert_time(post_info.get("created_utc", 0)),
                "score": post_info.get("score", 0),
                "upvote_ratio": post_info.get("upvote_ratio", 0.0),
                "num_comments": post_info.get("num_comments", 0),
                "num_comments_filtered": 0,  # 稍后计算
                "num_crossposts": post_info.get("num_crossposts", 0),
                "total_awards_received": post_info.get("total_awards_received", 0),
                "pinned": post_info.get("pinned", False),
                "distinguished": post_info.get("distinguished", None),
                "flair_text": post_info.get("link_flair_text", ""),
                "content_categories": post_info.get("content_categories", []),
                "category": post_info.get("category", ""),
                "pwls": post_info.get("pwls", -1),
                "wls": post_info.get("wls", -1),
                "user_reports": post_info.get("user_reports", []),
                "mod_reports": post_info.get("mod_reports", []),
                "author_patreon_flair": post_info.get("author_patreon_flair", 0),
                "comments": [],
                "is_valid": self._is_post_valid(post_info.get("title", ""))
            }
            
            # 解析评论
            for comment_node in comments_tree:
                parsed_comment = self._parse_comment(comment_node)
                if parsed_comment:
                    post_data["comments"].append(parsed_comment)
            
            # 计算过滤后的评论总数（包括所有层级的回复）
            post_data["num_comments_filtered"] = self._count_comments(post_data["comments"])
            
            return post_data
            
        except Exception as e:
            logging.error(f"获取帖子JSON失败: {e}")
            return None

    def _count_comments(self, comments):
        """递归统计评论总数（包括所有层级的回复）"""
        count = 0
        for comment in comments:
            count += 1  # 当前评论
            if comment.get("replies"):
                count += self._count_comments(comment["replies"])
        return count

    def _is_bot_or_mod_comment_or_deleted(self, author, body):
        """判断是否为版主/机器人评论或已删除评论"""
        # 检查评论人是否为 AutoModerator
        if author == "AutoModerator" or author == "[deleted]":
            return True
        
        # 检查评论内容是否包含机器人/版主特征, 或者删除
        body_lower = body.lower() if body else ""
        if "i am a bot" in body_lower or "moderator" in body_lower or "[deleted]" in body_lower:
            return True
        
        return False

    def _parse_comment(self, comment_data):
        """解析评论数据"""
        if comment_data.get('kind') == 'more':
            return None

        data = comment_data.get('data', {})
        
        # 过滤版主/机器人评论
        author = data.get("author", "[deleted]")
        body = data.get("body", "")
        if self._is_bot_or_mod_comment_or_deleted(author, body):
            return None
        
        utc_timestamp = data.get("created_utc", 0)
        
        parsed = {
            "author": author,
            "text": body if body else "[无文本]",
            "score": data.get("score", 0),
            "created_time": self._convert_time(utc_timestamp),
            "replies": [],
            "reply_count": 0
        }

        # 递归处理回复
        replies_raw = data.get("replies")
        if isinstance(replies_raw, dict):
            children = replies_raw.get('data', {}).get('children', [])
            for child in children:
                child_parsed = self._parse_comment(child)
                if child_parsed:
                    parsed["replies"].append(child_parsed)

        parsed["reply_count"] = len(parsed["replies"])
        return parsed

    def _convert_time(self, timestamp):
        """转换时间戳为可读格式"""
        if not timestamp:
            return "N/A"
        try:
            return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        except:
            return "N/A"

    def _extract_post_id(self, url):
        """提取帖子ID"""
        match = re.search(r'/comments/([a-zA-Z0-9]+)/', url)
        return match.group(1) if match else None
    
    def _is_post_valid(self, title):
        """检查帖子是否有效（未被删除或移除）"""
        if not title:
            return False
        
        title_lower = title.lower()
        invalid_keywords = ["deleted by", "removed by"]
        
        for keyword in invalid_keywords:
            if keyword in title_lower:
                return False
        
        return True

    async def crawl_posts(self):
        """主要爬取流程"""
        completed_normally = False
        current_index = 1
        
        try:
            # 加载URL索引（只读）
            url_list = self.load_url_list()
            if not url_list:
                return
            
            # 加载爬取进度
            current_index, self.total_crawled_count = self.load_crawl_progress()
            
            # 计算区间内的帖子数量
            range_count = self.end_index - self.start_index + 1
            
            # 检查进度是否超出区间范围
            if current_index > self.end_index:
                logging.info(f"区间 [{self.start_index}, {self.end_index}] 爬取已完成，如需重新爬取请删除进度文件")
                return
            
            # 初始化浏览器
            await self.init_browser()
            
            logging.info(f"开始爬取区间 [{self.start_index}, {self.end_index}] 共 {range_count} 个帖子，从索引 {current_index} 开始")
            
            # 遍历帖子获取详细数据
            consecutive_failures = 0
            
            for index in range(current_index, self.end_index + 1):
                url_item = url_list[index]
                url = url_item["url"]
                current_index = index
                
                logging.info(f"[{current_index}/{self.end_index}] (区间{self.start_index}-{self.end_index}) 处理: {url}")
                                
                try:
                    post_data = await self.fetch_post_json(url, index)
                    
                    if post_data:
                        self.all_posts_data.append(post_data)
                        consecutive_failures = 0
                        # 每10个帖子保存一次数据，传递当前索引（保存已处理的索引）
                        if len(self.all_posts_data) % 10 == 0:
                            self.save_data(current_index)
                    else:
                        consecutive_failures += 1
                        if consecutive_failures >= 3:
                            logging.error("连续失败过多，停止爬取")
                            break
                    
                    # 延迟
                    await self.page.wait_for_timeout(random.randint(self.delays['action_min'], self.delays['action_max']))
                    
                except Exception as e:
                    consecutive_failures += 1
                    logging.error(f"处理帖子出错: {e}")
                    
                    if consecutive_failures >= 3:
                        break
                    
                    await self.page.wait_for_timeout(random.randint(self.delays['action_min'], self.delays['action_max']))
            
            # 检查是否正常完成
            if current_index >= self.end_index:
                completed_normally = True
                logging.info(f"区间 [{self.start_index}, {self.end_index}] 所有帖子处理完成")
            
        except KeyboardInterrupt:
            logging.info("用户中断，进度已保存")
        except Exception as e:
            logging.error(f"爬取过程出错: {e}")
        finally:
            # 保存最终数据
            if self.all_posts_data:
                self.save_data(current_index)
            
            # 显示最终统计
            logging.info(f"爬取结束，本次运行总共爬取了 {self.total_crawled_count} 条帖子")
            
            # 完成后保留进度文件，方便查看爬取状态
            if completed_normally:
                logging.info("任务完成，进度文件已保留")
            
            await self.cleanup()

    async def cleanup(self):
        """清理资源"""
        try:
            if self.page:
                await self.page.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
        except Exception:
            pass


async def main():
    """主函数"""
    setup_logger()
    
    # 配置参数
    target_url = "https://www.reddit.com/r/dogs/"
    headless = False
    use_system_browser = 'chrome'  # 'chrome', 'edge', 或 None
    
    # 区间爬取参数（0-based索引，包含边界）
    # 设置为None表示不限制，爬取全部
    # 例如: start_index=0, end_index=99 表示爬取第1到第100个帖子
    start_index = None  # 起始位置，None表示从第1个开始
    end_index = None    # 结束位置，None表示爬到最后
    
    crawler = PostCrawler(
        subreddit_url=target_url,
        headless=headless,
        use_system_browser=use_system_browser,
        start_index=start_index,
        end_index=end_index,
        delays={
            'page_min': 3000, 'page_max': 5000,
            'action_min': 3000, 'action_max': 8000,
            'scroll_min': 5000, 'scroll_max': 10000,
            'api_min': 2000, 'api_max': 4000
        }
    )
    
    await crawler.crawl_posts()
    logging.info("Reddit爬取完成！")


if __name__ == "__main__":
    asyncio.run(main())
