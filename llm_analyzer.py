# coding=utf-8
"""
LLM分析器 - 用于分析Reddit帖子数据
场景一：从数据库读取帖子并分析
场景二：直接分析爬虫获取的帖子数据
"""
import json
import logging
import sqlite3
import time
import asyncio
import openai
import yaml
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path


# ==================== 全局常量：从配置文件加载 ====================
def _load_llm_config() -> Dict[str, str]:
    """从 llm_config.yaml 加载配置"""
    config_file = Path(__file__).parent / "llm_config.yaml"
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            return {
                "system_prompt": config.get("system_prompt", ""),
                "first_message": config.get("first_message", ""),
                "api_key": config.get("api_key", "")
            }
    except FileNotFoundError:
        logging.warning(f"配置文件不存在: {config_file}，使用默认空配置")
        return {"system_prompt": "", "first_message": "", "api_key": ""}
    except Exception as e:
        logging.error(f"读取配置文件失败: {e}")
        return {"system_prompt": "", "first_message": "", "api_key": ""}

_LLM_CONFIG = _load_llm_config()
SYSTEM_PROMPT = _LLM_CONFIG["system_prompt"]
USER_MESSAGE_TEMPLATE = _LLM_CONFIG["first_message"]
DEFAULT_API_KEY = _LLM_CONFIG["api_key"]


class LLMAnalyzer:
    """LLM分析器，用于分析Reddit帖子数据"""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "deepseek-chat",
                 base_url: Optional[str] = None,
                 db_path: str = "./outputs/reddit_posts.sqlite",
                 max_retries: int = 3,
                 max_concurrent: int = 10):
        # 如果未提供 api_key，从配置文件加载
        if api_key is None:
            api_key = DEFAULT_API_KEY
        
        self.config = {
            "api_key": api_key,
            "model": model,
            "base_url": base_url
        }
        self.db_path = db_path
        self.max_retries = max_retries
        self.max_concurrent = max_concurrent
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            handlers=[
                logging.FileHandler("llm_analyzer.log", encoding="utf-8"),
                logging.StreamHandler()
            ]
        )
        logging.info(f"LLM分析器初始化: model={model}")
    
    def get_posts_from_db(self, subreddit: Optional[str] = None, 
                         post_ids: Optional[List[str]] = None,
                         index_range: Optional[Tuple[int, int]] = None,
                         include_been_analyzed: bool = False) -> List[Dict]:
        """从数据库获取帖子"""
        if not Path(self.db_path).exists():
            logging.error(f"数据库文件不存在: {self.db_path}")
            return []
        
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            if post_ids:
                placeholders = ','.join('?' * len(post_ids))
                query = f"SELECT * FROM posts WHERE post_id IN ({placeholders}) AND is_valid = 1 {'' if include_been_analyzed else 'AND llm_analyze_result is NULL'} ORDER BY index_in_list ASC"
                cursor.execute(query, post_ids)
            elif index_range:
                start_idx, end_idx = index_range
                query = f"SELECT * FROM posts WHERE is_valid = 1 AND index_in_list BETWEEN ? AND ? {'' if include_been_analyzed else 'AND llm_analyze_result is NULL'} ORDER BY index_in_list ASC"
                cursor.execute(query, (start_idx, end_idx))
            elif subreddit:
                query = f"SELECT * FROM posts WHERE subreddit = ? AND is_valid = 1 {'' if include_been_analyzed else 'AND llm_analyze_result is NULL'} ORDER BY index_in_list ASC"
                cursor.execute(query, (subreddit,))
            else:
                logging.error("必须提供subreddit或post_ids参数")
                return []
            
            rows = cursor.fetchall()
            posts = [dict(row) for row in rows]
            return posts
            
        except Exception as e:
            logging.error(f"从数据库获取帖子失败: {e}")
            return []
        finally:
            conn.close()
    
    def _build_user_message(self, post: Dict) -> str:
        """
        构造 User Message（符合 DeepSeek Prompt Caching 要求）
        严格遵循 System-User 分离：所有动态数据只出现在 User Message
        """
        # 提取前10个根评论，并清理不必要的字段
        comments = []
        comments_data = post.get("comments", "")
        if comments_data:
            try:
                comments_list = json.loads(comments_data) if isinstance(comments_data, str) else comments_data
                if isinstance(comments_list, list):
                    comments = [self._clean_comment(c) for c in comments_list[:10]]
            except (json.JSONDecodeError, TypeError):
                pass
        
        # 构造符合 System Prompt 中定义的 Input Data Schema 的帖子数据
        post_data = {
            "title": post.get("title", ""),
            "selftext": post.get("body", ""),  # 对应 System Prompt 中的 selftext
            "top_comments": comments,
            "flair_text": post.get("flair_text", ""),
            "created_time": post.get("created_time", ""),
            "score": post.get("score", 0)
        }
        
        # 序列化为 JSON 字符串
        json_string = json.dumps(post_data, ensure_ascii=False, indent=2)
        
        # 使用全局常量模板
        user_message = USER_MESSAGE_TEMPLATE.format(post_json=json_string)
        
        return user_message
    
    def _clean_comment(self, comment: Dict) -> Dict:
        """层层递归，清理评论树数据，移除不必要的字段"""
        cleaned = {
            "text": comment.get("text", comment.get("body", "")),
            "score": comment.get("score", 0)
        }
        
        # 递归处理子评论
        if "replies" in comment and isinstance(comment["replies"], list):
            cleaned["replies"] = [self._clean_comment(reply) for reply in comment["replies"]]
        
        return cleaned
    
    def _save_result_to_db(self, post_id: str, result_data: Dict) -> bool:
        """将LLM分析结果保存到数据库"""
        if not Path(self.db_path).exists():
            logging.error(f"数据库文件不存在: {self.db_path}")
            return False
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 将结果序列化为JSON字符串
            result_json = json.dumps(result_data, ensure_ascii=False)
            
            # 更新数据库记录
            cursor.execute(
                "UPDATE posts SET llm_analyze_result = ? WHERE post_id = ?",
                (result_json, post_id)
            )
            
            conn.commit()
            affected_rows = cursor.rowcount
            conn.close()
            
            if affected_rows > 0:
                logging.info(f"帖子 {post_id} 分析结果已保存到数据库")
                return True
            else:
                logging.warning(f"帖子 {post_id} 未找到对应记录，无法保存结果")
                return False
                
        except Exception as e:
            logging.error(f"保存分析结果到数据库失败: {e}")
            return False
    
    def _call_sdk(self, messages: List[Dict], 
                  max_tokens: int, temperature: float) -> Tuple[bool, str, float, str]:
        """使用OpenAI SDK调用"""
        try:
            client = openai.OpenAI(
                api_key=self.config['api_key'],
                base_url=self.config.get('base_url')
            )
            
            start_time = time.time()
            response = client.chat.completions.create(
                model=self.config['model'],
                messages=messages,  # type: ignore
                max_tokens=max_tokens,
                temperature=temperature
            )
            elapsed_time = time.time() - start_time
            
            content = response.choices[0].message.content or ""

            logging.info(f"SDK调用成功，耗时: {elapsed_time:.3f}s")
            return True, content, elapsed_time, ""
            
        except Exception as e:
            error_msg = str(e)
            logging.error(f"SDK调用失败: {error_msg}")
            return False, "", 0, error_msg
    
    async def _call_sdk_async(self, messages: List[Dict], 
                              max_tokens: int, temperature: float) -> Tuple[bool, str, float, str]:
        """异步版本：使用AsyncOpenAI SDK调用"""
        try:
            client = openai.AsyncOpenAI(
                api_key=self.config['api_key'],
                base_url=self.config.get('base_url')
            )
            
            start_time = time.time()
            response = await client.chat.completions.create(
                model=self.config['model'],
                messages=messages,  # type: ignore
                max_tokens=max_tokens,
                temperature=temperature
            )
            elapsed_time = time.time() - start_time
            
            content = response.choices[0].message.content or ""
            logging.info(f"异步SDK调用成功，耗时: {elapsed_time:.3f}s")
            return True, content, elapsed_time, ""
            
        except Exception as e:
            error_msg = str(e)
            logging.error(f"异步SDK调用失败: {error_msg}")
            return False, "", 0, error_msg
    
    def _call_llm_once(self, user_message: str, max_tokens: int, 
                      temperature: float) -> Tuple[bool, str, float, str]:
        """
        调用LLM一次（无状态单轮问答，符合 Prompt Caching 要求）    
        每次调用都重新构造独立的 messages 数组：
        1. System Message: 使用全局常量 SYSTEM_PROMPT（静态，可被缓存）
        2. User Message: 包含当前帖子的动态数据
        """
        # 每次调用都重新初始化 messages，确保无状态
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message}
        ]
        return self._call_sdk(messages, max_tokens, temperature)
    
    async def _call_llm_once_async(self, user_message: str, max_tokens: int,
                                   temperature: float) -> Tuple[bool, str, float, str]:
        """异步版本：调用LLM一次"""
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message}
        ]
        return await self._call_sdk_async(messages, max_tokens, temperature)

    def analyze_post(self, post: Dict, max_tokens: int, 
                    temperature: float) -> Dict[str, Any]:
        """
        分析单个帖子（带重试机制）
        """
        post_id = post.get("post_id", "unknown")
        index_in_list = post.get("index_in_list", "未知")
        user_message = self._build_user_message(post)
        retry_count = 0
        last_error = ""
        
        for attempt in range(self.max_retries):
            logging.info(f"分析帖子 {post_id} (index: {index_in_list})，尝试 {attempt + 1}/{self.max_retries}")
            
            success, response, llm_time, error = self._call_llm_once(user_message, max_tokens, temperature)
            if not success:
                last_error = f"LLM调用失败: {error}"
                retry_count += 1
                time.sleep(2)
                continue
            
            parse_success, data, parse_error = self._parse_json_response(response)
            if parse_success:
                logging.info(f"帖子 {post_id} (index: {index_in_list}) 分析完成，耗时: {llm_time:.2f}s")
                # 保存结果到数据库
                self._save_result_to_db(post_id, data)
                return {
                    "success": True,
                    "post_id": post_id,
                    "data": data,
                    "raw_response": response,
                    "error": "",
                    "llm_time": llm_time,
                    "retry_count": retry_count
                }
            else:
                last_error = f"JSON解析失败: {parse_error}"
                retry_count += 1
                if attempt < self.max_retries - 1:
                    # 在重试时添加额外提示（仍然在 User Message 中）
                    user_message += "\n\nIMPORTANT: Please ensure you return ONLY the raw JSON object without any markdown formatting or additional text."
                    time.sleep(1)
        
        logging.error(f"帖子 {post_id} (index: {index_in_list}) 分析失败，已重试 {retry_count} 次")
        return {
            "success": False,
            "post_id": post_id,
            "data": None,
            "raw_response": response if 'response' in locals() else "",
            "error": last_error,
            "llm_time": 0,
            "retry_count": retry_count
        }
    
    async def analyze_post_async(self, post: Dict, semaphore: asyncio.Semaphore,
                                 max_tokens: int,
                                 temperature: float) -> Dict[str, Any]:
        """异步版本：分析单个帖子（带重试机制和并发控制）"""
        async with semaphore:  # 使用信号量控制并发
            post_id = post.get("post_id", "unknown")
            index_in_list = post.get("index_in_list", "未知")
            user_message = self._build_user_message(post)
            
            retry_count = 0
            last_error = ""
            response = ""
            
            for attempt in range(self.max_retries):
                logging.info(f"[异步] 分析帖子 {post_id} (index: {index_in_list})，尝试 {attempt + 1}/{self.max_retries}")
                
                success, response, llm_time, error = await self._call_llm_once_async(
                    user_message, max_tokens, temperature
                )
                
                if not success:
                    last_error = f"LLM调用失败: {error}"
                    retry_count += 1
                    await asyncio.sleep(2)
                    continue
                
                parse_success, data, parse_error = self._parse_json_response(response)
                
                if parse_success:
                    logging.info(f"[异步] 帖子 {post_id} (index: {index_in_list}) 分析完成，耗时: {llm_time:.2f}s")
                    # 保存结果到数据库
                    self._save_result_to_db(post_id, data)
                    return {
                        "success": True,
                        "post_id": post_id,
                        "data": data,
                        "raw_response": response,
                        "error": "",
                        "llm_time": llm_time,
                        "retry_count": retry_count
                    }
                else:
                    last_error = f"JSON解析失败: {parse_error}"
                    retry_count += 1
                    if attempt < self.max_retries - 1:
                        user_message += "\n\nIMPORTANT: Please ensure you return ONLY the raw JSON object without any markdown formatting or additional text."
                        await asyncio.sleep(1)
            
            logging.error(f"[异步] 帖子 {post_id} (index: {index_in_list}) 分析失败，已重试 {retry_count} 次")
            return {
                "success": False,
                "post_id": post_id,
                "data": None,
                "raw_response": response,
                "error": last_error,
                "llm_time": 0,
                "retry_count": retry_count
            }
    
    def _parse_json_response(self, response_content: str) -> Tuple[bool, Optional[Dict], str]:
        """解析LLM返回的JSON回复"""
        import re
        
        def validate_and_return(data: Dict, source: str) -> Tuple[bool, Optional[Dict], str]:
            """验证分类代码并返回结果"""
            if (self.check_category_code_name_valid(data.get("category_code", ""), data.get("category_name", "")) and
                self.check_category_code_name_valid(data.get("secondary_category_code", ""), data.get("secondary_category_name", ""))):
                logging.info(f"{source}成功, 结果为: {json.dumps(data, ensure_ascii=False, indent=2)}")
                return True, data, ""
            return False, None, "分类代码和名称不匹配"
        
        # 策略1: 直接解析
        try:
            return validate_and_return(json.loads(response_content), "直接解析JSON")
        except json.JSONDecodeError:
            pass
        
        # 策略2: 提取JSON代码块
        for match in re.findall(r'```(?:json)?\s*\n?(.*?)\n?```', response_content, re.DOTALL):
            try:
                return validate_and_return(json.loads(match.strip()), "提取JSON代码块")
            except json.JSONDecodeError:
                continue
        
        # 策略3: 提取JSON对象（按长度降序尝试）
        for match in sorted(re.findall(r'\{.*\}', response_content, re.DOTALL), key=len, reverse=True):
            try:
                return validate_and_return(json.loads(match), "提取JSON对象")
            except json.JSONDecodeError:
                continue
        
        return False, None, "无法从回复中解析JSON数据"
    
    def check_category_code_name_valid(self, category_code: str, category_name: str) -> bool:
        """检查分类代码和名称是否对应"""
        code_name_map = {
            # A. 安全与物理风险
            "A-01": "走失与召回失败",
            "A-02": "逃逸与围栏突破",
            "A-03": "环境威胁与事故",
            "A-04": "装备与硬件故障",
            # B. 生理健康与医疗
            "B-01": "急性病症与创伤",
            "B-02": "慢性病与长期护理",
            "B-03": "饮食、排泄与营养",
            "B-04": "日常护理与预防",
            # C. 心理健康与情绪状态
            "C-01": "分离焦虑",
            "C-02": "恐惧、应激与反应性",
            "C-03": "攻击性情绪",
            "C-04": "抑郁与低落",
            # D. 行为管理与训练
            "D-01": "服从性与技能训练",
            "D-02": "如厕训练",
            "D-03": "破坏性与坏习惯",
            "D-04": "社交与适应性",
            # E. 活动、探险与生活质量
            "E-01": "嗅闻、探险与自由活动",
            "E-02": "运动量与体能释放",
            "E-03": "室内丰容与游戏",
            "E-04": "情感连接与陪伴",
            # F. 养宠经验与观点
            "F-01": "品种选择与特性",
            "F-02": "养宠成本、法律与居住",
            "F-03": "产品/服务评测与推荐",
            # G. 其他
            "G-01": "无关/广告/垃圾信息",
            "G-02": "无法分类/特殊话题"
        }
        
        if not category_code:
            return True  # 允许空code

        # 检查代码是否存在
        if category_code not in code_name_map:
            logging.warning(f"无效的分类代码: {category_code}")
            return False
        
        # 检查名称是否匹配
        expected_name = code_name_map[category_code]
        if category_name != expected_name:
            logging.warning(f"分类名称不匹配: {category_code} 期望'{expected_name}'，实际'{category_name}'")
            return False
        
        return True 

    def analyze_posts_from_db(self, subreddit: Optional[str] = None,
                             post_ids: Optional[List[str]] = None,
                             index_range: Optional[Tuple[int, int]] = None,
                             max_tokens: int = 4000,
                             temperature: float = 0,
                             delay_between_posts: float = 1.0,
                             include_been_analyzed: bool = False,
                             concurrent: bool = False) -> List[Dict[str, Any]]:
        """
        场景一：从数据库读取帖子并分析
        
        Args:
            subreddit: 子版块名称
            post_ids: 帖子ID列表
            index_range: 帖子索引范围（元组，包含起始和结束索引）
            max_tokens: LLM最大输出 token 数（默认4000）
            temperature: LLM温度参数（默认0，确定性输出）
            delay_between_posts: 串行模式下帖子间隔时间（秒）
            concurrent: 是否启用并发模式（默认False，串行处理）
        """
        posts = self.get_posts_from_db(subreddit, post_ids, index_range)
        if not posts:
            logging.warning("没有获取到帖子")
            return []
        
        logging.info(f"准备分析 {len(posts)} 条帖子，筛选条件: subreddit={subreddit}, post_ids={post_ids}, index_range={index_range}")
        
        # 并发模式
        if concurrent:
            logging.info(f"启用并发模式，最大并发数: {self.max_concurrent}")
            async def run_all():
                semaphore = asyncio.Semaphore(self.max_concurrent)
                tasks = [
                    self.analyze_post_async(post, semaphore, max_tokens, temperature)
                    for post in posts
                ]
                return await asyncio.gather(*tasks)
            
            start_time = time.time()
            results = asyncio.run(run_all())
            elapsed = time.time() - start_time
            
            success_count = sum(1 for r in results if r["success"])
            logging.info(f"并发分析完成: 成功 {success_count}/{len(results)}，总耗时: {elapsed:.2f}s")
            return results
        else:
            # 串行模式
            results = []
            for i, post in enumerate(posts, 1):
                index_in_list = post.get("index_in_list", "未知")
                post_id = post.get("post_id", "unknown")
                logging.info(f"处理进度: {i}/{len(posts)} | index_in_list: {index_in_list} | post_id: {post_id}")
                result = self.analyze_post(post, max_tokens, temperature)
                results.append(result)
                if i < len(posts) and delay_between_posts > 0:
                    time.sleep(delay_between_posts)
            success_count = sum(1 for r in results if r["success"])
            logging.info(f"分析完成: 成功 {success_count}/{len(results)}")
            
            return results
    
    def analyze_post_directly(self, post_data: Dict,
                            max_tokens: int = 4000,
                            temperature: float = 0) -> Dict[str, Any]:
        """
        场景二：直接分析传入的帖子数据（用于爬虫实时调用）
        
        Args:
            post_data: 帖子数据字典
            max_tokens: LLM最大输出 token 数（默认4000）
            temperature: LLM温度参数（默认0，确定性输出）
        """
        logging.info(f"直接分析帖子: {post_data.get('post_id', 'unknown')}")
        return self.analyze_post(post_data, max_tokens, temperature)


def main():
    """示例用法"""
    
    # 初始化分析器（配置从 llm_config.yaml 自动加载）
    # max_concurrent: 控制最大并发请求数，默认为10
    analyzer = LLMAnalyzer(
        model="deepseek-chat",  # 或 "deepseek-reasoner"
        base_url="https://api.deepseek.com/v1",  # DeepSeek API 地址
        max_concurrent=10  # 最大并发数
    )
    
    # 场景一：从数据库读取并分析
    # 串行模式（默认）
    # results = analyzer.analyze_posts_from_db(
    #     subreddit="dogs",
    #     concurrent=False,
    #     # post_ids=["1kidwr3", "1kidhyp"],
    #     index_range=(0, 2),
    #     include_been_analyzed=False,
    # )
    
    # 并发模式（推荐用于大批量处理）
    results = analyzer.analyze_posts_from_db(
        subreddit="dogs",
        concurrent=True,
        index_range=(0, 40000),
        include_been_analyzed=False,
    )
    
    # for result in results:
    #     if result["success"]:
    #         print(f"\n帖子 {result['post_id']} 分析成功")
    #         print(json.dumps(result["data"], ensure_ascii=False, indent=2))
    #         print(f"耗时: {result['llm_time']:.2f}s, 重试: {result['retry_count']}")
    #     else:
    #         print(f"\n帖子 {result['post_id']} 失败: {result['error']}")
    
    # 场景二：直接分析帖子数据
    '''
    post_data = {
        "post_id": "test123",
        "title": "My dog has severe separation anxiety",
        "body": "When I leave for work, my dog howls and destroys the door...",
        "flair_text": "Help",
        "created_time": "2026-01-25",
        "score": 150,
        "comments": [
            {"author": "user1", "body": "Try crate training!", "score": 20},
            {"author": "user2", "body": "My dog had the same issue.", "score": 15}
        ]
    }
    
    result = analyzer.analyze_post_directly(post_data)
    
    if result["success"]:
        print("\n分析成功:")
        print(json.dumps(result["data"], ensure_ascii=False, indent=2))
        # 预期输出：category_code: "C-01", category_name: "分离焦虑"等
    '''

if __name__ == "__main__":
    main()
