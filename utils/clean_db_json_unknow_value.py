"""
清洗数据库中 llm_analyze_result 字段的工具
将 JSON 中值为 "unknown" 的字段替换为 null
"""
import json
import sqlite3
import logging
from pathlib import Path
from typing import Any, Dict

# ================= 配置区 =================
DB_PATH = "./outputs/reddit_posts.sqlite"

# 定义哪些值被视为 "Unknown" (不区分大小写)
DIRTY_VALUES = {
    "unknown", "n/a", "not mentioned", "none", "null", "unspecified",
    "not specified", "no information", "unknown (not mentioned)"
}

# ================= 日志配置 =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("clean_db_json_unknown_value.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)


def is_dirty_value(value: Any) -> bool:
    """判断一个值是否属于脏数据"""
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in DIRTY_VALUES
    return False


def clean_dict(data: Dict) -> Dict:
    """递归清洗字典中的所有 unknown 值"""
    cleaned = {}
    for key, value in data.items():
        if isinstance(value, dict):
            # 递归处理嵌套字典
            cleaned[key] = clean_dict(value)
        elif isinstance(value, list):
            # 处理列表
            cleaned_list = []
            for item in value:
                if isinstance(item, dict):
                    cleaned_list.append(clean_dict(item))
                elif not is_dirty_value(item):
                    cleaned_list.append(item)
            # 如果列表清洗后为空，设为 None
            cleaned[key] = cleaned_list if cleaned_list else None
        elif is_dirty_value(value):
            # 将脏值替换为 None
            cleaned[key] = None
        else:
            cleaned[key] = value
    # logging.info(f"清洗前: {data}, 清洗后: {cleaned}")
    return cleaned


def clean_llm_results(db_path: str = DB_PATH, dry_run: bool = False) -> None:
    """
    清洗数据库中所有 llm_analyze_result 字段
    
    Args:
        db_path: 数据库路径
        dry_run: 是否为演习模式（不实际写入数据库）
    """
    if not Path(db_path).exists():
        logging.error(f"数据库文件不存在: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 查询所有有 llm_analyze_result 的记录
        cursor.execute(
            "SELECT post_id, llm_analyze_result FROM posts WHERE llm_analyze_result IS NOT NULL"
        )
        rows = cursor.fetchall()
        
        if not rows:
            logging.info("没有找到需要清洗的记录")
            return
        
        logging.info(f"找到 {len(rows)} 条记录需要处理")
        
        cleaned_count = 0
        error_count = 0
        
        for post_id, llm_result_json in rows:
            try:
                # 解析 JSON
                result_data = json.loads(llm_result_json)
                
                # 清洗数据
                cleaned_data = clean_dict(result_data)
                
                # 序列化回 JSON
                cleaned_json = json.dumps(cleaned_data, ensure_ascii=False)
                
                # 检查是否有变化
                if cleaned_json != llm_result_json:
                    if not dry_run:
                        # 更新数据库
                        cursor.execute(
                            "UPDATE posts SET llm_analyze_result = ? WHERE post_id = ?",
                            (cleaned_json, post_id)
                        )
                    cleaned_count += 1
                    logging.info(f"{'[DRY RUN] ' if dry_run else ''}已清洗帖子: {post_id}")
                
            except json.JSONDecodeError as e:
                logging.error(f"帖子 {post_id} JSON 解析失败: {e}")
                error_count += 1
            except Exception as e:
                logging.error(f"处理帖子 {post_id} 时出错: {e}")
                error_count += 1
        
        if not dry_run:
            conn.commit()
            logging.info(f"清洗完成: 成功 {cleaned_count} 条，失败 {error_count} 条")
        else:
            logging.info(f"[DRY RUN] 预计清洗: {cleaned_count} 条，失败 {error_count} 条")
        
    except Exception as e:
        logging.error(f"数据库操作失败: {e}")
        conn.rollback()
    finally:
        conn.close()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="清洗数据库中 llm_analyze_result 的 unknown 值")
    parser.add_argument("--db", default=DB_PATH, help="数据库路径")
    parser.add_argument("--dry-run", action="store_true", help="演习模式，不实际写入数据库")
    
    args = parser.parse_args()
    
    logging.info("=" * 50)
    logging.info(f"开始清洗任务 {'[DRY RUN MODE]' if args.dry_run else ''}")
    logging.info(f"数据库路径: {args.db}")
    logging.info("=" * 50)
    
    clean_llm_results(args.db, args.dry_run)
    
    logging.info("=" * 50)
    logging.info("任务完成")
    logging.info("=" * 50)


if __name__ == "__main__":
    main()
