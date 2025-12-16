#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
京东评论采集器
=============
读取本地SKU文件，批量采集京东商品评论

特点：
1. 自动读取本地CSV文件中的SKU
2. 调用京东评论API（无需登录）
3. 支持断点续传
4. 实时保存数据
"""

import os
import sys
import csv
import json
import time
import random
import re
from datetime import datetime
from typing import List, Dict, Optional, Set
from dataclasses import dataclass, field
import requests
import logging

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# 输出目录
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 进度文件
PROGRESS_FILE = f"{OUTPUT_DIR}/.jd_progress.json"


@dataclass
class Comment:
    """评论数据"""
    sku_id: str = ""
    product_name: str = ""
    nickname: str = ""
    content: str = ""
    score: int = 0  # 1-5星
    creation_time: str = ""
    reference_time: str = ""  # 购买时间
    product_color: str = ""
    product_size: str = ""
    user_level: str = ""
    is_top: bool = False
    reply_count: int = 0
    useful_vote_count: int = 0
    days_after_confirm: int = 0
    crawl_time: str = ""
    
    def to_dict(self) -> Dict:
        return {
            '商品SKU': self.sku_id,
            '商品名称': self.product_name,
            '用户昵称': self.nickname,
            '评论内容': self.content,
            '评分': self.score,
            '评论时间': self.creation_time,
            '购买时间': self.reference_time,
            '商品颜色': self.product_color,
            '商品规格': self.product_size,
            '用户等级': self.user_level,
            '是否置顶': '是' if self.is_top else '否',
            '回复数': self.reply_count,
            '点赞数': self.useful_vote_count,
            '确认收货后天数': self.days_after_confirm,
            '采集时间': self.crawl_time
        }


class JDCommentCrawler:
    """京东评论爬虫"""
    
    # 京东评论API
    COMMENT_API = "https://club.jd.com/comment/productPageComments.action"
    
    # 请求头
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer': 'https://item.jd.com/',
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.completed_skus: Set[str] = set()
        self.failed_skus: Set[str] = set()
        self.comments: List[Comment] = []
        self.total_comments = 0
        
    def start(self):
        """启动爬虫"""
        self._print_banner()
        self._load_progress()
        self._main_menu()
    
    def _print_banner(self):
        """打印欢迎信息"""
        print("\n" + "=" * 60)
        print("🛒 京东评论采集器")
        print("=" * 60)
        print("功能：读取本地SKU文件，批量采集京东商品评论")
        print("特点：无需登录，API采集，效率高")
        print("=" * 60)
    
    def _load_progress(self):
        """加载进度"""
        if os.path.exists(PROGRESS_FILE):
            try:
                with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.completed_skus = set(data.get('completed', []))
                self.failed_skus = set(data.get('failed', []))
                self.total_comments = data.get('total_comments', 0)
                logger.info(f"已加载进度：完成 {len(self.completed_skus)} 个SKU，{self.total_comments} 条评论")
            except:
                pass
    
    def _save_progress(self):
        """保存进度"""
        try:
            with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
                json.dump({
                    'completed': list(self.completed_skus),
                    'failed': list(self.failed_skus),
                    'total_comments': self.total_comments
                }, f, ensure_ascii=False)
        except:
            pass
    
    def _main_menu(self):
        """主菜单"""
        while True:
            print("\n" + "=" * 60)
            print("主菜单")
            print("=" * 60)
            print("1. 📁 从本地CSV文件读取SKU并采集")
            print("2. 📝 手动输入SKU采集")
            print("3. 🧪 测试单个SKU")
            print("4. 📊 查看采集结果")
            print("5. 🔄 重试失败的SKU")
            print("6. 🗑️  清除进度")
            print("0. 退出")
            print("=" * 60)
            
            choice = input("请选择 (0-6): ").strip()
            
            if choice == "1":
                self._crawl_from_file()
            elif choice == "2":
                self._crawl_manual_input()
            elif choice == "3":
                self._test_single_sku()
            elif choice == "4":
                self._view_results()
            elif choice == "5":
                self._retry_failed()
            elif choice == "6":
                self._clear_progress()
            elif choice == "0":
                print("\n再见！")
                break
    
    def _crawl_from_file(self):
        """从文件读取SKU采集"""
        print("\n" + "-" * 60)
        print("📁 从CSV文件读取SKU")
        print("-" * 60)
        
        # 默认路径
        desktop_path = os.path.expanduser("~/Desktop")
        default_file = os.path.join(desktop_path, "汽车零配件数据_完整版.csv")
        
        print(f"\n默认文件路径: {default_file}")
        
        filepath = input("输入CSV文件路径 (直接回车使用默认): ").strip()
        if not filepath:
            filepath = default_file
        
        # 检查文件
        if not os.path.exists(filepath):
            print(f"\n❌ 文件不存在: {filepath}")
            print("\n请检查：")
            print("  1. 文件路径是否正确")
            print("  2. 文件名是否正确（包括扩展名）")
            
            # 尝试列出桌面文件
            if os.path.exists(desktop_path):
                print(f"\n桌面文件列表:")
                for f in os.listdir(desktop_path):
                    if f.endswith('.csv'):
                        print(f"  - {f}")
            return
        
        # 读取SKU
        skus = self._read_skus_from_csv(filepath)
        
        if not skus:
            print("未找到有效的SKU")
            return
        
        print(f"\n从文件中读取到 {len(skus)} 个SKU")
        
        # 过滤已完成的
        new_skus = [s for s in skus if s not in self.completed_skus]
        
        if not new_skus:
            print("所有SKU都已采集过")
            return
        
        print(f"待采集: {len(new_skus)} 个（已跳过 {len(skus) - len(new_skus)} 个）")
        
        # 设置每个SKU采集的评论页数
        print("\n每个SKU采集多少页评论？")
        print("  1页 = 约10条评论")
        print("  5页 = 约50条评论")
        print("  10页 = 约100条评论")
        
        try:
            max_pages = int(input("每个SKU采集页数 (默认5): ").strip() or "5")
        except:
            max_pages = 5
        
        confirm = input(f"\n确认开始？将采集 {len(new_skus)} 个SKU × {max_pages} 页 (y/n): ").strip().lower()
        
        if confirm == 'y':
            self._crawl_skus(new_skus, max_pages)
    
    def _read_skus_from_csv(self, filepath: str) -> List[str]:
        """从CSV文件读取SKU"""
        skus = []
        
        try:
            # 尝试不同编码
            encodings = ['utf-8-sig', 'utf-8', 'gbk', 'gb2312', 'gb18030']
            
            for encoding in encodings:
                try:
                    with open(filepath, 'r', encoding=encoding) as f:
                        # 读取第一行判断分隔符
                        first_line = f.readline()
                        f.seek(0)
                        
                        # 判断分隔符
                        if '\t' in first_line:
                            delimiter = '\t'
                        else:
                            delimiter = ','
                        
                        reader = csv.DictReader(f, delimiter=delimiter)
                        
                        # 查找SKU列
                        sku_columns = ['sku', 'SKU', 'sku_id', 'SKU_ID', 'skuId', 
                                      '商品ID', '商品编号', 'product_id', 'item_id',
                                      'id', 'ID', '京东SKU', 'jd_sku']
                        
                        found_column = None
                        for col in sku_columns:
                            if col in reader.fieldnames:
                                found_column = col
                                break
                        
                        if not found_column:
                            # 如果没找到，尝试第一列
                            found_column = reader.fieldnames[0] if reader.fieldnames else None
                        
                        if found_column:
                            for row in reader:
                                sku = str(row.get(found_column, '')).strip()
                                # 提取纯数字SKU
                                match = re.search(r'(\d{5,15})', sku)
                                if match:
                                    skus.append(match.group(1))
                            
                            if skus:
                                print(f"✓ 使用列 '{found_column}' 提取SKU")
                                break
                                
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    logger.debug(f"读取失败 ({encoding}): {e}")
                    continue
            
            # 去重
            skus = list(dict.fromkeys(skus))
            
        except Exception as e:
            logger.error(f"读取CSV失败: {e}")
        
        return skus
    
    def _crawl_manual_input(self):
        """手动输入SKU"""
        print("\n" + "-" * 60)
        print("输入京东SKU（每行一个，输入空行结束）：")
        print("示例: 100012043978")
        print("-" * 60)
        
        skus = []
        while True:
            line = input().strip()
            if not line:
                break
            match = re.search(r'(\d{5,15})', line)
            if match:
                skus.append(match.group(1))
        
        if skus:
            try:
                max_pages = int(input("\n每个SKU采集页数 (默认5): ").strip() or "5")
            except:
                max_pages = 5
            
            self._crawl_skus(skus, max_pages)
    
    def _test_single_sku(self):
        """测试单个SKU"""
        print("\n" + "-" * 60)
        sku = input("输入京东SKU进行测试: ").strip()
        
        match = re.search(r'(\d{5,15})', sku)
        if not match:
            print("无效的SKU格式")
            return
        
        sku = match.group(1)
        print(f"\n测试SKU: {sku}")
        print("-" * 60)
        
        # 获取第一页评论
        comments, product_name = self._fetch_comments(sku, page=0)
        
        if comments:
            print(f"\n✓ 测试成功！")
            print(f"  商品名称: {product_name[:40]}...")
            print(f"  获取评论: {len(comments)} 条")
            print("\n前3条评论预览：")
            for i, c in enumerate(comments[:3]):
                print(f"\n  [{i+1}] {c.nickname} ({c.score}星)")
                print(f"      {c.content[:50]}...")
        else:
            print("\n✗ 测试失败，未获取到评论")
            print("可能原因：")
            print("  1. SKU不存在")
            print("  2. 商品无评论")
            print("  3. 网络问题")
    
    def _crawl_skus(self, skus: List[str], max_pages: int = 5):
        """批量采集SKU评论"""
        print("\n" + "=" * 60)
        print(f"开始采集 {len(skus)} 个SKU的评论")
        print(f"每个SKU最多 {max_pages} 页")
        print("=" * 60)
        
        success_count = 0
        fail_count = 0
        total_new_comments = 0
        
        for i, sku in enumerate(skus):
            print(f"\n[{i+1}/{len(skus)}] SKU: {sku}", end=" ")
            
            try:
                sku_comments = 0
                product_name = ""
                
                for page in range(max_pages):
                    comments, name = self._fetch_comments(sku, page)
                    
                    if not product_name and name:
                        product_name = name
                    
                    if comments:
                        self.comments.extend(comments)
                        sku_comments += len(comments)
                        total_new_comments += len(comments)
                    else:
                        # 没有更多评论了
                        break
                    
                    # 延迟
                    time.sleep(random.uniform(0.5, 1.5))
                
                if sku_comments > 0:
                    self.completed_skus.add(sku)
                    success_count += 1
                    print(f"✓ {sku_comments}条 - {product_name[:20]}...")
                else:
                    self.failed_skus.add(sku)
                    fail_count += 1
                    print("✗ 无评论")
                
                # 定期保存
                if (i + 1) % 10 == 0:
                    self._save_comments()
                    self._save_progress()
                    print(f"\n  [进度] 已保存 {total_new_comments} 条评论")
                
                # 随机延迟
                time.sleep(random.uniform(1, 2))
                
            except Exception as e:
                print(f"✗ 错误: {e}")
                self.failed_skus.add(sku)
                fail_count += 1
        
        # 最终保存
        self._save_comments()
        self._save_progress()
        
        print("\n" + "=" * 60)
        print(f"✓ 采集完成！")
        print(f"  成功: {success_count} 个SKU")
        print(f"  失败: {fail_count} 个SKU")
        print(f"  新增评论: {total_new_comments} 条")
        print(f"  累计评论: {self.total_comments} 条")
        print(f"  保存位置: {OUTPUT_DIR}/jd_comments.csv")
        print("=" * 60)
    
    def _fetch_comments(self, sku: str, page: int = 0) -> tuple:
        """获取评论"""
        comments = []
        product_name = ""
        
        params = {
            'productId': sku,
            'score': 0,  # 0=全部, 1=差评, 2=中评, 3=好评
            'sortType': 5,  # 5=推荐排序, 6=时间排序
            'page': page,
            'pageSize': 10,
            'isShadowSku': 0,
            'fold': 1
        }
        
        try:
            response = self.session.get(
                self.COMMENT_API,
                params=params,
                timeout=10
            )
            
            # 处理JSONP响应
            text = response.text
            if text.startswith('fetchJSON_comment98'):
                text = text[text.index('(') + 1: text.rindex(')')]
            
            data = json.loads(text)
            
            # 获取商品名称
            product_info = data.get('productCommentSummary', {})
            product_name = product_info.get('productName', '')
            
            # 解析评论
            for item in data.get('comments', []):
                comment = Comment()
                comment.sku_id = sku
                comment.product_name = product_name
                comment.nickname = item.get('nickname', '')
                comment.content = item.get('content', '').replace('\n', ' ').strip()
                comment.score = item.get('score', 0)
                comment.creation_time = item.get('creationTime', '')
                comment.reference_time = item.get('referenceTime', '')
                comment.product_color = item.get('productColor', '')
                comment.product_size = item.get('productSize', '')
                comment.user_level = item.get('userLevelName', '')
                comment.is_top = item.get('topped', 0) == 1
                comment.reply_count = item.get('replyCount', 0)
                comment.useful_vote_count = item.get('usefulVoteCount', 0)
                comment.days_after_confirm = item.get('days', 0)
                comment.crawl_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                if comment.content:
                    comments.append(comment)
            
        except Exception as e:
            logger.debug(f"获取评论失败: {e}")
        
        return comments, product_name
    
    def _save_comments(self):
        """保存评论"""
        if not self.comments:
            return
        
        filename = f"{OUTPUT_DIR}/jd_comments.csv"
        
        try:
            file_exists = os.path.exists(filename)
            
            with open(filename, 'a', newline='', encoding='utf-8-sig') as f:
                fieldnames = list(self.comments[0].to_dict().keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                
                for comment in self.comments:
                    writer.writerow(comment.to_dict())
            
            self.total_comments += len(self.comments)
            logger.info(f"保存 {len(self.comments)} 条评论")
            self.comments = []
            
        except Exception as e:
            logger.error(f"保存失败: {e}")
    
    def _view_results(self):
        """查看结果"""
        filename = f"{OUTPUT_DIR}/jd_comments.csv"
        
        print("\n" + "=" * 60)
        print("📊 采集结果统计")
        print("=" * 60)
        
        print(f"\n进度统计：")
        print(f"  已完成SKU: {len(self.completed_skus)}")
        print(f"  失败SKU: {len(self.failed_skus)}")
        print(f"  累计评论: {self.total_comments}")
        
        if os.path.exists(filename):
            try:
                with open(filename, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                
                print(f"\n文件统计：")
                print(f"  文件: {filename}")
                print(f"  总记录数: {len(rows)}")
                
                if rows:
                    # 按SKU统计
                    sku_counts = {}
                    for row in rows:
                        sku = row.get('商品SKU', '')
                        sku_counts[sku] = sku_counts.get(sku, 0) + 1
                    
                    print(f"  涉及SKU数: {len(sku_counts)}")
                    
                    # 评分分布
                    scores = {}
                    for row in rows:
                        score = row.get('评分', '0')
                        scores[score] = scores.get(score, 0) + 1
                    
                    print(f"\n评分分布：")
                    for s in sorted(scores.keys(), reverse=True):
                        count = scores[s]
                        pct = count / len(rows) * 100
                        print(f"  {s}星: {count} ({pct:.1f}%)")
                    
                    print(f"\n最近5条评论：")
                    for row in rows[-5:]:
                        content = row.get('评论内容', '')[:40]
                        score = row.get('评分', '')
                        print(f"  [{score}星] {content}...")
                        
            except Exception as e:
                print(f"读取失败: {e}")
        else:
            print(f"\n暂无采集数据")
        
        print("=" * 60)
    
    def _retry_failed(self):
        """重试失败的SKU"""
        if not self.failed_skus:
            print("\n没有失败的SKU")
            return
        
        print(f"\n有 {len(self.failed_skus)} 个失败的SKU")
        confirm = input("确认重试？(y/n): ").strip().lower()
        
        if confirm == 'y':
            skus = list(self.failed_skus)
            self.failed_skus.clear()
            self._crawl_skus(skus, max_pages=5)
    
    def _clear_progress(self):
        """清除进度"""
        confirm = input("确认清除所有进度？(y/n): ").strip().lower()
        
        if confirm == 'y':
            self.completed_skus.clear()
            self.failed_skus.clear()
            self.total_comments = 0
            if os.path.exists(PROGRESS_FILE):
                os.remove(PROGRESS_FILE)
            print("✓ 进度已清除")


def main():
    """主函数"""
    try:
        crawler = JDCommentCrawler()
        crawler.start()
    except KeyboardInterrupt:
        print("\n\n用户中断")
    except Exception as e:
        print(f"\n程序错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()