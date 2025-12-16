"""
京东汽车零配件采集器 - 评分+评论数增强版
新增功能：
1. 商品评分（如 4.8分）
2. 评论数量（如 10万+条）
3. 多重提取策略确保数据完整
"""

from DrissionPage import ChromiumPage, ChromiumOptions
import csv, time, os, re, random, ctypes
from ctypes import wintypes
from datetime import datetime
import json

class AutoPartsScraper:
    def __init__(self):
        co = ChromiumOptions()
        co.set_argument('--mute-audio')
        co.set_argument('--no-first-run')
        
        self.dp = ChromiumPage(addr_or_opts=co)
        print("="*60)
        print("✅ 浏览器已启动 (评分+评论数增强版)")
        print("="*60)

    def _get_true_desktop_path(self):
        try:
            buf = ctypes.create_unicode_buffer(wintypes.MAX_PATH)
            ctypes.windll.shell32.SHGetSpecialFolderPathW(None, buf, 0x0000, False)
            return buf.value
        except:
            return os.path.join(os.path.expanduser("~"), 'Desktop')

    def run(self, filename='关键词.txt', pages=15, output='汽车零配件数据.csv'):
        desktop_path = self._get_true_desktop_path()
        keywords_file = os.path.join(desktop_path, filename)
        output_file = os.path.join(desktop_path, output)

        if not os.path.exists(keywords_file):
            try:
                with open(keywords_file, 'w', encoding='utf-8') as f:
                    f.write("全合成机油\n行车记录仪\n米其林轮胎")
            except: pass

        with open(keywords_file, 'r', encoding='utf-8') as f:
            keywords = [k.strip() for k in re.split(r'[,，\n]', f.read()) if k.strip()]
        
        if not keywords:
            print(f"⚠️  {filename} 为空。")
            return

        print(f"📋 采集任务: {len(keywords)}个词 | 目标: {pages}页/词")
        
        self.dp.listen.start(['pc_search_searchWare', 'search', 'wareList'])
        
        total_count = 0
        
        for idx, kw in enumerate(keywords, 1):
            print(f"\n{'='*60}")
            print(f"🎯 [{idx}/{len(keywords)}] 正在采集: {kw}")
            print(f"{'='*60}")
            
            self.dp.listen.clear() 
            url = f'https://search.jd.com/Search?keyword={kw}&enc=utf-8&psort=3'
            self.dp.get(url)
            
            print("⏳ 等待页面...", end="")
            if not self.dp.ele('@data-sku', timeout=6):
                print(" 超时(准备硬解析)")
                self._handle_captcha()
            else:
                print(" 完成")

            kw_products = []
            
            for page in range(1, pages + 1):
                print(f"\n   📄 第 {page} 页", end="")
                self._human_scroll()
                
                raw_items = self._try_api()
                source = "API"
                
                if not raw_items:
                    raw_items = self.dp.eles('@data-sku')
                    raw_items = [item for item in raw_items if item.rect.size[1] > 10] 
                    source = "DOM"
                
                if not raw_items:
                    raw_items = self._try_regex_chunks()
                    source = "源码硬抠"

                if len(raw_items) == 0:
                    print(f" -> {source}捕获(0个) -> 🛑 暂停！")
                    print("👉 请在浏览器手动刷新或验证，解决后按回车重试...")
                    input()
                    raw_items = self.dp.eles('@data-sku')
                    source = "重试DOM" if raw_items else "重试失败"

                print(f" -> {source}捕获({len(raw_items)}个)", end="")

                valid_items = []
                for i, item in enumerate(raw_items, 1):
                    p = self._parse_item_nuclear(item, kw, page, i)
                    if p: valid_items.append(p)

                print(f" -> ✅ 入库: {len(valid_items)}条", end="")
                kw_products.extend(valid_items)

                if page < pages:
                    self.dp.listen.clear() 
                    if not self._next_page():
                        print(f" [停止]", end="")
                        break
                    time.sleep(random.uniform(3, 5))
            
            if kw_products:
                self._save(kw_products, output_file)
                total_count += len(kw_products)
            
            self._remove_keyword_from_file(keywords_file, kw)
            if idx < len(keywords): time.sleep(random.randint(3, 5))
        
        print(f"\n🎉 完成！总计: {total_count}条")
        self.dp.quit()

    def _human_scroll(self):
        """多次滚动加载更多商品"""
        for _ in range(3):
            self.dp.scroll.to_bottom()
            time.sleep(1.5)
        self.dp.scroll.up(300)
        time.sleep(1)

    def _extract_comment_count(self, text):
        """提取评论数量"""
        if not text:
            return '0'
        
        # 模式1: 10万+条评价 / 10万+评价
        match = re.search(r'(\d+\.?\d*万?\+?)(?:条)?评价', text)
        if match:
            return match.group(1)
        
        # 模式2: 已有10000人评价 / 10000人评价
        match = re.search(r'(?:已有)?(\d+)人评价', text)
        if match:
            return match.group(1)
        
        # 模式3: 评论数 10000
        match = re.search(r'评论[数量]?\s*[:：]?\s*(\d+)', text)
        if match:
            return match.group(1)
        
        # 模式4: 单独的大数字
        match = re.search(r'(\d+\.?\d*万\+?)', text)
        if match:
            num_str = match.group(1)
            if '万' in num_str:
                return num_str
        
        # 模式5: commentCount JSON字段
        match = re.search(r'commentCount["\']?\s*[:：]\s*["\']?(\d+)', text)
        if match:
            return match.group(1)
        
        return '0'

    def _extract_rating(self, text):
        """提取商品评分（新增）"""
        if not text:
            return ''
        
        # 模式1: 4.8分 / 4.8 / 98% 好评
        match = re.search(r'(\d+\.?\d*)分', text)
        if match:
            return match.group(1)
        
        # 模式2: 好评率 98%
        match = re.search(r'好评率?\s*[:：]?\s*(\d+)%', text)
        if match:
            percent = int(match.group(1))
            # 转换为5分制评分（近似）
            return str(round(percent * 5 / 100, 1))
        
        # 模式3: 直接的数字评分（如API返回）
        match = re.search(r'(?:评分|score)["\']?\s*[:：]\s*["\']?(\d+\.?\d*)', text, re.I)
        if match:
            return match.group(1)
        
        return ''

    def _parse_item_nuclear(self, item, kw, page, idx):
        """核弹级解析器 - 增加评分和评论数"""
        try:
            res = {
                '采集时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                '关键词': kw, '页码': page,
                'SKU': '', '标题': '', '价格': '', 
                '店铺': '', '评分': '', '评论数': '', '链接': ''
            }
            
            # --- 1. API 模式 ---
            if isinstance(item, dict) and not item.get('is_chunk'):
                res['SKU'] = str(item.get('skuId') or item.get('sku') or '')
                res['标题'] = (item.get('wname') or item.get('wareName') or 
                             item.get('title') or item.get('name') or '')
                res['价格'] = str(item.get('jdPrice') or item.get('price') or '')
                res['店铺'] = item.get('goodShop', {}).get('goodShopName') or item.get('shopName') or '京东'
                
                # API 评论数
                comment_count = item.get('commentCount') or item.get('comments') or 0
                res['评论数'] = str(comment_count)
                
                # API 评分
                score = item.get('score') or item.get('rating') or item.get('goodRate') or ''
                res['评分'] = str(score) if score else ''

            # --- 2. DOM 模式 ---
            elif hasattr(item, 'ele'):
                res['SKU'] = item.attr('data-sku') or ''
                
                # [标题]
                img_ele = item.ele('tag:img', timeout=0.1)
                if img_ele: 
                    res['标题'] = img_ele.attr('alt') or ''
                
                if not res['标题']:
                    t_ele = item.ele('.p-name em', timeout=0.1)
                    if t_ele: 
                        res['标题'] = t_ele.text.strip()

                if not res['标题'] or len(res['标题']) < 3:
                    lines = item.text.split('\n')
                    valid_lines = [l for l in lines if len(l) > 8 and '¥' not in l]
                    if valid_lines:
                        res['标题'] = max(valid_lines, key=len).strip()

                # [价格]
                full_text = item.text
                p_match = re.search(r'[¥￥]\s*(\d+(\.\d+)?)', full_text)
                if p_match:
                    res['价格'] = p_match.group(1)
                
                # [评论数] - 多重策略
                comment_ele = item.ele('.p-commit', timeout=0.1)
                if comment_ele:
                    res['评论数'] = self._extract_comment_count(comment_ele.text)
                
                if not res['评论数'] or res['评论数'] == '0':
                    res['评论数'] = self._extract_comment_count(full_text)
                
                if not res['评论数'] or res['评论数'] == '0':
                    comment_attr = item.attr('data-comment')
                    if comment_attr:
                        res['评论数'] = comment_attr

                # [评分] - 新增提取逻辑
                # 策略1: 查找评分元素
                rating_ele = item.ele('.p-score', timeout=0.1)
                if rating_ele:
                    res['评分'] = self._extract_rating(rating_ele.text)
                
                # 策略2: 从评论区域提取
                if not res['评分'] and comment_ele:
                    res['评分'] = self._extract_rating(comment_ele.text)
                
                # 策略3: 从全文本提取
                if not res['评分']:
                    res['评分'] = self._extract_rating(full_text)
                
                # 策略4: 查找 data-score 属性
                if not res['评分']:
                    score_attr = item.attr('data-score')
                    if score_attr:
                        res['评分'] = score_attr

                # [店铺]
                shop_ele = item.ele('.p-shop a', timeout=0.1)
                if shop_ele:
                    res['店铺'] = shop_ele.text.strip()
                
                if not res['店铺'] or res['店铺'] == '京东':
                    shop_div = item.ele('.p-shop', timeout=0.1)
                    if shop_div:
                        shop_text = shop_div.text.strip()
                        shop_text = re.sub(r'(进店|关注|自营)', '', shop_text).strip()
                        if shop_text:
                            res['店铺'] = shop_text
                
                if not res['店铺'] or res['店铺'] == '京东':
                    shop_match = re.search(r'([^\s]+?(?:旗舰店|专营店|官方店|自营|京东))', full_text)
                    if shop_match:
                        res['店铺'] = shop_match.group(1)
                    else:
                        res['店铺'] = '京东'

            # --- 3. 源码硬抠模式 ---
            elif isinstance(item, dict) and item.get('is_chunk'):
                chunk = item['chunk_html']
                sku_m = re.search(r'data-sku="(\d+)"', chunk)
                res['SKU'] = sku_m.group(1) if sku_m else ''
                
                t_match = re.search(r'title="([^"]+)"', chunk)
                if t_match: 
                    res['标题'] = t_match.group(1)
                else:
                    t_match = re.search(r'em[^>]*>([^<]+)</em>', chunk)
                    if t_match: 
                        res['标题'] = re.sub(r'<[^>]+>', '', t_match.group(1)).strip()
                
                p_match = re.search(r'[¥￥]\s*(\d+(\.\d+)?)', chunk)
                if p_match: 
                    res['价格'] = p_match.group(1)
                
                # 源码提取评论数
                res['评论数'] = self._extract_comment_count(chunk)
                
                # 源码提取评分
                res['评分'] = self._extract_rating(chunk)
                
                # 源码提取店铺
                shop_match = re.search(r'data-shop[^>]*>([^<]+)</a>', chunk)
                if shop_match:
                    res['店铺'] = shop_match.group(1).strip()
                else:
                    shop_match = re.search(r'([^\s]+?(?:旗舰店|专营店|官方店|自营|京东))', chunk)
                    res['店铺'] = shop_match.group(1) if shop_match else '京东'

            # 格式化
            if res['SKU']: 
                res['SKU'] = f"\t{res['SKU']}" 
            if res['价格']: 
                res['价格'] = re.sub(r'[^\d\.]', '', str(res['价格']))
            
            res['链接'] = f"https://item.jd.com/{res['SKU'].strip()}.html"
            
            return res if res['SKU'].strip() else None
            
        except Exception as e:
            return None

    def _try_api(self):
        try:
            p = self.dp.listen.wait(['pc_search_searchWare', 'search', 'wareList'], timeout=2)
            if p: 
                return self._find_list_in_json(p.response.body)
        except: 
            return []

    def _find_list_in_json(self, data):
        if isinstance(data, dict):
            if 'skuId' in data and ('wname' in data or 'wareName' in data): 
                return [data]
            for key in ['wareList', 'wareInfo', 'searchm']:
                if key in data and isinstance(data[key], list): 
                    return data[key]
                if key in data and isinstance(data[key], dict): 
                    return self._find_list_in_json(data[key])
            for v in data.values():
                if isinstance(v, (dict, list)): 
                    r = self._find_list_in_json(v)
                    if r: 
                        return r
        elif isinstance(data, list):
            for i in data:
                r = self._find_list_in_json(i)
                if r: 
                    return r if isinstance(r, list) else [r]
        return []

    def _try_regex_chunks(self):
        try:
            html = self.dp.html
            chunks = []
            for match in re.finditer(r'data-sku="(\d+)"', html):
                start = match.start()
                chunk = html[max(0, start-200): min(len(html), start+1500)]
                chunks.append({'chunk_html': chunk, 'is_chunk': True})
            return chunks
        except: 
            return []

    def _next_page(self):
        try:
            btn = self.dp.ele('.pn-next', timeout=1) or self.dp.ele('text:下一页', timeout=1)
            if btn and 'disabled' not in (btn.attr('class') or ''):
                btn.scroll.to_center()
                btn.click(by_js=True)
                return True
            return False
        except: 
            return False

    def _handle_captcha(self):
        if self.dp.ele('.JDJR-bigpic', timeout=1): 
            self.dp.wait.ele_hidden('.JDJR-bigpic', timeout=60)
        if 'passport.jd.com' in self.dp.url: 
            print("\n🚨 请登录！")
            while 'passport.jd.com' in self.dp.url: 
                time.sleep(2)

    def _save(self, products, filename):
        try:
            exist = os.path.exists(filename)
            with open(filename, 'a', encoding='utf-8-sig', newline='') as f:
                headers = list(products[0].keys())
                w = csv.DictWriter(f, fieldnames=headers)
                if not exist: 
                    w.writeheader()
                w.writerows(products)
        except: 
            pass

    def _remove_keyword_from_file(self, filename, kw):
        try:
            with open(filename, 'r', encoding='utf-8') as f: 
                lines = f.read().splitlines()
            lines = [l for l in lines if kw not in l and l.strip()]
            with open(filename, 'w', encoding='utf-8') as f: 
                f.write('\n'.join(lines))
        except: 
            pass

if __name__ == '__main__':
    AutoPartsScraper().run()