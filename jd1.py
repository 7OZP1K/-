"""
京东汽车零配件采集器 - 全能补全版
特性：
1. [暴力补全] 标题和价格为空时，启用全文本扫描和链接匹配，确保不漏数据。
2. [API增强] 兼容更多API字段命名，防止接口改版导致取不到值。
3. [核心逻辑] 优先级：API > DOM(增强) > 源码正则(增强)。
"""

from DrissionPage import ChromiumPage, ChromiumOptions
import csv, time, os, re, random, ctypes
from ctypes import wintypes
from datetime import datetime
import json

class AutoPartsScraper:
    def __init__(self):
        co = ChromiumOptions()
        # 开启图片加载以提高安全性，防止被识别为机器人
        # co.set_argument('--blink-settings=imagesEnabled=false') 
        co.set_argument('--mute-audio')
        co.set_argument('--no-first-run')
        
        self.dp = ChromiumPage(addr_or_opts=co)
        print("="*60)
        print("✅ 浏览器已启动 (数据补全模式)")
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
                print(f"📝 已在桌面创建测试文件: {filename}")
            except: pass

        with open(keywords_file, 'r', encoding='utf-8') as f:
            keywords = [k.strip() for k in re.split(r'[,，\n]', f.read()) if k.strip()]
        
        if not keywords:
            print(f"⚠️  {filename} 为空，请添加关键词。")
            return

        print(f"📋 采集任务:")
        print(f"   关键词数: {len(keywords)} | 目标页数: {pages}")
        print(f"   排序模式: 销量/评价数降序 (psort=3)")
        print(f"   输出文件: {output}")
        
        self.dp.listen.start(['pc_search_searchWare', 'search', 'wareList'])
        
        total_count = 0
        
        for idx, kw in enumerate(keywords, 1):
            print(f"\n{'='*60}")
            print(f"🎯 [{idx}/{len(keywords)}] 正在采集: {kw}")
            print(f"{'='*60}")
            
            self.dp.listen.clear() 
            url = f'https://search.jd.com/Search?keyword={kw}&enc=utf-8&psort=3'
            self.dp.get(url)
            
            print("⏳ 等待页面加载...", end="")
            if not self.dp.ele('@data-sku', timeout=6):
                print(" 超时(准备启用硬解析)")
                self._handle_captcha()
            else:
                print(" 完成")

            kw_products = []
            
            for page in range(1, pages + 1):
                print(f"\n   📄 第 {page} 页", end="")
                self._human_scroll()
                
                # --- 策略执行 ---
                raw_items = self._try_api()
                source = "API"
                
                if not raw_items:
                    raw_items = self.dp.eles('@data-sku')
                    raw_items = [item for item in raw_items if item.rect.size[1] > 0] 
                    source = "DOM"
                
                if not raw_items:
                    raw_items = self._try_regex_chunks()
                    source = "源码硬抠"

                if len(raw_items) == 0:
                    print(f" -> {source}捕获(0个)")
                    print("\n🛑 【异常】检测到0数据！可能是验证码或未登录。")
                    print("👉 请在浏览器手动处理，然后按【回车】重试...")
                    input()
                    print("🔄 重试...", end="")
                    raw_items = self._try_api()
                    if not raw_items: raw_items = self.dp.eles('@data-sku')
                    source = "重试" if raw_items else "重试失败"

                print(f" -> {source}捕获({len(raw_items)}个)", end="")

                valid_items = []
                for i, item in enumerate(raw_items, 1):
                    p = self._parse_item_robust(item, kw, page, i)
                    if p: valid_items.append(p)

                print(f" -> ✅ 入库: {len(valid_items)}条", end="")
                kw_products.extend(valid_items)

                if page < pages:
                    self.dp.listen.clear() 
                    if not self._next_page():
                        print(f" [停止翻页]", end="")
                        break
                    time.sleep(random.uniform(3, 5))
            
            if kw_products:
                self._save(kw_products, output_file)
                total_count += len(kw_products)
            
            self._remove_keyword_from_file(keywords_file, kw)
            if idx < len(keywords):
                print("\n☕ 休息 5 秒...")
                time.sleep(5)
        
        print(f"\n🎉 全部采集完成！累计获取: {total_count} 条数据")
        self.dp.quit()

    # ================= 核心增强逻辑 =================

    def _human_scroll(self):
        self.dp.scroll.to_bottom()
        time.sleep(1.5)
        self.dp.scroll.up(500)
        time.sleep(0.5)
        self.dp.scroll.up(300)

    def _parse_item_robust(self, item, kw, page, idx):
        """[增强版] 解析器：穷尽一切手段获取标题和价格"""
        try:
            res = {
                '采集时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                '关键词': kw, '页码': page,
                'SKU': '', '标题': '', '价格': '', 
                '店铺': '', '评论数': '', '链接': ''
            }
            raw_comment = '0'
            
            # --- 1. API 模式 (优先) ---
            if isinstance(item, dict) and not item.get('is_chunk'):
                res['SKU'] = str(item.get('skuId') or item.get('sku') or '')
                # 尝试所有可能的标题字段
                res['标题'] = (item.get('wname') or item.get('wareName') or 
                             item.get('title') or item.get('name') or '')
                # 尝试所有可能的价格字段
                res['价格'] = str(item.get('jdPrice') or item.get('price') or '')
                res['店铺'] = item.get('goodShop', {}).get('goodShopName') or item.get('shopName') or ''
                raw_comment = str(item.get('commentCount') or '0')

            # --- 2. DOM 模式 (暴力查找) ---
            elif hasattr(item, 'ele'):
                res['SKU'] = item.attr('data-sku') or ''
                
                # [标题修复] 多重兜底查找
                t_ele = item.ele('.p-name a', timeout=0.1) # 1. 找标准标题链接
                if t_ele:
                    # 优先取 title 属性（通常是全名），没有取文本
                    res['标题'] = t_ele.attr('title') or t_ele.text.strip()
                
                # 2. 如果没找到，找任何含SKU的链接
                if not res['标题']:
                    t_ele = item.ele(f'a[href*="{res["SKU"]}"]', timeout=0.1)
                    if t_ele: res['标题'] = t_ele.attr('title') or t_ele.text.strip()
                
                # 3. 如果还没找到，找字数最多的文本行（终极兜底）
                if not res['标题']:
                    lines = item.text.split('\n')
                    # 过滤掉纯数字或价格行
                    valid_lines = [l for l in lines if len(l) > 5 and not re.match(r'^[¥￥0-9\.]+$', l.strip())]
                    if valid_lines:
                        res['标题'] = max(valid_lines, key=len).strip()

                # [价格] 优先找 .p-price
                p_ele = item.ele('.p-price', timeout=0.1)
                if p_ele:
                    # 提取任何看起来像价格的数字 (支持 ¥299.00 或 299)
                    price_match = re.search(r'[¥￥]?\s*(\d+(\.\d+)?)', p_ele.text)
                    res['价格'] = price_match.group(1) if price_match else ''
                
                # 如果没找到，暴力扫描整个卡片文本
                if not res['价格']:
                    full_text = item.text
                    price_match = re.search(r'[¥￥]\s*(\d+(\.\d+)?)', full_text)
                    res['价格'] = price_match.group(1) if price_match else ''

                s_ele = item.ele('.p-shop', timeout=0.1)
                res['店铺'] = s_ele.text.strip() if s_ele else '京东'
                
                c_ele = item.ele('.p-commit', timeout=0.1)
                raw_comment = c_ele.text.strip() if c_ele else '0'

            # --- 3. 源码硬抠模式 ---
            elif isinstance(item, dict) and item.get('is_chunk'):
                chunk = item['chunk_html']
                sku_m = re.search(r'data-sku="(\d+)"', chunk)
                res['SKU'] = sku_m.group(1) if sku_m else ''
                
                # [标题修复] 增加对 title="..." 属性的匹配
                t_match_attr = re.search(r'class="p-name".*?title="([^"]+)"', chunk, re.DOTALL)
                if t_match_attr:
                    res['标题'] = t_match_attr.group(1).strip()
                else:
                    # 备选：匹配 em 标签
                    t_match = re.search(r'class="p-name".*?em[^>]*>([^<]+)</em>', chunk, re.DOTALL)
                    if t_match: res['标题'] = t_match.group(1).strip()
                
                # 价格：放宽正则，寻找 ¥ 后面的数字
                p_match = re.search(r'[¥￥](?:<[^>]+>)*\s*(\d+(?:\.\d+)?)', chunk)
                if p_match:
                    res['价格'] = p_match.group(1)
                
                raw_comment = "0+"

            # [修复] Excel 科学计数法
            if res['SKU']: res['SKU'] = f"\t{res['SKU']}" 
            res['评论数'] = raw_comment
            res['链接'] = f"https://item.jd.com/{res['SKU'].strip()}.html"
            
            # 只有当SKU存在时才返回
            return res if res['SKU'].strip() else None
            
        except: return None

    def _try_api(self):
        try:
            p = self.dp.listen.wait(['pc_search_searchWare', 'search', 'wareList'], timeout=2)
            if p: return self._find_list_in_json(p.response.body)
        except: return []

    def _find_list_in_json(self, data):
        if isinstance(data, dict):
            if 'skuId' in data and ('wname' in data or 'wareName' in data): return [data]
            for key in ['wareList', 'wareInfo', 'searchm']:
                if key in data and isinstance(data[key], list): return data[key]
                if key in data and isinstance(data[key], dict): return self._find_list_in_json(data[key])
            for v in data.values():
                if isinstance(v, (dict, list)): 
                    r = self._find_list_in_json(v)
                    if r: return r
        elif isinstance(data, list):
            for i in data:
                r = self._find_list_in_json(i)
                if r: return r if isinstance(r, list) else [r]
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
        except: return []

    def _next_page(self):
        try:
            btn = self.dp.ele('.pn-next', timeout=1) or self.dp.ele('text:下一页', timeout=1)
            if btn and 'disabled' not in (btn.attr('class') or ''):
                btn.scroll.to_center(); btn.click(by_js=True); return True
            return False
        except: return False

    def _handle_captcha(self):
        if self.dp.ele('.JDJR-bigpic', timeout=1): self.dp.wait.ele_hidden('.JDJR-bigpic', timeout=60)
        if 'passport.jd.com' in self.dp.url: 
            print("\n🚨 请登录！"); 
            while 'passport.jd.com' in self.dp.url: time.sleep(2)

    def _save(self, products, filename):
        try:
            exist = os.path.exists(filename)
            with open(filename, 'a', encoding='utf-8-sig', newline='') as f:
                headers = list(products[0].keys())
                w = csv.DictWriter(f, fieldnames=headers)
                if not exist: w.writeheader()
                w.writerows(products)
        except: pass

    def _remove_keyword_from_file(self, filename, kw):
        try:
            with open(filename, 'r', encoding='utf-8') as f: lines = f.read().splitlines()
            lines = [l for l in lines if kw not in l and l.strip()]
            with open(filename, 'w', encoding='utf-8') as f: f.write('\n'.join(lines))
        except: pass

if __name__ == '__main__':
    AutoPartsScraper().run()