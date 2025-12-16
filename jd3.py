"""
京东采集器 - 多线程极速版 (适配 AMD 9955HX)
特性：
1. [并发加速] 补全功能采用 32 线程并发，充分利用高性能CPU和网络带宽。
2. [数据清洗] 增强了对无效数据的过滤，减少“空数据”入库。
3. [双模运行] 
   - 模式1: 浏览器采集 (稳定防封，单线程)
   - 模式2: 接口补全 (多线程极速，每秒处理几十条)
"""

from DrissionPage import ChromiumPage, ChromiumOptions
import csv, time, os, re, random, json, requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# 全局锁，防止多线程写入CSV时冲突
csv_lock = Lock()

class JDFinalScraper:
    """主采集器：负责翻页抓取商品基础信息 (SKU/标题/价格)"""
    def __init__(self):
        co = ChromiumOptions()
        co.set_argument('--mute-audio')
        co.set_argument('--no-first-run')
        # 移除自动化特征，降低被检测概率
        co.set_argument('--disable-blink-features=AutomationControlled')
        
        # 兼容新旧版本 DrissionPage
        try:
            co.headless(False)
        except AttributeError:
            try: co.set_headless(False) 
            except: pass
        
        self.dp = ChromiumPage(addr_or_opts=co)
        print("="*60)
        print("✅ 主采集器已启动")
        print("="*60)

    def _get_desktop_path(self):
        return os.path.join(os.path.expanduser("~"), 'Desktop')

    def run(self, filename='关键词.txt', pages=10, output='汽车零配件数据.csv'):
        desktop_path = self._get_desktop_path()
        keywords_file = os.path.join(desktop_path, filename)
        output_file = os.path.join(desktop_path, output)

        if not os.path.exists(keywords_file):
            try:
                with open(keywords_file, 'w', encoding='utf-8') as f:
                    f.write("全合成机油\n行车记录仪\n米其林轮胎")
            except: pass

        if not os.path.exists(keywords_file):
            print(f"❌ 找不到关键词文件")
            return

        with open(keywords_file, 'r', encoding='utf-8') as f:
            keywords = [k.strip() for k in re.split(r'[,，\n]', f.read()) if k.strip()]
        
        # 启动API监听
        self.dp.listen.start(['pc_search_searchWare', 'api.m.jd.com', 'search'])
        
        total_count = 0
        
        for idx, kw in enumerate(keywords, 1):
            print(f"\n{'='*60}")
            print(f"🎯 [{idx}/{len(keywords)}] 正在采集: {kw}")
            print(f"{'='*60}")
            
            self.dp.listen.clear() 
            url = f'https://search.jd.com/Search?keyword={kw}&enc=utf-8&psort=3'
            self.dp.get(url)
            
            # 等待加载
            if not self.dp.ele('@data-sku', timeout=6):
                print("⚠️  等待超时，尝试手动验证...")
                self._handle_captcha()

            kw_products = []
            
            for page in range(1, pages + 1):
                print(f"   📄 第{page}页", end=" ", flush=True)
                self._human_scroll()
                
                # --- 多重策略 ---
                raw_items = self._try_api_targeted() # API优先
                source = "API"
                
                if not raw_items:
                    raw_items = self.dp.eles('@data-sku')
                    # 过滤无效元素
                    raw_items = [item for item in raw_items if item.rect.size[1] > 0] 
                    source = "DOM"
                
                if not raw_items:
                    raw_items = self._try_regex_chunks()
                    source = "源码"

                if len(raw_items) == 0:
                    print(f" -> {source}(0) 🛑 暂停! 请在浏览器手动操作...")
                    input("👉 解决后按回车...")
                    raw_items = self.dp.eles('@data-sku')
                    
                print(f"-> {source}({len(raw_items)})", end="")

                valid_items = []
                for i, item in enumerate(raw_items, 1):
                    p = self._parse_item_universal(item, kw, page, i)
                    # [数据清洗] 如果SKU都没有，绝对不要
                    if p and p['SKU'].strip(): 
                        valid_items.append(p)

                print(f" -> ✅ {len(valid_items)}条", end="")
                kw_products.extend(valid_items)

                if page < pages:
                    self.dp.listen.clear() 
                    if not self._next_page():
                        print(f" [无下页]", end="")
                        break
                    time.sleep(random.uniform(2, 4))
                else:
                    print("")

            # 实时保存
            if kw_products:
                self._save(kw_products, output_file)
                total_count += len(kw_products)
            
            self._remove_keyword(keywords_file, kw)
            if idx < len(keywords): time.sleep(3)
        
        print(f"\n🎉 采集结束！总计: {total_count}条")
        self.dp.quit()

    # --- 辅助方法 (保持原有的稳定逻辑) ---
    def _human_scroll(self):
        self.dp.scroll.to_bottom(); time.sleep(1); self.dp.scroll.up(300)

    def _try_api_targeted(self):
        try:
            packets = self.dp.listen.steps(timeout=2)
            for packet in packets:
                if 'pc_search_searchWare' in packet.url or 'api.m.jd.com' in packet.url:
                    items = self._find_list_in_json(packet.response.body)
                    if items: return items
        except: pass
        return []

    def _find_list_in_json(self, data):
        if isinstance(data, dict):
            if 'skuId' in data and 'jdPrice' in data: return [data]
            for key in ['Paragraph', 'wareList', 'wareInfo', 'searchm', 'data', 'goodsList']:
                if key in data:
                    res = self._find_list_in_json(data[key])
                    if res: return res
            for v in data.values():
                if isinstance(v, (dict, list)): 
                    res = self._find_list_in_json(v)
                    if res: return res
        elif isinstance(data, list):
            if len(data) > 0 and isinstance(data[0], dict) and ('skuId' in data[0] or 'sku' in data[0]):
                return data
            for i in data:
                res = self._find_list_in_json(i)
                if res: return res
        return []

    def _parse_item_universal(self, item, kw, page, idx):
        try:
            res = {
                '采集时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                '关键词': kw, '页码': page,
                'SKU': '', '标题': '', '价格': '', 
                '店铺': '', '销量': '', '评分': '', '链接': ''
            }
            
            # API模式
            if isinstance(item, dict) and not item.get('is_chunk'):
                res['SKU'] = str(item.get('skuId') or item.get('sku') or '')
                res['标题'] = (item.get('wname') or item.get('wareName') or item.get('title') or '')
                res['价格'] = str(item.get('jdPrice') or item.get('price') or '')
                res['店铺'] = item.get('goodShop', {}).get('goodShopName') or item.get('shop_name') or ''
                res['销量'] = str(item.get('commentCount') or '0')

            # DOM模式
            elif hasattr(item, 'ele'):
                res['SKU'] = item.attr('data-sku') or ''
                t_ele = item.ele('.p-name a', timeout=0.1)
                res['标题'] = t_ele.attr('title') or t_ele.text.strip() if t_ele else ''
                if not res['标题']: res['标题'] = item.ele('.p-name em').text.strip() if item.ele('.p-name em') else ''
                
                p_box = item.ele('.p-price', timeout=0.1)
                if p_box:
                    match = re.search(r'(\d+(\.\d+)?)', p_box.text)
                    if match: res['价格'] = match.group(1)
                
                c_box = item.ele('.p-commit', timeout=0.1)
                if c_box:
                    match = re.search(r'(\d+[万\+]*)', c_box.text)
                    if match: res['销量'] = match.group(1)
                
                s_ele = item.ele('.p-shop', timeout=0.1)
                res['店铺'] = s_ele.text.strip() if s_ele else '京东'

            # 源码模式
            elif isinstance(item, dict) and item.get('is_chunk'):
                chunk = item['chunk_html']
                sku_m = re.search(r'data-sku="(\d+)"', chunk)
                res['SKU'] = sku_m.group(1) if sku_m else ''
                t_m = re.search(r'title="([^"]+)"', chunk)
                res['标题'] = t_m.group(1) if t_m else ''
                p_m = re.search(r'class="p-price".*?(\d+\.\d+)', chunk)
                res['价格'] = p_m.group(1) if p_m else ''
                c_m = re.search(r'(\d+[万\+]*)条评价', chunk)
                res['销量'] = c_m.group(1) if c_m else '0'

            # 格式化
            if res['SKU']: res['SKU'] = f"\t{res['SKU']}" 
            if res['价格']: res['价格'] = re.sub(r'[^\d\.]', '', str(res['价格']))
            res['链接'] = f"https://item.jd.com/{res['SKU'].strip()}.html"
            
            return res if res['SKU'].strip() else None
        except: return None

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
        if self.dp.ele('.JDJR-bigpic', timeout=1) or 'passport.jd.com' in self.dp.url:
            print("\n🚨 请在浏览器完成验证...")
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

    def _remove_keyword(self, filename, kw):
        try:
            with open(filename, 'r', encoding='utf-8') as f: lines = f.read().splitlines()
            lines = [l for l in lines if kw not in l and l.strip()]
            with open(filename, 'w', encoding='utf-8') as f: f.write('\n'.join(lines))
        except: pass


# ---------------------------------------------------------------------
# 多线程补全模块 (利用 9955HX 高性能)
# ---------------------------------------------------------------------
class MultiThreadFiller:
    def __init__(self, workers=32):
        self.workers = workers # 线程数，9955HX可以轻松跑32-64线程
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://item.jd.com/'
        })

    def process_item(self, row):
        """单个商品处理函数"""
        sku = row.get('SKU', '').strip().replace('\t', '')
        if not sku: return None

        # 检查是否需要补全
        has_score = row.get('评分') and row['评分'].strip()
        has_sales = row.get('销量') and row['销量'].strip() and row['销量'] != '0'
        
        if has_score and has_sales:
            return None # 不需要处理

        # 请求数据
        try:
            url = f"https://club.jd.com/comment/productCommentSummaries.action"
            params = {'referenceIds': sku}
            # 设置较短超时，利用多线程快速过
            resp = self.session.get(url, params=params, timeout=5)
            data = resp.json()
            
            if 'CommentsCount' in data and data['CommentsCount']:
                item_data = data['CommentsCount'][0]
                
                # 补全评分
                if not has_score:
                    rate = item_data.get('GoodRateShow', 0)
                    # 将好评率(100)转换为5分制(5.0)
                    score = round(float(rate) * 5 / 100, 1)
                    row['评分'] = str(score)
                
                # 补全销量
                if not has_sales:
                    c_str = item_data.get('CommentCountStr', '')
                    c_num = item_data.get('CommentCount', 0)
                    if c_str and c_str != '0':
                        row['销量'] = c_str.replace('+', '')
                    elif c_num:
                        row['销量'] = str(c_num)
                        
            return row # 返回更新后的行
        except:
            return None # 失败返回空

    def run(self, csv_file, output_file):
        print(f"\n🚀 启动多线程补全 ({self.workers}线程)...")
        desktop = os.path.join(os.path.expanduser("~"), 'Desktop')
        input_path = os.path.join(desktop, csv_file)
        output_path = os.path.join(desktop, output_file)

        if not os.path.exists(input_path):
            print("❌ 文件不存在")
            return

        all_data = []
        with open(input_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            if '评分' not in fieldnames: fieldnames.append('评分')
            if '销量' not in fieldnames: fieldnames.append('销量')
            all_data = list(reader)

        print(f"📋 总数据: {len(all_data)} 条，正在分配任务...")

        # 准备已完成集合，避免重复写入
        processed_skus = set()
        if os.path.exists(output_path):
            with open(output_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    processed_skus.add(row.get('SKU', '').strip())
            print(f"📚 历史已完成: {len(processed_skus)} 条 (跳过)")
        else:
            with open(output_path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

        # 筛选任务
        tasks = []
        for row in all_data:
            sku = row.get('SKU', '').strip()
            if sku not in processed_skus:
                tasks.append(row)

        if not tasks:
            print("✅ 所有数据已完成")
            return

        print(f"⚡ 开始并发处理 {len(tasks)} 条任务...")
        
        count = 0
        success = 0
        
        # 使用线程池并发处理
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {executor.submit(self.process_item, row): row for row in tasks}
            
            for future in as_completed(futures):
                count += 1
                result_row = future.result()
                
                # 原始行（用于失败时也保存，防止数据丢失）
                original_row = futures[future]
                row_to_save = result_row if result_row else original_row
                
                # 线程安全写入
                with csv_lock:
                    try:
                        with open(output_path, 'a', encoding='utf-8-sig', newline='') as f:
                            writer = csv.DictWriter(f, fieldnames=fieldnames)
                            # 确保SKU格式
                            if not row_to_save['SKU'].startswith('\t'):
                                row_to_save['SKU'] = f"\t{row_to_save['SKU']}"
                            writer.writerow(row_to_save)
                    except: pass
                
                if result_row: success += 1
                
                # 进度条
                if count % 50 == 0:
                    print(f"\r🚀 进度: {count}/{len(tasks)} | 成功补全: {success}", end="")

        print(f"\n\n🎉 全部完成！成功补全: {success} 条")
        print(f"💾 结果保存至: {output_file}")


if __name__ == '__main__':
    print("1. 采集数据 (单线程稳定)")
    print("2. 极速补全数据 (多线程，9955HX火力全开)")
    choice = input("请选择: ").strip()
    
    if choice == '1':
        JDFinalScraper().run()
    else:
        csv_file = input("输入文件名 [默认: 汽车零配件数据.csv]: ").strip() or '汽车零配件数据.csv'
        # 32线程对于9955HX来说非常轻松，既能跑满网速又不会卡死
        MultiThreadFiller(workers=32).run(
            csv_file=csv_file, 
            output_file=csv_file.replace('.csv', '_完整版.csv')
        )