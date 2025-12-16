"""
京东商品采集器 
	--author:7OZP1K
"""
from DrissionPage import ChromiumPage, ChromiumOptions
import csv, time, os, re, random, json, requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

csv_lock = Lock()

class JDProductScraper:
    """商品信息采集"""
    
    def __init__(self):
        co = ChromiumOptions()
        co.set_argument('--mute-audio')
        co.set_argument('--no-first-run')
        co.set_argument('--disable-blink-features=AutomationControlled')
        
        try:
            co.headless(False)
        except AttributeError:
            try: 
                co.set_headless(False) 
            except: 
                pass
        
        self.dp = ChromiumPage(addr_or_opts=co)
        print("=" * 60)
        print("浏览器采集器已启动")
        print("=" * 60)

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
                print(f"已创建测试文件: {filename}")
            except: 
                pass

        if not os.path.exists(keywords_file):
            print(f"找不到关键词文件")
            return

        with open(keywords_file, 'r', encoding='utf-8') as f:
            keywords = [k.strip() for k in re.split(r'[,，\n]', f.read()) if k.strip()]
        
        self.dp.listen.start(['pc_search_searchWare', 'api.m.jd.com', 'search'])
        
        total_count = 0
        
        for idx, kw in enumerate(keywords, 1):
            print(f"\n{'=' * 60}")
            print(f"[{idx}/{len(keywords)}] 正在采集: {kw}")
            print(f"{'=' * 60}")
            
            self.dp.listen.clear() 
            url = f'https://search.jd.com/Search?keyword={kw}&enc=utf-8&psort=3'
            self.dp.get(url)
            
            if not self.dp.ele('@data-sku', timeout=6):
                print("等待超时，尝试手动验证...")
                self._handle_captcha()

            kw_products = []
            
            for page in range(1, pages + 1):
                print(f"   第{page}页", end=" ", flush=True)
                self._human_scroll()
                
                # 获取数据：优先API，其次DOM，最后正则
                raw_items = self._try_api_targeted()
                source = "API"
                
                if not raw_items:
                    raw_items = self.dp.eles('@data-sku')
                    raw_items = [item for item in raw_items if item.rect.size[1] > 0] 
                    source = "DOM"
                
                if not raw_items:
                    raw_items = self._try_regex_chunks()
                    source = "源码"

                if len(raw_items) == 0:
                    print(f" -> {source}(0) 暂停! 请在浏览器手动操作...")
                    input("解决后按回车...")
                    raw_items = self.dp.eles('@data-sku')
                    
                print(f"-> {source}({len(raw_items)})", end="")

                valid_items = []
                for i, item in enumerate(raw_items, 1):
                    p = self._parse_item_universal(item, kw, page, i)
                    if p and p['SKU'].strip(): 
                        valid_items.append(p)

                print(f" -> {len(valid_items)}条", end="")
                kw_products.extend(valid_items)

                if page < pages:
                    self.dp.listen.clear() 
                    if not self._next_page():
                        print(f" [无下页]", end="")
                        break
                    time.sleep(random.uniform(2, 4))
                else:
                    print("")

            if kw_products:
                self._save(kw_products, output_file)
                total_count += len(kw_products)
            
            self._remove_keyword(keywords_file, kw)
            if idx < len(keywords): 
                time.sleep(3)
        
        print(f"\n采集结束！总计: {total_count}条")
        self.dp.quit()

    def _human_scroll(self):
        self.dp.scroll.to_bottom()
        time.sleep(1)
        self.dp.scroll.up(300)

    def _try_api_targeted(self):
        try:
            packets = self.dp.listen.steps(timeout=2)
            for packet in packets:
                if 'pc_search_searchWare' in packet.url or 'api.m.jd.com' in packet.url:
                    items = self._find_list_in_json(packet.response.body)
                    if items: 
                        return items
        except: 
            pass
        return []

    def _find_list_in_json(self, data):
        if isinstance(data, dict):
            if 'skuId' in data and 'jdPrice' in data: 
                return [data]
            for key in ['Paragraph', 'wareList', 'wareInfo', 'searchm', 'data', 'goodsList']:
                if key in data:
                    res = self._find_list_in_json(data[key])
                    if res: 
                        return res
            for v in data.values():
                if isinstance(v, (dict, list)): 
                    res = self._find_list_in_json(v)
                    if res: 
                        return res
        elif isinstance(data, list):
            if len(data) > 0 and isinstance(data[0], dict) and ('skuId' in data[0] or 'sku' in data[0]):
                return data
            for i in data:
                res = self._find_list_in_json(i)
                if res: 
                    return res
        return []

    def _parse_item_universal(self, item, kw, page, idx):
        try:
            res = {
                '采集时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                '关键词': kw, 
                '页码': page,
                'SKU': '', 
                '标题': '', 
                '价格': '', 
                '店铺': '', 
                '销量': '', 
                '评分': '', 
                '链接': ''
            }
            
            # API数据
            if isinstance(item, dict) and not item.get('is_chunk'):
                res['SKU'] = str(item.get('skuId') or item.get('sku') or '')
                res['标题'] = (item.get('wname') or item.get('wareName') or item.get('title') or '')
                res['价格'] = str(item.get('jdPrice') or item.get('price') or '')
                res['店铺'] = item.get('goodShop', {}).get('goodShopName') or item.get('shop_name') or ''
                res['销量'] = str(item.get('commentCount') or '0')

            # DOM数据
            elif hasattr(item, 'ele'):
                res['SKU'] = item.attr('data-sku') or ''
                t_ele = item.ele('.p-name a', timeout=0.1)
                res['标题'] = t_ele.attr('title') or t_ele.text.strip() if t_ele else ''
                if not res['标题']: 
                    res['标题'] = item.ele('.p-name em').text.strip() if item.ele('.p-name em') else ''
                
                p_box = item.ele('.p-price', timeout=0.1)
                if p_box:
                    match = re.search(r'(\d+(\.\d+)?)', p_box.text)
                    if match: 
                        res['价格'] = match.group(1)
                
                c_box = item.ele('.p-commit', timeout=0.1)
                if c_box:
                    match = re.search(r'(\d+[万\+]*)', c_box.text)
                    if match: 
                        res['销量'] = match.group(1)
                
                s_ele = item.ele('.p-shop', timeout=0.1)
                res['店铺'] = s_ele.text.strip() if s_ele else '京东'

            # 源码数据
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

            if res['SKU']: 
                res['SKU'] = f"\t{res['SKU']}" 
            if res['价格']: 
                res['价格'] = re.sub(r'[^\d\.]', '', str(res['价格']))
            res['链接'] = f"https://item.jd.com/{res['SKU'].strip()}.html"
            
            return res if res['SKU'].strip() else None
        except: 
            return None

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
        if self.dp.ele('.JDJR-bigpic', timeout=1) or 'passport.jd.com' in self.dp.url:
            print("\n请在浏览器完成验证...")
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

    def _remove_keyword(self, filename, kw):
        try:
            with open(filename, 'r', encoding='utf-8') as f: 
                lines = f.read().splitlines()
            lines = [l for l in lines if kw not in l and l.strip()]
            with open(filename, 'w', encoding='utf-8') as f: 
                f.write('\n'.join(lines))
        except: 
            pass


class MultiThreadFiller:
    """多线程数据补全"""
    
    def __init__(self, workers=32):
        self.workers = workers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://item.jd.com/'
        })

    def process_item(self, row):
        sku = row.get('SKU', '').strip().replace('\t', '')
        if not sku: 
            return None

        has_score = row.get('评分') and row['评分'].strip()
        has_sales = row.get('销量') and row['销量'].strip() and row['销量'] != '0'
        
        if has_score and has_sales:
            return None

        try:
            url = f"https://club.jd.com/comment/productCommentSummaries.action"
            params = {'referenceIds': sku}
            resp = self.session.get(url, params=params, timeout=5)
            data = resp.json()
            
            if 'CommentsCount' in data and data['CommentsCount']:
                item_data = data['CommentsCount'][0]
                
                if not has_score:
                    rate = item_data.get('GoodRateShow', 0)
                    score = round(float(rate) * 5 / 100, 1)
                    row['评分'] = str(score)
                
                if not has_sales:
                    c_str = item_data.get('CommentCountStr', '')
                    c_num = item_data.get('CommentCount', 0)
                    if c_str and c_str != '0':
                        row['销量'] = c_str.replace('+', '')
                    elif c_num:
                        row['销量'] = str(c_num)
                        
            return row
        except:
            return None

    def run(self, csv_file, output_file):
        print(f"\n启动多线程补全 ({self.workers}线程)...")
        desktop = os.path.join(os.path.expanduser("~"), 'Desktop')
        input_path = os.path.join(desktop, csv_file)
        output_path = os.path.join(desktop, output_file)

        if not os.path.exists(input_path):
            print("文件不存在")
            return

        all_data = []
        with open(input_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            if '评分' not in fieldnames: 
                fieldnames = list(fieldnames) + ['评分']
            if '销量' not in fieldnames: 
                fieldnames = list(fieldnames) + ['销量']
            all_data = list(reader)

        print(f"总数据: {len(all_data)}条，正在分配任务...")

        processed_skus = set()
        if os.path.exists(output_path):
            with open(output_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    processed_skus.add(row.get('SKU', '').strip())
            print(f"历史已完成: {len(processed_skus)}条(跳过)")
        else:
            with open(output_path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

        tasks = []
        for row in all_data:
            sku = row.get('SKU', '').strip()
            if sku not in processed_skus:
                tasks.append(row)

        if not tasks:
            print("所有数据已完成")
            return

        print(f"开始并发处理 {len(tasks)}条任务...")
        
        count = 0
        success = 0
        
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {executor.submit(self.process_item, row): row for row in tasks}
            
            for future in as_completed(futures):
                count += 1
                result_row = future.result()
                original_row = futures[future]
                row_to_save = result_row if result_row else original_row
                
                with csv_lock:
                    try:
                        with open(output_path, 'a', encoding='utf-8-sig', newline='') as f:
                            writer = csv.DictWriter(f, fieldnames=fieldnames)
                            if not row_to_save['SKU'].startswith('\t'):
                                row_to_save['SKU'] = f"\t{row_to_save['SKU']}"
                            writer.writerow(row_to_save)
                    except: 
                        pass
                
                if result_row: 
                    success += 1
                
                if count % 50 == 0:
                    print(f"\r进度: {count}/{len(tasks)} | 成功补全: {success}", end="")

        print(f"\n\n全部完成！成功补全: {success}条")
        print(f"结果保存至: {output_file}")


if __name__ == '__main__':
    print("1. 采集数据(单线程)")
    print("2. 极速补全数据(多线程)")
    choice = input("请选择: ").strip()
    
    if choice == '1':
        JDProductScraper().run()
    else:
        csv_file = input("输入文件名[默认: 汽车零配件数据.csv]: ").strip() or '汽车零配件数据.csv'
        MultiThreadFiller(workers=32).run(
            csv_file=csv_file, 
            output_file=csv_file.replace('.csv', '_完整版.csv')
        )