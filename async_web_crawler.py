import aiohttp
import asyncio
import aiofiles
import time
import random
import json
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Set, Tuple, Optional, AsyncGenerator

class AsyncWebCrawler:
    def __init__(self, seed_urls: List[str], max_pages: int = 100000, output_dir: str = 'crawled_data', 
                 max_concurrency: int = 100, user_agents: Optional[List[str]] = None):
        self.seed_urls = seed_urls
        self.max_pages = max_pages
        self.output_dir = output_dir
        self.max_concurrency = max_concurrency
        self.visited_urls = set()  
        self.pending_urls = set(seed_urls)  
        self.crawled_pages = []  
        self.semaphore = asyncio.Semaphore(max_concurrency)  
        
        # 用户代理池
        self.user_agents = user_agents or [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 11.5; rv:90.0) Gecko/20100101 Firefox/90.0'
        ]
        
        # 创建输出目录
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        # 加载已保存的爬取数据
        self.load_crawler_state()
    
    def is_valid_url(self, url: str) -> bool:
        """检查URL是否有效"""
        try:
            parsed = urlparse(url)
            return bool(parsed.netloc) and bool(parsed.scheme)
        except ValueError:
            return False
    
    def get_random_user_agent(self) -> str:
        """获取随机用户代理"""
        return random.choice(self.user_agents)
    
    async def fetch_page(self, session: aiohttp.ClientSession, url: str) -> Tuple[Optional[str], List[str]]:
        """异步获取网页内容并提取链接"""
        headers = {'User-Agent': self.get_random_user_agent()}
        
        try:
            # 添加随机延迟，避免频繁请求
            await asyncio.sleep(random.uniform(0.1, 0.5))
            
            async with self.semaphore:
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status != 200:
                        print(f"无法访问 {url}: HTTP状态码 {response.status}")
                        return None, []
                    
                    # 检查内容类型
                    content_type = response.headers.get('Content-Type', '')
                    if 'html' not in content_type.lower():
                        print(f"{url} 不是HTML页面")
                        return None, []
                    
                    
                    content = await response.text()
                    
                    
                    soup = BeautifulSoup(content, 'html.parser')
                    
                    
                    text_content = soup.get_text()
                    
                    # 提取链接
                    links = []
                    for link in soup.find_all('a'):
                        href = link.get('href')
                        if href:
                            absolute_url = urljoin(url, href)
                            if self.is_valid_url(absolute_url):
                                links.append(absolute_url)
                    
                    return text_content, links
        
        except Exception as e:
            print(f"爬取 {url} 时出错: {e}")
            return None, []
    
    def extract_title(self, soup: BeautifulSoup) -> str:
        """提取网页标题"""
        title = soup.title
        return title.text.strip() if title else "无标题"
    
    async def process_url(self, session: aiohttp.ClientSession, url: str) -> None:
        """处理单个URL"""
        # 检查是否已访问
        if url in self.visited_urls:
            return
        
        print(f"爬取 #{len(self.visited_urls) + 1}: {url}")
        
        # 获取页面内容和链接
        content, links = await self.fetch_page(session, url)
        
        if content:
            
            soup = BeautifulSoup(content, 'html.parser')
            title = self.extract_title(soup)
            
            
            anchor_texts = []
            for link in soup.find_all('a'):
                anchor_text = link.get_text().strip()
                if anchor_text:
                    anchor_texts.append(anchor_text)
            
            # 保存页面数据
            page_id = len(self.crawled_pages) + 1
            page_data = {
                'id': page_id,
                'title': title,
                'url': url,
                'content': content,
                'anchor_texts': anchor_texts[:10]  
            }
            
            self.crawled_pages.append(page_data)
            
            
            await self.save_page_data(page_data)
            
            
            for link in links:
                if link not in self.visited_urls:
                    self.pending_urls.add(link)
        
        # 标记为已访问
        self.visited_urls.add(url)
    
    async def save_page_data(self, page_data: Dict) -> None:
        """异步保存单个页面数据到文件"""
        filename = os.path.join(self.output_dir, f"page_{page_data['id']}.json")
        async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(page_data, ensure_ascii=False, indent=2))
    
    def save_crawler_state(self) -> None:
        """保存爬虫状态"""
        state_file = os.path.join(self.output_dir, 'crawler_state.json')
        
        state = {
            'visited_urls': list(self.visited_urls),
            'pending_urls': list(self.pending_urls),
            'crawled_pages_count': len(self.crawled_pages)
        }
        
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    
    def load_crawler_state(self) -> None:
        """加载爬虫状态"""
        state_file = os.path.join(self.output_dir, 'crawler_state.json')
        
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                
                self.visited_urls = set(state.get('visited_urls', []))
                self.pending_urls = set(state.get('pending_urls', []))
                
                # 加载已爬取的页面ID
                crawled_pages_count = state.get('crawled_pages_count', 0)
                self.crawled_pages = []
                
                print(f"已加载爬虫状态: {len(self.visited_urls)} 个已访问URL, {len(self.pending_urls)} 个待访问URL")
            except Exception as e:
                print(f"加载爬虫状态失败: {e}")
    
    async def crawl(self) -> None:
        """异步爬取网页"""
        print(f"开始爬取网页，目标数量: {self.max_pages}，并发数: {self.max_concurrency}")
        
        # 创建会话
        async with aiohttp.ClientSession() as session:
            
            tasks = []
            for url in list(self.pending_urls)[:self.max_concurrency]:
                tasks.append(self.process_url(session, url))
            
            
            await asyncio.gather(*tasks)
            
            # 持续处理待爬取URL，直到达到最大页数或没有待爬取URL
            processed_count = len(self.visited_urls)
            while self.pending_urls and processed_count < self.max_pages:
                
                batch_size = min(self.max_concurrency, self.max_pages - processed_count, len(self.pending_urls))
                urls_to_process = [self.pending_urls.pop() for _ in range(batch_size)]
                
                
                tasks = [self.process_url(session, url) for url in urls_to_process]
                
                
                await asyncio.gather(*tasks)
                
                
                processed_count = len(self.visited_urls)
                
                
                if processed_count % 100 == 0:
                    self.save_crawler_state()
                    print(f"已爬取 {processed_count} 个页面，当前待处理队列: {len(self.pending_urls)}")
        
        # 最终保存爬取状态
        self.save_crawler_state()
        print(f"爬取完成! 共爬取 {len(self.visited_urls)} 个网页")

# 使用示例
if __name__ == "__main__":
    async def main():
        seed_urls = [
            "https://finance.nankai.edu.cn",
            "https://zfxy.nankai.edu.cn",
            "https://medical.nankai.edu.cn",
            "https://bs.nankai.edu.cn",
            "https://phil.nankai.edu.cn",
            "https://wxy.nankai.edu.cn",
            "https://chem.nankai.edu.cn",
            "https://physics.nankai.edu.cn",
            "https://cs.nankai.edu.cn",
            "https://history.nankai.edu.cn",
            "https://cc.nankai.edu.cn",
            "https://cyber.nankai.edu.cn",
            "https://math.nankai.edu.cn",
            "https://ai.nankai.edu.cn",
            "https://jsfz.nankai.edu.cn",
            "https://xb.nankai.edu.cn",
            "https://rsc.nankai.edu.cn",
            "https://shsj.nankai.edu.cn",
            "https://cwc.nankai.edu.cn",
            "https://jwc.nankai.edu.cn",
            "https://xgb.nankai.edu.cn",
            "https://wlaq.nankai.edu.cn",
            "https://graduate.nankai.edu.cn",
            "https://online.nankai.edu.cn",
            "https://zhgl.nankai.edu.cn",
            "https://less.nankai.edu.cn",
            "https://libic.nankai.edu.cn"

        ]
        
        crawler = AsyncWebCrawler(seed_urls, max_pages=110000, max_concurrency=70)
        await crawler.crawl()
    
    asyncio.run(main())    