import os
import re
import math
import json
from collections import defaultdict, Counter
from typing import List, Dict, Set, Tuple, Union

class SearchEngine:
    def __init__(self):
        # 索引数据结构
        self.index = defaultdict(dict)  
        self.documents = {} 
        self.doc_lengths = {}  
        self.doc_count = 0  
        self.term_doc_freq = defaultdict(int)  
        self.pagerank = {} 
        
        # 索引域权重配置
        self.field_weights = {
            'title': 3.0,
            'content': 1.0,
            'url': 1.5,
            'anchor_texts': 2.0
        }
        
        # 数据存储路径
        self.data_dir = 'search_data'
        self.index_file = os.path.join(self.data_dir, 'index.json')
        self.documents_file = os.path.join(self.data_dir, 'documents.json')
        self.doc_lengths_file = os.path.join(self.data_dir, 'doc_lengths.json')
        self.term_doc_freq_file = os.path.join(self.data_dir, 'term_doc_freq.json')
        self.pagerank_file = os.path.join(self.data_dir, 'pagerank.json')
        
        # 加载已存在的数据
        self.load_data()
    
    def preprocess_text(self, text: str) -> List[str]:
        """预处理文本：转换为小写、分词、去除停用词"""
        
        stopwords = set(['the', 'and', 'of', 'to', 'a', 'in', 'that', 'for', 'on', 'with', 'as', 'at', 'by'])
        
        
        tokens = re.findall(r'\b\w+\b', text.lower())
        
        
        return [token for token in tokens if token not in stopwords]
    
    def build_index(self, documents: List[Dict]):
        """构建索引"""
        print("开始构建索引...")
        
        # 构建基本索引
        for doc in documents:
            doc_id = doc['id']
            self.documents[doc_id] = {
                'title': doc['title'],
                'url': doc['url'],
                'content': doc['content'],
                'anchor_texts': doc.get('anchor_texts', [])
            }
            
           
            field_terms = {
                'title': self.preprocess_text(doc['title']),
                'content': self.preprocess_text(doc['content']),
                'url': self.preprocess_text(doc['url']),
                'anchor_texts': self.preprocess_text(' '.join(doc.get('anchor_texts', [])))
            }
            
            # 计算加权词频
            term_freq = defaultdict(float)
            for field, terms in field_terms.items():
                weight = self.field_weights[field]
                for term in terms:
                    term_freq[term] += weight
            
            # 更新倒排索引
            for term, tf in term_freq.items():
                self.index[term][doc_id] = tf
            
           
            self.doc_lengths[doc_id] = sum(tf for tf in term_freq.values())
            
            self.doc_count += 1
        
        # 计算词项的文档频率
        for term in self.index:
            self.term_doc_freq[term] = len(self.index[term])
        
        # 构建链接图并计算PageRank
        self._build_link_graph_and_compute_pagerank()
        
        
        self.save_data()
        
        print(f"索引构建完成！共索引 {self.doc_count} 个文档")
    
    def _build_link_graph_and_compute_pagerank(self, damping_factor: float = 0.85, max_iterations: int = 100, tolerance: float = 1e-6):
        """构建链接图并计算PageRank"""
        print("正在计算PageRank...")
        
        
        url_to_doc_id = {doc['url']: doc_id for doc_id, doc in self.documents.items()}
        
        
        adjacency_list = defaultdict(set)
        
       
        url_pattern = re.compile(r'https?://[^\s,]+')
        
        for doc_id, doc in self.documents.items():
            
            content_links = url_pattern.findall(doc['content'])
            for link in content_links:
                if link in url_to_doc_id:
                    adjacency_list[doc_id].add(url_to_doc_id[link])
            
            # 从锚文本中提取链接（假设锚文本中的URL是第一个词）
            for anchor_text in doc.get('anchor_texts', []):
                first_word = anchor_text.split()[0]
                if first_word.startswith('http') and first_word in url_to_doc_id:
                    adjacency_list[doc_id].add(url_to_doc_id[first_word])
        
       
        initial_pagerank = 1.0 / self.doc_count
        self.pagerank = {doc_id: initial_pagerank for doc_id in self.documents}
        
        # 迭代计算PageRank
        for _ in range(max_iterations):
            new_pagerank = {}
            max_diff = 0
            
            for doc_id in self.documents:
                # 基本PageRank公式: PR(u) = (1-d)/N + d * Σ(PR(v)/L(v))
                sum_incoming = 0
                for incoming_doc_id, outgoing_links in adjacency_list.items():
                    if doc_id in outgoing_links:
                        sum_incoming += self.pagerank[incoming_doc_id] / len(outgoing_links)
                
                new_pr = (1 - damping_factor) / self.doc_count + damping_factor * sum_incoming
                new_pagerank[doc_id] = new_pr
                
                max_diff = max(max_diff, abs(new_pr - self.pagerank[doc_id]))
            
            self.pagerank = new_pagerank
            
            # 检查收敛
            if max_diff < tolerance:
                break
        
        print("PageRank计算完成")
    
    def save_data(self):
        """保存索引数据到文件"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        
        with open(self.index_file, 'w', encoding='utf-8') as f:
            json.dump(self.index, f)
        
        with open(self.documents_file, 'w', encoding='utf-8') as f:
            json.dump(self.documents, f)
        
        with open(self.doc_lengths_file, 'w', encoding='utf-8') as f:
            json.dump(self.doc_lengths, f)
        
        with open(self.term_doc_freq_file, 'w', encoding='utf-8') as f:
            json.dump(self.term_doc_freq, f)
        
        with open(self.pagerank_file, 'w', encoding='utf-8') as f:
            json.dump(self.pagerank, f)
    
    def load_data(self):
        """从文件加载索引数据"""
        try:
            if os.path.exists(self.index_file):
                with open(self.index_file, 'r', encoding='utf-8') as f:
                    self.index = defaultdict(dict, json.load(f))
            
            if os.path.exists(self.documents_file):
                with open(self.documents_file, 'r', encoding='utf-8') as f:
                    self.documents = json.load(f)
                    self.doc_count = len(self.documents)
            
            if os.path.exists(self.doc_lengths_file):
                with open(self.doc_lengths_file, 'r', encoding='utf-8') as f:
                    self.doc_lengths = json.load(f)
            
            if os.path.exists(self.term_doc_freq_file):
                with open(self.term_doc_freq_file, 'r', encoding='utf-8') as f:
                    self.term_doc_freq = defaultdict(int, json.load(f))
            
            if os.path.exists(self.pagerank_file):
                with open(self.pagerank_file, 'r', encoding='utf-8') as f:
                    self.pagerank = json.load(f)
            
            if self.doc_count > 0:
                print(f"成功加载索引数据，包含 {self.doc_count} 个文档")
        except Exception as e:
            print(f"加载索引数据失败: {e}")
    
    def calculate_tf_idf(self, term: str, doc_id: int) -> float:
        """计算词项在文档中的TF-IDF值"""
        if term not in self.index or doc_id not in self.index[term]:
            return 0
        
        
        tf = self.index[term][doc_id]
        
        
        df = self.term_doc_freq[term]
        idf = math.log((self.doc_count + 1) / (df + 1)) + 1
        
        return tf * idf
    
    def vector_space_search(self, query: str, limit: int = 10) -> List[Dict]:
        """向量空间模型搜索"""
        
        query_terms = self.preprocess_text(query)
        if not query_terms:
            return []
        
        
        query_vector = Counter(query_terms)
        
        # 查找包含任何查询词的文档
        candidate_docs = set()
        for term in query_terms:
            if term in self.index:
                candidate_docs.update(self.index[term].keys())
        
        
        doc_scores = {}
        for doc_id in candidate_docs:
            score = 0
            for term in query_terms:
                # 查询词的权重 (TF-IDF)
                query_tf = query_vector[term]
                query_idf = math.log((self.doc_count + 1) / (self.term_doc_freq.get(term, 0) + 1)) + 1
                query_weight = query_tf * query_idf
                
                # 文档词的权重 (TF-IDF)
                doc_weight = self.calculate_tf_idf(term, doc_id)
                
                
                score += query_weight * doc_weight
            
            
            if self.doc_lengths.get(doc_id, 0) > 0:
                score /= self.doc_lengths[doc_id]
            
            # 结合PageRank
            pagerank_score = self.pagerank.get(doc_id, 0.15 / self.doc_count) if self.doc_count > 0 else 0.15
            combined_score = score * 0.7 + pagerank_score * 0.3
            
            doc_scores[doc_id] = combined_score
        
        # 排序并返回结果
        sorted_docs = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)[:limit]
        
        results = []
        for doc_id, score in sorted_docs:
            doc = self.documents.get(doc_id, {})
            results.append({
                'id': doc_id,
                'title': doc.get('title', ''),
                'url': doc.get('url', ''),
                'content': doc.get('content', ''),
                'anchor_texts': doc.get('anchor_texts', []),
                'pagerank': self.pagerank.get(doc_id, 0),
                'relevance': score,
                'score': score
            })
        
        return results    