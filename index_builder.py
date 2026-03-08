import os
import json
from typing import List, Dict, Any
from search_engine_backend import SearchEngine

def load_documents_from_directory(data_dir: str) -> List[Dict[str, Any]]:
    """从目录加载网页文档"""
    documents = []
    page_files = [f for f in os.listdir(data_dir) if f.startswith('page_') and f.endswith('.json')]
    
    for filename in page_files:
        file_path = os.path.join(data_dir, filename)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                doc = json.load(f)
                documents.append(doc)
        except Exception as e:
            print(f"加载文件 {filename} 失败: {e}")
    
    return documents

def build_index(data_dir: str = 'crawled_data', index_dir: str = 'search_data'):
    """构建搜索引擎索引"""
    print(f"从 {data_dir} 加载网页文档...")
    documents = load_documents_from_directory(data_dir)
    print(f"共加载 {len(documents)} 个网页文档")
    
    if not documents:
        print("没有找到网页文档，无法构建索引")
        return
    
    
    engine = SearchEngine()
    
    # 构建索引
    engine.build_index(documents)
    
    print(f"索引已构建完成并保存到 {index_dir}")

if __name__ == "__main__":
    # 构建索引
    build_index()    