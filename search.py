import argparse
import json
import os
import time
import arxiv 
import logging

# 设置基本日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def search_arxiv_papers(keyword, category, max_results=100):
    """
    使用官方 arXiv API 搜索并返回论文详细信息列表。
    """
    query = f"({keyword}) AND cat:{category}"
    logging.info(f"构建的 arXiv API 查询: {query}")

    try:
        logging.info("正在通过官方 API 请求 arXiv...")

        # 1. 创建 Client 实例 (推荐做法)
        client = arxiv.Client(
            page_size=100,     # API 分页时每页获取的数量
            delay_seconds=3,   # 两次 API 请求之间的最小延迟（秒），礼貌性设置
            num_retries=3      # 请求失败时的重试次数
        )

        # 2. 创建 Search 对象
        search = arxiv.Search(
            query=query,
            max_results=max_results, # 用户期望获取的总结果数
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Descending
        )

        papers_found = []
        
        # 3. 使用 client.results(search_object) 获取结果生成器
        # arxiv 库会根据 Search 对象中设置的 max_results 来限制返回的论文总数。
        results_generator = client.results(search)

        for result in results_generator:
            papers_found.append({
                "entry_id": result.entry_id.split('/')[-1],
                "title": result.title,
                "authors": [str(author) for author in result.authors],
                "summary": result.summary,
                "published_date": result.published.strftime('%Y-%m-%d %H:%M:%S %Z'),
                "updated_date": result.updated.strftime('%Y-%m-%d %H:%M:%S %Z'),
                "pdf_url": result.pdf_url,
                "primary_category": result.primary_category,
                "categories": result.categories
            })
            # 如果想在这里也手动限制，可以添加 (但通常不需要，Search 的 max_results 已处理)
            # if len(papers_found) >= max_results:
            #    logging.info(f"已达到请求的 max_results ({max_results})。停止获取更多论文。")
            #    break
        
        logging.info(f"成功检索到 {len(papers_found)} 篇论文的详细信息。")
        return papers_found

    except Exception as e:
        logging.error(f"arXiv 搜索过程中发生错误: {e}", exc_info=True)
        return []

def main():
    parser = argparse.ArgumentParser(description="搜索 arXiv 并将结果保存到 JSON。")
    parser.add_argument('--keyword', required=True, help="搜索关键词 (例如, 'language models')。")
    parser.add_argument('--category', required=True, help="arXiv 具体分类 (例如, 'cs.AI', 'cs.LO')。请使用具体子分类而非顶级分类如 'cs'。")
    parser.add_argument('--max-results', type=int, default=10, help="要获取的最大结果数。")
    parser.add_argument('--output-json', default='arxiv_search_results.json', help="输出 JSON 的文件名。")
    parser.add_argument('--json-dir', default='search_results_json', help="保存 JSON 文件的目录。")

    args = parser.parse_args()

    # 检查分类是否过于宽泛
    if args.category.lower() == "cs" or args.category.lower() == "math" or args.category.lower() == "physics" or args.category.lower() == "stat": # 等等
        logging.warning(f"警告: 您提供的分类 '{args.category}' 是一个顶级分类。arXiv API 通常期望更具体的子分类 (如 'cs.AI', 'math.AP') 以获得有效结果。您可能不会得到任何结果。")


    output_dir = os.path.expanduser(os.path.expandvars(args.json_dir))
    os.makedirs(output_dir, exist_ok=True)
    output_json_path = os.path.join(output_dir, args.output_json)

    logging.info(f"开始 arXiv 搜索，关键词='{args.keyword}', 分类='{args.category}', 最大结果数={args.max_results}。")
    start_time = time.time()

    papers = search_arxiv_papers(args.keyword, args.category, args.max_results)

    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"搜索完成，耗时 {elapsed_time:.2f} 秒。")

    if papers:
        output_data = {
            "query_details": {
                "keyword": args.keyword,
                "category": args.category,
                "max_results_requested": args.max_results,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S %Z"),
                "search_duration_seconds": round(elapsed_time, 2)
            },
            "results_count": len(papers),
            "papers": papers
        }
        try:
            with open(output_json_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)
            logging.info(f"搜索结果已成功保存到 {output_json_path}")
        except IOError as e:
            logging.error(f"无法将 JSON 写入 {output_json_path}: {e}")
    else:
        logging.warning(f"未找到符合 '{args.keyword}' 在分类 '{args.category}' 下的论文，或发生了错误。请检查您的分类是否足够具体。")
        output_data = {
            "query_details": {
                "keyword": args.keyword,
                "category": args.category,
                "max_results_requested": args.max_results,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S %Z"),
                "search_duration_seconds": round(elapsed_time, 2)
            },
            "results_count": 0,
            "papers": []
        }
        try:
            with open(output_json_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)
            logging.info(f"空结果集或错误指示已保存到 {output_json_path}")
        except IOError as e:
            logging.error(f"无法将空 JSON 写入 {output_json_path}: {e}")

if __name__ == "__main__":
    main()