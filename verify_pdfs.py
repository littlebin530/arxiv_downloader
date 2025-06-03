#!/usr/bin/env python3
import argparse
import json
import os
import logging
from pathlib import Path
from PyPDF2 import PdfReader # PyPDF2 >= 3.0.0
import time
# 如果你用的是旧版 PyPDF2 (如 1.x 或 2.x)，导入是 from PyPDF2 import PdfFileReader

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def sanitize_filename(name):
    return name.replace('/', '_')

# 假设 sanitize_filename 函数与 download.py 中的一致
def sanitize_filename_for_comparison(name_part):
    """用于从文件名反推 entry_id (如果 entry_id 就是文件名主体)"""
    # 这个函数需要根据你的 download.py 中 sanitize_filename 的具体实现来调整
    # 如果 sanitize_filename 只是 name.replace('/', '_')
    # 那么这里可能不需要太多操作，或者需要一个反向的映射（如果更复杂）
    # 简单假设文件名（不含.pdf）就是 safe_entry_id
    return name_part

def get_entry_id_from_filename(pdf_filename_stem):
    """
    尝试从PDF文件名（不含扩展名）推断原始 entry_id。
    这依赖于 download.py 中是如何用 entry_id 生成文件名的。
    如果 sanitize_filename 只是将 '/' 替换为 '_', 则这里可以做相应处理。
    """
    # 这是一个示例，你可能需要根据你的 sanitize_filename 调整
    # 假设 sanitize_filename(entry_id) 的结果是 pdf_filename_stem
    # 如果 entry_id 中本身不包含 '_' (新版ID) 或只在特定位置有 '_' (旧版ID反转)
    # 这里简单返回 stem，假设它就是 safe_entry_id，后续查找 URL 时再想办法
    return pdf_filename_stem # 实际上这只是 safe_entry_id

def find_original_paper_details(safe_entry_id_to_find, search_results_jsons_dir):
    """
    (可选功能) 遍历 search.py 生成的 JSON 文件，尝试找到原始论文信息 (如 pdf_url)。
    Args:
        safe_entry_id_to_find (str): 从损坏PDF文件名得到的 safe_entry_id。
        search_results_jsons_dir (str): 存放 search.py 输出的 JSON 文件的目录。
    Returns:
        dict: 包含原始 paper_details 的字典，如果找到的话，否则为 None。
    """
    if not search_results_jsons_dir or not os.path.isdir(search_results_jsons_dir):
        return None

    for root, _, files in os.walk(search_results_jsons_dir):
        for filename in files:
            if filename.endswith('.json'):
                json_path = os.path.join(root, filename)
                try:
                    with open(json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    papers = data.get('papers', [])
                    for paper in papers:
                        entry_id = paper.get('entry_id')
                        if entry_id:
                            # 比较 sanitize_filename(entry_id) 与 safe_entry_id_to_find
                            if sanitize_filename(entry_id) == safe_entry_id_to_find:
                                return paper # 找到了匹配的原始信息
                except Exception as e:
                    logging.debug(f"处理JSON文件 {json_path} 时出错: {e}")
    return None


def verify_and_cleanup_pdfs(pdf_root_dir, min_size_kb=10, output_log_dir="corrupted_pdf_logs", search_json_root_dir=None, dry_run=False):
    """
    遍历PDF目录，检查PDF完整性，删除不完整文件，并记录信息。
    Args:
        pdf_root_dir (str): 要检查的PDF根目录。
        min_size_kb (int): 文件大小的最小阈值 (KB)，小于此值初步认为不完整。
        output_log_dir (str): 保存损坏PDF信息的目录。
        search_json_root_dir (str, optional): search.py 输出的JSON文件的根目录，用于查找原始pdf_url。
        dry_run (bool): 如果为True，则只报告不删除文件。
    """
    if not os.path.isdir(pdf_root_dir):
        logging.error(f"错误：PDF根目录 '{pdf_root_dir}' 不是有效目录。")
        return

    os.makedirs(output_log_dir, exist_ok=True)
    corrupted_files_log_path = os.path.join(output_log_dir, f"corrupted_pdfs_{time.strftime('%Y%m%d_%H%M%S')}.json")
    
    corrupted_pdf_records = []
    min_size_bytes = min_size_kb * 1024

    logging.info(f"开始检查 '{pdf_root_dir}' 中的PDF文件...")
    if dry_run:
        logging.info("注意：当前为 DRY RUN 模式，不会删除任何文件。")

    pdf_files_to_check = list(Path(pdf_root_dir).rglob('*.pdf'))
    total_pdfs = len(pdf_files_to_check)
    logging.info(f"找到 {total_pdfs} 个PDF文件进行检查。")

    for i, pdf_path_obj in enumerate(pdf_files_to_check):
        pdf_path = str(pdf_path_obj)
        pdf_filename_stem = pdf_path_obj.stem # 文件名（不含.pdf）
        relative_path = str(pdf_path_obj.relative_to(pdf_root_dir))
        
        logging.debug(f"检查 ({i+1}/{total_pdfs}): {pdf_path}")
        is_corrupted = False
        corruption_reason = ""

        try:
            # 1. 检查文件大小
            file_size = os.path.getsize(pdf_path)
            if file_size < min_size_bytes:
                is_corrupted = True
                corruption_reason = f"文件太小 ({file_size / 1024:.2f} KB)，小于阈值 {min_size_kb} KB。"
                logging.warning(f"标记损坏 (大小): {pdf_path} - {corruption_reason}")

            # 2. 如果大小通过，尝试用 PyPDF2 打开
            if not is_corrupted:
                try:
                    # PdfReader(strict=False) 可以对某些轻微损坏更宽容
                    # 但如果目的是检测不完整下载，strict=True 可能更好
                    reader = PdfReader(pdf_path, strict=True) 
                    if len(reader.pages) == 0: # 没有页面的PDF也认为不完整
                        is_corrupted = True
                        corruption_reason = "PDF解析成功但没有页面。"
                        logging.warning(f"标记损坏 (无页面): {pdf_path} - {corruption_reason}")
                    else:
                        # logging.debug(f"PDF有效: {pdf_path}, 页数: {len(reader.pages)}")
                        pass # PDF 看起来是好的

                except Exception as e: # PyPDF2 可能会抛出多种异常
                    is_corrupted = True
                    corruption_reason = f"PyPDF2解析失败: {type(e).__name__} - {str(e)}"
                    logging.warning(f"标记损坏 (解析): {pdf_path} - {corruption_reason}")
        
        except FileNotFoundError:
            logging.error(f"文件在检查时未找到 (可能已被其他进程删除?): {pdf_path}")
            continue # 跳过此文件
        except Exception as e:
            is_corrupted = True # 其他意外错误也标记为损坏
            corruption_reason = f"检查过程中发生未知错误: {str(e)}"
            logging.error(f"标记损坏 (未知错误): {pdf_path} - {corruption_reason}")


        if is_corrupted:
            record = {
                "original_filepath": pdf_path,
                "relative_filepath": relative_path,
                "corruption_reason": corruption_reason,
                "safe_entry_id": pdf_filename_stem, # 这是从文件名来的，可能是 sanitize_filename(entry_id)
                "retrieved_entry_id": None, # 如果能从其他地方获取原始ID
                "retrieved_pdf_url": None,  # 如果能找到原始URL
                "retrieved_title": None     # 如果能找到原始标题
            }
            
            # (可选) 尝试从 search.py 的JSON输出中查找原始信息
            if search_json_root_dir:
                original_details = find_original_paper_details(pdf_filename_stem, search_json_root_dir)
                if original_details:
                    record["retrieved_entry_id"] = original_details.get('entry_id')
                    record["retrieved_pdf_url"] = original_details.get('pdf_url')
                    record["retrieved_title"] = original_details.get('title')
                    logging.info(f"为 {pdf_filename_stem} 找到原始信息: URL={record['retrieved_pdf_url']}")
                else:
                    logging.warning(f"未能为 {pdf_filename_stem} 在 '{search_json_root_dir}' 中找到原始下载信息。")
            
            corrupted_pdf_records.append(record)

            if not dry_run:
                try:
                    os.remove(pdf_path)
                    logging.info(f"已删除损坏的PDF: {pdf_path}")
                except OSError as e:
                    logging.error(f"删除文件 {pdf_path} 失败: {e}")
            else:
                logging.info(f"[DRY RUN] 将会删除: {pdf_path}")

    # 保存损坏文件记录
    if corrupted_pdf_records:
        logging.info(f"共找到 {len(corrupted_pdf_records)} 个损坏/不完整的PDF文件。")
        try:
            # 将记录包装在一个类似 search.py 输出的结构中，方便 download.py 重用
            output_data_for_retry = {
                "query_details": {
                    "type": "corrupted_pdfs_retry_list",
                    "source_pdf_directory_checked": pdf_root_dir,
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S %Z"),
                },
                "results_count": len(corrupted_pdf_records),
                # 我们需要将 corrupted_pdf_records 转换成 download.py 能理解的 'papers' 列表
                # 每个 'paper' 至少需要 'entry_id' 和 'pdf_url'
                "papers": [
                    {
                        "entry_id": rec.get("retrieved_entry_id") or rec["safe_entry_id"], # 优先用检索到的原始ID
                        "title": rec.get("retrieved_title") or f"Corrupted PDF: {rec['safe_entry_id']}",
                        "pdf_url": rec.get("retrieved_pdf_url") or f"https://export.arxiv.org/pdf/{rec.get('retrieved_entry_id') or rec['safe_entry_id']}", # 尝试构造URL
                        # 可以添加其他原始details中的字段，如果找到了的话
                        "original_filepath_when_corrupted": rec["original_filepath"],
                        "corruption_reason": rec["corruption_reason"]
                    }
                    for rec in corrupted_pdf_records if rec.get("retrieved_pdf_url") or rec.get("retrieved_entry_id") or rec.get("safe_entry_id") # 只记录能构造重试信息的
                ]
            }
            # 过滤掉那些实在无法构造重试信息的
            output_data_for_retry["papers"] = [p for p in output_data_for_retry["papers"] if p.get("entry_id") and p.get("pdf_url")]
            output_data_for_retry["results_count"] = len(output_data_for_retry["papers"])


            if output_data_for_retry["papers"]:
                with open(corrupted_files_log_path, 'w', encoding='utf-8') as f_log:
                    json.dump(output_data_for_retry, f_log, indent=4, ensure_ascii=False)
                logging.info(f"损坏/不完整的PDF信息已记录到: {corrupted_files_log_path}")
                print(f"\nINFO: 损坏/不完整的PDF信息已记录到 '{corrupted_files_log_path}'.")
                print(f"       你可以使用此文件作为 download.py 的 --input-json 参数来尝试重新下载。")
            else:
                logging.info("找到了损坏的PDF，但无法为它们构造有效的重试信息（缺少entry_id或pdf_url）。")

        except IOError as e:
            logging.error(f"无法写入损坏PDF日志到 {corrupted_files_log_path}: {e}")
    else:
        logging.info("在指定目录中未发现损坏/不完整的PDF文件。")

    logging.info("PDF完整性检查和清理完成。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="检查PDF文件的完整性，删除损坏文件并记录信息。")
    parser.add_argument("pdf_directory", help="包含PDF文件的根目录 (会递归搜索)。")
    parser.add_argument("--min-size-kb", type=int, default=10,
                        help="文件大小的最小阈值 (KB)，小于此值初步认为不完整 (default: 10KB)。")
    parser.add_argument("--log-dir", default="corrupted_pdf_logs",
                        help="保存损坏PDF信息的目录 (default: corrupted_pdf_logs)。")
    parser.add_argument("--search-json-dir", default=None,
                        help="(可选) search.py 输出的JSON文件的根目录，用于尝试查找原始pdf_url和entry_id。")
    parser.add_argument("--dry-run", action="store_true",
                        help="只报告问题，不实际删除文件。")
    
    args = parser.parse_args()

    verify_and_cleanup_pdfs(args.pdf_directory, 
                            min_size_kb=args.min_size_kb, 
                            output_log_dir=args.log_dir,
                            search_json_root_dir=args.search_json_dir,
                            dry_run=args.dry_run)