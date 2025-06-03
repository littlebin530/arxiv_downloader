#!/usr/bin/env python3
import json
import subprocess
import argparse
import os
import sys
import time
import logging
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')

def sanitize_filename(name):
    if not isinstance(name, str):
        logging.warning(f"sanitize_filename 接收到非字符串输入: {name} (类型: {type(name)})")
        return str(name) 
    return name.replace('/', '_')

def sanitize_for_dirname(text, max_len=50):
    if not isinstance(text, str): text = str(text)
    sanitized = re.sub(r'[^a-zA-Z0-9_.-]+', '_', text)
    sanitized = re.sub(r'_+', '_', sanitized)
    if sanitized.startswith('_'): sanitized = sanitized[1:]
    if sanitized.endswith('_'): sanitized = sanitized[:-1]
    return sanitized[:max_len] if max_len > 0 and len(sanitized) > max_len else sanitized

GLOBAL_EXISTING_PDFS = {}

def scan_global_output_directory(base_output_dir):
    # ... (此函数保持不变) ...
    global GLOBAL_EXISTING_PDFS
    GLOBAL_EXISTING_PDFS = {}
    logging.info(f"开始扫描全局输出目录: {base_output_dir} 以查找已存在的 PDF...")
    count = 0
    if not os.path.isdir(base_output_dir):
        logging.warning(f"全局扫描目录不存在: {base_output_dir}")
        return
    for pdf_path in Path(base_output_dir).rglob('*.pdf'):
        safe_entry_id_from_filename = pdf_path.stem
        GLOBAL_EXISTING_PDFS[safe_entry_id_from_filename] = str(pdf_path)
        count += 1
    logging.info(f"全局扫描完成。在 '{base_output_dir}' 中找到 {count} 个 PDF 文件。")


def measure_speed(url, timeout=5, read_size=10240):
    # ... (此函数保持不变) ...
    try:
        start_time = time.time()
        with urllib.request.urlopen(url, timeout=timeout) as response:
            response.read(read_size)
            elapsed = time.time() - start_time
            logging.debug(f"测速 {url}: {elapsed:.3f} 秒 (读取 {read_size} 字节)")
            return elapsed
    except urllib.error.URLError as e:
        logging.warning(f"连接 {url} 测速时出错 (URLError): {e}")
        return float('inf')
    except Exception as e:
        logging.warning(f"连接 {url} 测速时发生未知错误: {e}")
        return float('inf')

def choose_best_download_url(entry_id, primary_pdf_url):
    # ... (此函数保持不变) ...
    export_mirror_url = f"https://export.arxiv.org/pdf/{entry_id}"
    urls_to_test = {}
    if primary_pdf_url:
        urls_to_test["primary"] = primary_pdf_url
    if export_mirror_url and export_mirror_url != primary_pdf_url:
        urls_to_test["export_mirror"] = export_mirror_url
    
    if not urls_to_test:
        logging.warning(f"没有可用的URL进行测速或下载 {entry_id}。")
        return None

    if len(urls_to_test) == 1:
        chosen_url_key = list(urls_to_test.keys())[0]
        chosen_url = urls_to_test[chosen_url_key]
        logging.debug(f"只有一个可用下载源 ({chosen_url_key}) for {entry_id}: {chosen_url}")
        return chosen_url

    logging.info(f"正在为 {entry_id} 测试下载速度...")
    speeds = {}
    for name, url in urls_to_test.items():
        speeds[name] = measure_speed(url)
        logging.info(f"源 '{name}' ({url}) for {entry_id} 响应时间: {speeds[name]:.3f} 秒")

    best_source_name = None
    min_speed = float('inf')
    for name, speed_val in speeds.items():
        if speed_val < min_speed:
            min_speed = speed_val
            best_source_name = name
    
    if best_source_name and min_speed != float('inf'):
        chosen_url = urls_to_test[best_source_name]
        logging.info(f"为 {entry_id} 选择下载源: {best_source_name} ({chosen_url})，速度: {min_speed:.3f} 秒")
        return chosen_url
    else:
        logging.warning(f"所有源对 {entry_id} 测速失败。回退到主URL或镜像URL。")
        return primary_pdf_url if primary_pdf_url else export_mirror_url

def download_single_pdf(paper_details, sub_dir_path, download_source_preference="primary", single_file_timeout=300):
    # ... (此函数保持不变, 它返回 (entry_id, status, detail_message_or_path)) ...
    entry_id = paper_details.get('entry_id')
    primary_pdf_url = paper_details.get('pdf_url')
    title = paper_details.get('title', 'UnknownTitle')

    if not entry_id:
        msg = f"缺少 entry_id，无法处理。标题: {title}"
        logging.warning(msg)
        return None, "FAILED", msg

    safe_entry_id = sanitize_filename(entry_id) 
    output_filename = f"{safe_entry_id}.pdf"
    output_path_current_subdir = os.path.join(sub_dir_path, output_filename)

    if GLOBAL_EXISTING_PDFS and safe_entry_id in GLOBAL_EXISTING_PDFS:
        global_path = GLOBAL_EXISTING_PDFS[safe_entry_id]
        if Path(global_path).resolve() == Path(output_path_current_subdir).resolve():
            logging.info(f"PDF '{output_filename}' ({entry_id}) 已存在于当前子目录 '{sub_dir_path}'. 跳过。")
            return entry_id, "EXISTED_SUBDIR", str(output_path_current_subdir)
        else:
            logging.info(f"PDF '{output_filename}' ({entry_id}) 已在全局目录中找到: '{global_path}'. 跳过在 '{sub_dir_path}' 的下载。")
            return entry_id, "EXISTED_GLOBAL", str(global_path)

    if os.path.exists(output_path_current_subdir) and os.path.getsize(output_path_current_subdir) > 0:
        logging.info(f"PDF '{output_filename}' ({entry_id}) 已存在于当前子目录 '{sub_dir_path}' (独立检查). 跳过。")
        GLOBAL_EXISTING_PDFS[safe_entry_id] = str(output_path_current_subdir) 
        return entry_id, "EXISTED_SUBDIR", str(output_path_current_subdir)
        
    download_url_to_use = None
    if download_source_preference == "primary":
        download_url_to_use = primary_pdf_url
        if not download_url_to_use:
            download_url_to_use = f"https://export.arxiv.org/pdf/{entry_id}"
    elif download_source_preference == "export":
        download_url_to_use = f"https://export.arxiv.org/pdf/{entry_id}"
    elif download_source_preference == "fastest":
        download_url_to_use = choose_best_download_url(entry_id, primary_pdf_url)
    else:
        download_url_to_use = primary_pdf_url
        if not download_url_to_use:
             download_url_to_use = f"https://export.arxiv.org/pdf/{entry_id}"

    if not download_url_to_use:
        msg = f"无法确定 {entry_id} ({title}) 的下载链接。"
        logging.error(msg)
        return entry_id, "FAILED", msg

    logging.info(f"尝试从 {download_url_to_use} 下载: '{title}' ({entry_id}) 到 '{output_path_current_subdir}'，超时: {single_file_timeout}s")
    try:
        subprocess.run(
            ['wget', '-O', output_path_current_subdir, '-c', '-q', '--timeout=60', '--tries=2', download_url_to_use],
            check=True, capture_output=True, text=True, timeout=single_file_timeout
        )
        logging.info(f"成功下载 '{output_filename}' ({entry_id}) 到 '{sub_dir_path}'")
        GLOBAL_EXISTING_PDFS[safe_entry_id] = str(output_path_current_subdir)
        return entry_id, "DOWNLOADED", str(output_path_current_subdir)
    except subprocess.TimeoutExpired:
        msg = f"下载 {entry_id} ({download_url_to_use}) 超时 ({single_file_timeout}s)."
        logging.error(msg)
        if os.path.exists(output_path_current_subdir):
            try: os.remove(output_path_current_subdir); logging.debug(f"已删除超时的部分文件: {output_path_current_subdir}")
            except OSError: pass
        return entry_id, "FAILED", msg
    except subprocess.CalledProcessError as e:
        stderr_output = e.stderr.strip() if e.stderr else "N/A"
        msg = f"wget failed for {entry_id} ({download_url_to_use}). Code: {e.returncode}. Stderr: {stderr_output}"
        logging.error(msg)
        if os.path.exists(output_path_current_subdir):
            try: os.remove(output_path_current_subdir); logging.debug(f"已删除下载失败的部分文件: {output_path_current_subdir}")
            except OSError: pass
        return entry_id, "FAILED", msg
    except FileNotFoundError:
        msg = "wget command not found. Please ensure wget is installed and in your PATH."
        logging.error(msg)
        return entry_id, "FAILED", msg
    except Exception as e:
        msg = f"下载 {entry_id} ({download_url_to_use}) 发生未知错误: {e}"
        logging.error(msg, exc_info=True)
        if os.path.exists(output_path_current_subdir):
            try: os.remove(output_path_current_subdir); logging.debug(f"已删除未知错误的部分文件: {output_path_current_subdir}")
            except OSError: pass
        return entry_id, "FAILED", msg


def download_pdfs_from_json(json_file_path, base_output_dir, download_source_pref, single_file_timeout, max_workers):
    # ... (打开JSON, papers_to_download 等逻辑不变) ...
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        logging.error(f"JSON 文件未找到: {json_file_path}")
        return []
    except json.JSONDecodeError:
        logging.error(f"解码 JSON 文件错误: {json_file_path}")
        return []

    papers_to_download = data.get('papers', [])
    if not papers_to_download:
        logging.info(f"{json_file_path} 中没有列出要下载的论文。")
        return []

    # --- 构建子目录名 (保持之前的逻辑，基于 keyword 和 category) ---
    query_details = data.get('query_details', {})
    keyword_from_json = query_details.get('keyword', '')
    category_from_json = query_details.get('category', '')
    
    subdir_name_parts = []
    if keyword_from_json:
        subdir_name_parts.append(f"kw_{sanitize_for_dirname(keyword_from_json, 35)}")
    if category_from_json:
        subdir_name_parts.append(f"cat_{sanitize_for_dirname(category_from_json, 25)}")

    if subdir_name_parts:
        pdf_output_subdir_name = "_".join(filter(None, subdir_name_parts))
        if not pdf_output_subdir_name: 
            json_filename_stem_for_fallback = os.path.splitext(os.path.basename(json_file_path))[0]
            match_ts = re.search(r'(_\d{8}_\d{6})$', json_filename_stem_for_fallback)
            if match_ts:
                json_filename_stem_for_fallback = json_filename_stem_for_fallback[:match_ts.start()]
            pdf_output_subdir_name = sanitize_for_dirname(json_filename_stem_for_fallback, 60)
            if not pdf_output_subdir_name:
                 pdf_output_subdir_name = "unknown_query_parameters"
            logging.info(f"关键词/分类信息不足以构成有意义的目录名，回退到基于JSON文件名: {pdf_output_subdir_name}")
    else:
        logging.warning(f"在 {json_file_path} 中未找到有效的 keyword/category 信息，将使用JSON文件名构建子目录。")
        json_filename_stem_for_fallback = os.path.splitext(os.path.basename(json_file_path))[0]
        match_ts = re.search(r'(_\d{8}_\d{6})$', json_filename_stem_for_fallback)
        if match_ts:
            json_filename_stem_for_fallback = json_filename_stem_for_fallback[:match_ts.start()]
        pdf_output_subdir_name = sanitize_for_dirname(json_filename_stem_for_fallback, 60)
        if not pdf_output_subdir_name:
            pdf_output_subdir_name = "unknown_query_parameters"

    if not pdf_output_subdir_name: 
        pdf_output_subdir_name = "general_arxiv_downloads" 

    pdf_output_subdir = os.path.join(base_output_dir, pdf_output_subdir_name)
    # --- 子目录名构建结束 ---

    try:
        os.makedirs(pdf_output_subdir, exist_ok=True)
        logging.info(f"PDF将保存到子目录: {pdf_output_subdir}")
    except OSError as e:
        logging.error(f"无法创建目录 {pdf_output_subdir}: {e}")
        return papers_to_download

    subdir_metadata = {
        "source_json_file": os.path.basename(json_file_path),
        "output_subdirectory_name": os.path.basename(pdf_output_subdir), 
        "query_keyword_used_for_subdir_name": keyword_from_json, # 保存用于命名的原始信息
        "query_category_used_for_subdir_name": category_from_json, # 保存用于命名的原始信息
        "total_planned": len(papers_to_download),
        "downloaded_here_count": 0,
        "existed_in_subdir_count": 0,
        "skipped_global_duplicate_count": 0,
        "failed_download_count": 0,
        "details_downloaded_here": [],
        "details_existed_subdir": [],
        "details_skipped_global_duplicates": [],
        "details_failed_downloads": []
    }
    
    batch_failed_paper_details = [] 
    paper_map = {paper.get('entry_id'): paper for paper in papers_to_download if paper.get('entry_id')}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_entry_id = {
            executor.submit(download_single_pdf, paper, pdf_output_subdir, download_source_pref, single_file_timeout): paper.get('entry_id')
            for paper in papers_to_download if paper.get('entry_id')
        }

        for future in as_completed(future_to_entry_id):
            entry_id_from_future = future_to_entry_id[future]
            original_paper_details = paper_map.get(entry_id_from_future)

            try:
                processed_entry_id, status, detail = future.result()
                # ... (更新 subdir_metadata 的逻辑保持不变) ...
                if status == "DOWNLOADED":
                    subdir_metadata["downloaded_here_count"] += 1
                    if processed_entry_id: subdir_metadata["details_downloaded_here"].append({"entry_id": processed_entry_id, "path": detail})
                elif status == "EXISTED_SUBDIR":
                    subdir_metadata["existed_in_subdir_count"] += 1
                    if processed_entry_id: subdir_metadata["details_existed_subdir"].append({"entry_id": processed_entry_id, "path": detail})
                elif status == "EXISTED_GLOBAL":
                    subdir_metadata["skipped_global_duplicate_count"] += 1
                    if processed_entry_id:
                        subdir_metadata["details_skipped_global_duplicates"].append({
                            "entry_id": processed_entry_id,
                            "found_at_path": detail
                        })
                elif status == "FAILED":
                    subdir_metadata["failed_download_count"] += 1
                    if original_paper_details:
                        detail_msg = detail if detail else "Unknown error during download_single_pdf"
                        # 为了 retry.py，我们需要原始的 paper_details，而不仅仅是 entry_id 和 reason
                        # 同时，给原始的 paper_details 添加一个失败原因的字段
                        failure_record = original_paper_details.copy() # 浅拷贝
                        failure_record['_failure_reason'] = detail_msg
                        failure_record['_original_input_json'] = json_file_path # 记录来源
                        batch_failed_paper_details.append(failure_record)
                        
                        # 更新子目录元数据中的失败详情
                        subdir_metadata["details_failed_downloads"].append({
                            "entry_id": processed_entry_id or entry_id_from_future or "Unknown ID",
                            "reason": detail_msg
                        })
                    else:
                        logging.error(f"下载失败，且无法找到原始论文详情 for entry_id hint: {entry_id_from_future}. Error: {detail}")


            except Exception as exc:
                # ... (异常处理，确保添加 original_paper_details 到 batch_failed_paper_details) ...
                logging.error(f"论文 (ID提示: {entry_id_from_future}) 在并行下载中产生执行异常: {exc}", exc_info=True)
                subdir_metadata["failed_download_count"] += 1
                if original_paper_details:
                    failure_record = original_paper_details.copy()
                    failure_record['_failure_reason'] = f"Execution exception: {str(exc)}"
                    failure_record['_original_input_json'] = json_file_path
                    batch_failed_paper_details.append(failure_record)

                    subdir_metadata["details_failed_downloads"].append({
                        "entry_id": entry_id_from_future,
                        "reason": f"Execution exception: {str(exc)}"
                    })


    metadata_filename = "_metadata.json" 
    metadata_filepath = os.path.join(pdf_output_subdir, metadata_filename)
    try:
        with open(metadata_filepath, 'w', encoding='utf-8') as mf:
            json.dump(subdir_metadata, mf, indent=4, ensure_ascii=False)
        logging.info(f"子目录元数据已保存到: {metadata_filepath}")
    except IOError as e:
        logging.error(f"无法写入子目录元数据 {metadata_filepath}: {e}")
        
    logging.info(f"完成处理 {json_file_path}. 下载到此目录: {subdir_metadata['downloaded_here_count']}, "
                 f"已存在于此目录: {subdir_metadata['existed_in_subdir_count']}, "
                 f"全局重复跳过: {subdir_metadata['skipped_global_duplicate_count']}, "
                 f"失败: {subdir_metadata['failed_download_count']}.")
                 
    return batch_failed_paper_details


def process_directory(input_json_dir, base_pdf_output_dir, download_source_pref, single_file_timeout, max_workers):
    # ... (此函数保持不变) ...
    if not os.path.isdir(input_json_dir):
        logging.error(f"提供的输入 JSON 目录不是有效目录: {input_json_dir}")
        return []

    json_files_list = [f for f in os.listdir(input_json_dir) if f.endswith('.json') and
                  os.path.isfile(os.path.join(input_json_dir, f))]

    if not json_files_list:
        logging.warning(f"在目录中未找到 .json 文件: {input_json_dir}")
        return []
    
    overall_failed_downloads = []
    for json_file_name in json_files_list:
        full_json_path = os.path.join(input_json_dir, json_file_name)
        logging.info(f"--- 开始处理 JSON 文件: {json_file_name} ---")
        failed_list = download_pdfs_from_json(full_json_path, base_pdf_output_dir, download_source_pref, single_file_timeout, max_workers)
        if failed_list:
            overall_failed_downloads.extend(failed_list)
        logging.info(f"--- 完成处理 JSON 文件: {json_file_name} ---")
    
    return overall_failed_downloads

# --- MODIFICATION START: 失败日志追加逻辑 ---
def append_to_failed_log(failed_log_path, new_failures):
    """
    将新的失败记录追加到现有的失败日志JSON文件中。
    如果文件不存在，则创建它。
    失败记录会基于 entry_id 进行去重。
    """
    if not new_failures:
        return

    existing_failures_papers = []
    if os.path.exists(failed_log_path) and os.path.getsize(failed_log_path) > 0:
        try:
            with open(failed_log_path, 'r', encoding='utf-8') as f_log:
                log_data = json.load(f_log)
                existing_failures_papers = log_data.get('papers', [])
        except json.JSONDecodeError:
            logging.warning(f"无法解析现有的失败日志文件 {failed_log_path}。将创建一个新的。")
        except Exception as e:
            logging.error(f"读取现有失败日志 {failed_log_path} 时出错: {e}。将创建一个新的。")
    
    # 合并并去重
    # 使用字典确保 entry_id 的唯一性，后来的会覆盖早先的（如果paper_details不同的话）
    # 或者，如果只想添加新的，则需要检查 entry_id 是否已存在
    
    # 我们希望的是：如果一个 entry_id 已在 existing_failures_papers 中，就不再添加相同的 new_failure
    # 同时，如果 new_failures 中有重复的 entry_id，也只保留一个
    
    combined_failures_map = {paper.get('entry_id'): paper for paper in existing_failures_papers if paper.get('entry_id')}
    newly_added_count = 0
    
    for paper in new_failures:
        entry_id = paper.get('entry_id')
        if entry_id:
            if entry_id not in combined_failures_map: # 只添加真正新的失败项
                combined_failures_map[entry_id] = paper
                newly_added_count += 1
            else:
                # 可选：如果想更新已存在失败项的 _failure_reason 或 _original_input_json
                # combined_failures_map[entry_id]['_failure_reason'] = paper.get('_failure_reason', combined_failures_map[entry_id].get('_failure_reason'))
                # combined_failures_map[entry_id]['_original_input_json'] = paper.get('_original_input_json', combined_failures_map[entry_id].get('_original_input_json'))
                pass # 当前逻辑：如果已存在，不更新，不重复添加
        else: # 没有 entry_id 的失败项，直接添加（很难去重）
            # 这种项目理论上不应该发生，因为 download_single_pdf 会返回 None 作为 entry_id
            # 但如果发生了，我们还是记录一下
            existing_failures_papers.append(paper) # 实际上应该用 combined_failures_map 的 values
                                                # 但没有 entry_id 无法做 map key
                                                # 为了简单，我们假设所有 new_failures 都有 entry_id
                                                # 或者在收集 new_failures 时就过滤掉无 entry_id 的
            logging.warning(f"记录了一个没有entry_id的失败项: {paper.get('title', 'Unknown title')}")


    updated_papers_list = list(combined_failures_map.values())
    # 如果之前有无ID的失败项，需要把它们也加回来（如果它们不在combined_failures_map中）
    for paper in existing_failures_papers:
        if not paper.get('entry_id'):
            # 检查是否已通过其他方式（例如标题）存在于 updated_papers_list 中，避免重复添加无ID项
            # 这里简化：假设无ID项直接保留
            is_already_present = False
            for p_updated in updated_papers_list:
                if p_updated.get('title') == paper.get('title') and not p_updated.get('entry_id'):
                    is_already_present = True
                    break
            if not is_already_present:
                updated_papers_list.append(paper)


    if newly_added_count > 0 or not os.path.exists(failed_log_path): # 只有当有新条目添加或者文件原先不存在时才写
        logging.info(f"向 {failed_log_path} 追加/更新 {newly_added_count} 个新的失败记录。总计 {len(updated_papers_list)} 条。")
        failed_output_data = {
            "query_details": {
                "type": "accumulated_failed_downloads_log",
                "last_updated_timestamp": time.strftime("%Y-%m-%d %H:%M:%S %Z"),
            },
            "results_count": len(updated_papers_list),
            "papers": updated_papers_list
        }
        try:
            with open(failed_log_path, 'w', encoding='utf-8') as f_log:
                json.dump(failed_output_data, f_log, indent=4, ensure_ascii=False)
            logging.info(f"失败的下载详情已更新到: {failed_log_path}")
            if newly_added_count > 0:
                 print(f"\nINFO: {newly_added_count} 个新的下载失败已记录到 '{failed_log_full_path}'.")
        except IOError as e:
            logging.error(f"无法写入失败日志到 {failed_log_path}: {e}")
    elif existing_failures_papers and not new_failures : # 文件存在，但没有新的失败
        logging.info(f"本次运行没有新的下载失败。现有的失败日志 {failed_log_path} 包含 {len(existing_failures_papers)} 条记录。")
    else: # 文件不存在，且没有新的失败
        logging.info("本次运行没有下载失败。")

# --- MODIFICATION END: 失败日志追加逻辑 ---


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download arXiv PDFs based on JSON input.")
    # ... (参数定义保持不变) ...
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--input-json', help="Path to a single JSON file containing paper details.")
    group.add_argument('--input-dir', help="Path to a directory containing multiple JSON files.")
    parser.add_argument('--output-dir', required=True, help="Base directory to save downloaded PDFs and metadata.")
    parser.add_argument(
        '--download-source',
        choices=['primary', 'export', 'fastest'],
        default='export',
        help="Download source preference (default: export)."
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=300,
        help="Timeout in seconds for downloading a single PDF file (default: 300)."
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help="Number of parallel download workers (default: 4)."
    )
    parser.add_argument(
        '--failed-log',
        default='failed_downloads.json',
        help="Filename for the persistent log of failed downloads (default: failed_downloads.json). Will be placed in the PDF output base directory."
    )
    parser.add_argument(
        '--global-deduplication',
        action='store_true',
        help="Enable global deduplication by scanning the entire --output-dir before downloads."
    )
    
    args = parser.parse_args()

    pdf_output_dir = os.path.expanduser(os.path.expandvars(args.output_dir))
    try:
        os.makedirs(pdf_output_dir, exist_ok=True) # output-dir 也用作 failed-log 的存放目录
        logging.info(f"确保 PDF 输出目录存在: {pdf_output_dir}")
    except OSError as e:
        logging.error(f"无法创建 PDF 输出目录 {pdf_output_dir}: {e}")
        sys.exit(1)

    if args.global_deduplication:
        scan_global_output_directory(pdf_output_dir)
    else:
        logging.info("全局去重未启用。仅检查当前下载子目录中是否已存在文件。")

    # failed_log_full_path 现在由 append_to_failed_log 内部处理，基于 args.failed_log 和 args.output_dir
    failed_log_full_path = os.path.join(pdf_output_dir, args.failed_log) 
    
    current_run_failures = [] # 只收集当前运行的失败

    if args.input_json:
        input_json_path = os.path.expanduser(os.path.expandvars(args.input_json))
        logging.info(f"处理单个 JSON 文件: {input_json_path} | 源: {args.download_source} | 超时: {args.timeout}s | Workers: {args.workers}")
        failures_from_this_json = download_pdfs_from_json(input_json_path, pdf_output_dir, args.download_source, args.timeout, args.workers)
        if failures_from_this_json:
            current_run_failures.extend(failures_from_this_json)
    elif args.input_dir:
        input_json_dir_path = os.path.expanduser(os.path.expandvars(args.input_dir))
        logging.info(f"处理目录中的所有 JSON 文件: {input_json_dir_path} | 源: {args.download_source} | 超时: {args.timeout}s | Workers: {args.workers}")
        failures_from_dir = process_directory(input_json_dir_path, pdf_output_dir, args.download_source, args.timeout, args.workers)
        if failures_from_dir:
            current_run_failures.extend(failures_from_dir)

    # --- MODIFICATION START: 调用追加逻辑 ---
    if current_run_failures:
        logging.info(f"当前运行检测到 {len(current_run_failures)} 个下载失败，将尝试追加到日志。")
        append_to_failed_log(failed_log_full_path, current_run_failures)
    else:
        logging.info("当前运行没有检测到新的下载失败。")
        # 检查一下持久失败日志文件是否仍然有内容（如果它存在的话）
        if os.path.exists(failed_log_full_path) and os.path.getsize(failed_log_full_path) > 0:
             try:
                with open(failed_log_full_path, 'r', encoding='utf-8') as f_log_check:
                    log_data_check = json.load(f_log_check)
                    if log_data_check.get('papers'):
                        print(f"\nINFO: 当前运行无新失败，但持久失败日志 '{failed_log_full_path}' 仍包含未处理的条目。请使用 retry.py 处理。")
                    else: # 文件存在但 papers 列表为空或不存在
                        os.remove(failed_log_full_path) # 可以安全删除空的失败日志
                        logging.info(f"空的持久失败日志文件 '{failed_log_full_path}' 已被移除。")
             except Exception: # json解析错误等
                pass # 保持原样
        else:
            if os.path.exists(failed_log_full_path): # 文件存在但为空
                 os.remove(failed_log_full_path)
                 logging.info(f"空的持久失败日志文件 '{failed_log_full_path}' 已被移除。")


    logging.info("所有下载任务执行完毕。")
    # --- MODIFICATION END ---