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

# --- 从 download.py 复制必要的函数和全局变量 ---
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
    export_mirror_url = f"https://export.arxiv.org/pdf/{entry_id}"
    urls_to_test = {}
    if primary_pdf_url:
        urls_to_test["primary"] = primary_pdf_url
    if export_mirror_url and export_mirror_url != primary_pdf_url:
        urls_to_test["export_mirror"] = export_mirror_url
    if not urls_to_test: return None
    if len(urls_to_test) == 1: 
        chosen_url = list(urls_to_test.values())[0]
        logging.debug(f"只有一个可用下载源 ({list(urls_to_test.keys())[0]}) for {entry_id}: {chosen_url}")
        return chosen_url
    logging.info(f"正在为 {entry_id} 测试下载速度...")
    speeds = {name: measure_speed(url) for name, url in urls_to_test.items()}
    for name, speed_val in speeds.items(): logging.info(f"源 '{name}' ({urls_to_test[name]}) for {entry_id} 响应时间: {speed_val:.3f} 秒")
    best_source_name = min(speeds, key=speeds.get, default=None)
    if best_source_name and speeds[best_source_name] != float('inf'):
        chosen_url = urls_to_test[best_source_name]
        logging.info(f"为 {entry_id} 选择下载源: {best_source_name} ({chosen_url})，速度: {speeds[best_source_name]:.3f} 秒")
        return chosen_url
    logging.warning(f"所有源对 {entry_id} 测速失败。回退。")
    return primary_pdf_url if primary_pdf_url else export_mirror_url

def download_single_pdf(paper_details, sub_dir_path, download_source_preference="primary", single_file_timeout=300):
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

def determine_target_subdir(paper_details, base_output_dir, default_subdir_name="retry_downloads"):
    original_input_json_path = paper_details.get('_original_input_json')
    keyword = None
    category = None

    if original_input_json_path and os.path.exists(original_input_json_path):
        try:
            with open(original_input_json_path, 'r', encoding='utf-8') as f_orig:
                original_data = json.load(f_orig)
            query_details = original_data.get('query_details', {})
            keyword = query_details.get('keyword')
            category = query_details.get('category')
        except Exception as e:
            logging.warning(f"无法从 {original_input_json_path} 加载原始查询详情: {e}")
    
    if not keyword: keyword = paper_details.get('keyword_from_query') 
    if not category: category = paper_details.get('category_from_query') or paper_details.get('primary_category')

    subdir_name_parts = []
    if keyword:
        subdir_name_parts.append(f"kw_{sanitize_for_dirname(keyword, 35)}")
    if category:
        subdir_name_parts.append(f"cat_{sanitize_for_dirname(category, 25)}")

    if subdir_name_parts:
        subdir_name = "_".join(filter(None, subdir_name_parts))
        if not subdir_name: subdir_name = default_subdir_name
    else:
        subdir_name = default_subdir_name
        
    return os.path.join(base_output_dir, subdir_name)


def retry_failed_downloads(failed_log_path, base_output_dir, download_source_pref, single_file_timeout, max_workers, enable_global_dedup):
    if not os.path.exists(failed_log_path) or os.path.getsize(failed_log_path) == 0:
        logging.info(f"失败日志文件 '{failed_log_path}' 不存在或为空。无需重试。")
        return

    # --- MODIFICATION: 根据参数启用全局扫描 ---
    if enable_global_dedup:
        scan_global_output_directory(base_output_dir) # base_output_dir 是 retry.py 的 --output-dir
    else:
        logging.info("全局去重未启用 (retry.py)。仅检查当前下载子目录中是否已存在文件。")
        GLOBAL_EXISTING_PDFS.clear() # 确保是空的
    # --- END MODIFICATION ---

    try:
        with open(failed_log_path, 'r', encoding='utf-8') as f_log:
            data = json.load(f_log)
    except Exception as e:
        logging.error(f"无法读取或解析失败日志文件 '{failed_log_path}': {e}")
        return

    papers_to_retry = data.get('papers', [])
    if not papers_to_retry:
        logging.info(f"'{failed_log_path}' 中没有列出需要重试的论文。")
        return

    logging.info(f"找到 {len(papers_to_retry)} 篇论文在 '{failed_log_path}' 中等待重试。")

    still_failed_after_retry = []
    successfully_retried_count = 0
    
    paper_map_for_retry = {paper.get('entry_id'): paper for paper in papers_to_retry if paper.get('entry_id')}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_entry_id_retry = {}
        for paper in papers_to_retry:
            entry_id = paper.get('entry_id')
            if not entry_id:
                logging.warning(f"跳过重试，因为条目缺少 entry_id: {paper.get('title', 'Unknown title')}")
                still_failed_after_retry.append(paper)
                continue

            target_subdir_for_retry = determine_target_subdir(paper, base_output_dir)
            try: # 尝试创建目录，如果失败则记录此paper为失败
                os.makedirs(target_subdir_for_retry, exist_ok=True)
            except OSError as e:
                logging.error(f"无法为 {entry_id} 创建目标子目录 {target_subdir_for_retry}: {e}")
                still_failed_after_retry.append(paper)
                continue
            
            future_to_entry_id_retry[
                executor.submit(download_single_pdf, paper, target_subdir_for_retry, download_source_pref, single_file_timeout)
            ] = entry_id

        for future in as_completed(future_to_entry_id_retry):
            entry_id_from_future = future_to_entry_id_retry[future]
            original_failed_paper_details = paper_map_for_retry.get(entry_id_from_future)

            try:
                processed_entry_id, status, detail = future.result()
                if status in ["DOWNLOADED", "EXISTED_SUBDIR", "EXISTED_GLOBAL"]:
                    successfully_retried_count += 1
                    logging.info(f"重试成功: {processed_entry_id} (状态: {status})")
                else: # FAILED
                    logging.warning(f"重试失败: {processed_entry_id or 'Unknown ID'}. 原因: {detail}")
                    if original_failed_paper_details:
                        original_failed_paper_details['_retry_attempt_failed_reason'] = detail
                        original_failed_paper_details['_last_retry_timestamp'] = time.strftime("%Y%m%d_%H%M%S")
                        still_failed_after_retry.append(original_failed_paper_details)
            except Exception as exc:
                logging.error(f"论文 (ID提示: {entry_id_from_future}) 在重试下载中产生执行异常: {exc}", exc_info=True)
                if original_failed_paper_details:
                    original_failed_paper_details['_retry_attempt_failed_reason'] = f"Execution exception: {str(exc)}"
                    original_failed_paper_details['_last_retry_timestamp'] = time.strftime("%Y%m%d_%H%M%S")
                    still_failed_after_retry.append(original_failed_paper_details)
    
    logging.info(f"重试完成。成功下载/找到: {successfully_retried_count}。仍然失败: {len(still_failed_after_retry)}。")

    if still_failed_after_retry:
        logging.info(f"正在更新失败日志 '{failed_log_path}'，剩余 {len(still_failed_after_retry)} 个条目。")
        updated_failed_data = {
            "query_details": {
                "type": "accumulated_failed_downloads_log",
                "last_retry_run_timestamp": time.strftime("%Y-%m-%d %H:%M:%S %Z"),
            },
            "results_count": len(still_failed_after_retry),
            "papers": still_failed_after_retry
        }
        try:
            with open(failed_log_path, 'w', encoding='utf-8') as f_log:
                json.dump(updated_failed_data, f_log, indent=4, ensure_ascii=False)
            print(f"\nINFO: 失败日志 '{failed_log_path}' 已更新。仍有 {len(still_failed_after_retry)} 篇论文下载失败。")
        except IOError as e:
            logging.error(f"无法更新失败日志 '{failed_log_path}': {e}")
    elif os.path.exists(failed_log_path):
        logging.info(f"所有在 '{failed_log_path}' 中的论文均已成功重试。正在删除该文件。")
        try:
            os.remove(failed_log_path)
            print(f"\nINFO: 失败日志 '{failed_log_path}' 中的所有论文均已处理，文件已删除。")
        except OSError as e:
            logging.error(f"无法删除空的失败日志 '{failed_log_path}': {e}")


if __name__ == "__main__":
    retry_parser = argparse.ArgumentParser(
        description="Retry downloading PDFs listed in a failed_downloads.json file. Global deduplication is ENABLED by default.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter # 显示默认值
    )
    retry_parser.add_argument(
        '--failed-log-file', 
        required=True, 
        help="Path to the failed_downloads.json file generated by download.py."
    )
    retry_parser.add_argument(
        '--output-dir', 
        required=True, 
        help="Base directory where PDFs should be downloaded (must match original --output-dir from download.py)."
    )
    retry_parser.add_argument(
        '--download-source',
        choices=['primary', 'export', 'fastest'],
        default='export', # 保持与 download.py 一致的默认值
        help="Download source preference."
    )
    retry_parser.add_argument(
        '--timeout',
        type=int,
        default=300, # 保持与 download.py 一致的默认值
        help="Timeout in seconds for downloading a single PDF file."
    )
    retry_parser.add_argument(
        '--workers',
        type=int,
        default=4, # 保持与 download.py 一致的默认值
        help="Number of parallel download workers."
    )
    # --- MODIFICATION: 修改全局去重参数，使其默认启用 ---
    retry_parser.add_argument(
        '--disable-global-dedup', # 参数变为禁用
        action='store_false',    # 当出现此参数时，dest 的值会是 False
        dest='enable_global_deduplication', # 将值存储到 enable_global_deduplication
        help="Disable global deduplication (it's enabled by default for retry)."
    )
    retry_parser.set_defaults(enable_global_deduplication=True) # 设置 enable_global_deduplication 的默认值为 True
    # --- END MODIFICATION ---

    retry_args = retry_parser.parse_args()

    retry_failed_downloads(
        retry_args.failed_log_file,
        retry_args.output_dir,
        retry_args.download_source,
        retry_args.timeout,
        retry_args.workers,
        retry_args.enable_global_deduplication # 传递解析后的值
    )

    logging.info("Retry script finished.")