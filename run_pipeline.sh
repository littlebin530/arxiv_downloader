#!/bin/bash

# --- 脚本配置 ---
set -e
set -o pipefail

# --- 用户可配置的路径和命令 (如果需要请修改) ---
PYTHON_CMD="python3"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
SEARCH_SCRIPT_PATH="${SCRIPT_DIR}/search.py"
DOWNLOAD_SCRIPT_PATH="${SCRIPT_DIR}/download.py"

# --- 可选参数的默认值 ---
DEFAULT_MAX_RESULTS=10
DEFAULT_JSON_OUTPUT_BASE_DIR="arxiv_search_data"
DEFAULT_PDF_OUTPUT_BASE_DIR="arxiv_downloaded_papers"
DEFAULT_DOWNLOAD_SOURCE="export"
DEFAULT_DOWNLOAD_WORKERS=4
DEFAULT_DOWNLOAD_TIMEOUT=300
DEFAULT_DOWNLOAD_FAILED_LOG_FILENAME="failed_downloads.json"
DEFAULT_GLOBAL_DEDUPLICATION="true"

# --- 日志目录和文件 ---
LOG_BASE_DIR="./logs/run_pipeline" # 基础日志目录
mkdir -p "$LOG_BASE_DIR" # 确保日志目录存在
CURRENT_TIME_FOR_LOG=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_BASE_DIR}/${CURRENT_TIME_FOR_LOG}.log" # 日志文件，使用 .log 后缀

# --- 辅助函数：用法信息 ---
usage() {
  echo "用法: $0 --keyword \"<搜索关键词>\" --category <arXiv分类> [可选参数]"
  echo ""
  echo "描述: 根据关键词和分类搜索arXiv论文并下载。全局去重功能默认启用。"
  echo "      所有控制台输出将被重定向到日志文件: ${LOG_BASE_DIR}/<时间戳>.log"
  echo ""
  echo "必需参数:"
  echo "  --keyword <字符串>        用于搜索的关键词。"
  echo "  --category <字符串>       具体的arXiv分类 (例如: cs.AI, math.AP)。"
  echo ""
  echo "可选参数:"
  echo "  --max-results <整数>      搜索时获取的最大论文数 (默认: ${DEFAULT_MAX_RESULTS})。"
  echo "  --json-base-dir <路径>    存储中间JSON文件的基础目录 (默认: ${DEFAULT_JSON_OUTPUT_BASE_DIR})。"
  echo "  --pdf-base-dir <路径>     存储下载的PDF文件的基础目录 (默认: ${DEFAULT_PDF_OUTPUT_BASE_DIR})。"
  echo "  --dl-source <选项>        PDF下载源 ('primary', 'export', 'fastest') (默认: ${DEFAULT_DOWNLOAD_SOURCE})。"
  echo "  --dl-workers <整数>       并行下载的工作线程数 (默认: ${DEFAULT_DOWNLOAD_WORKERS})。"
  echo "  --dl-timeout <整数>       单个PDF下载的超时时间 (秒) (默认: ${DEFAULT_DOWNLOAD_TIMEOUT})。"
  echo "  --dl-failed-log <文件名>  记录下载失败信息的文件名 (默认: ${DEFAULT_DOWNLOAD_FAILED_LOG_FILENAME})。保存在PDF基础目录中。"
  echo "  --disable-global-dedup    禁用全局去重 (默认启用)。"
  echo "  --help                      显示此帮助信息并退出。"
  echo ""
  echo "示例:"
  echo "  $0 --keyword \"transformer\" --category cs.LG --dl-workers 8 --disable-global-dedup"
  exit 1
}


# --- 参数解析 ---
KEYWORD_ARG=""
CATEGORY_ARG=""
MAX_RESULTS_ARG="${DEFAULT_MAX_RESULTS}"
JSON_BASE_DIR_ARG="${DEFAULT_JSON_OUTPUT_BASE_DIR}"
PDF_BASE_DIR_ARG="${DEFAULT_PDF_OUTPUT_BASE_DIR}"
DOWNLOAD_SOURCE_ARG="${DEFAULT_DOWNLOAD_SOURCE}"
DOWNLOAD_WORKERS_ARG="${DEFAULT_DOWNLOAD_WORKERS}"
DOWNLOAD_TIMEOUT_ARG="${DEFAULT_DOWNLOAD_TIMEOUT}"
DOWNLOAD_FAILED_LOG_FILENAME_ARG="${DEFAULT_DOWNLOAD_FAILED_LOG_FILENAME}"
SHOULD_GLOBAL_DEDUP_BE_ENABLED="${DEFAULT_GLOBAL_DEDUPLICATION}"

while [[ "$#" -gt 0 ]]; do
  case $1 in
    --keyword) KEYWORD_ARG="$2"; shift ;;
    --category) CATEGORY_ARG="$2"; shift ;;
    --max-results) MAX_RESULTS_ARG="$2"; shift ;;
    --json-base-dir) JSON_BASE_DIR_ARG="$2"; shift ;;
    --pdf-base-dir) PDF_BASE_DIR_ARG="$2"; shift ;;
    --dl-source) DOWNLOAD_SOURCE_ARG="$2"; shift ;;
    --dl-workers) DOWNLOAD_WORKERS_ARG="$2"; shift ;;
    --dl-timeout) DOWNLOAD_TIMEOUT_ARG="$2"; shift ;;
    --dl-failed-log) DOWNLOAD_FAILED_LOG_FILENAME_ARG="$2"; shift ;;
    --disable-global-dedup) SHOULD_GLOBAL_DEDUP_BE_ENABLED="false";;
    --help) usage ;;
    *) echo "错误：未知的参数: $1"; usage ;; # 中文
  esac
  shift
done

if [ -z "$KEYWORD_ARG" ] || [ -z "$CATEGORY_ARG" ]; then
  echo "错误：缺少必需参数 --keyword 和/或 --category。" # 中文
  usage
fi

# --- 执行主要流程的函数 ---
main_pipeline() {
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

    sanitize_for_path() {
        local input_string="$1"; local max_len="${2:-50}"; local sanitized_string
        sanitized_string=$(echo "$input_string" | sed 's/[^a-zA-Z0-9_.-]/_/g' | sed 's/__*/_/g' | sed 's/^_//g' | sed 's/_$//g')
        echo "${sanitized_string:0:$max_len}"
    }
    SAFE_KEYWORD=$(sanitize_for_path "$KEYWORD_ARG")
    SAFE_CATEGORY=$(sanitize_for_path "$CATEGORY_ARG")

    CURRENT_RUN_JSON_DIR="${JSON_BASE_DIR_ARG}/${SAFE_CATEGORY}/${SAFE_KEYWORD}_${TIMESTAMP}"
    CURRENT_RUN_JSON_FILENAME="search_results.json"
    FULL_JSON_OUTPUT_PATH="${CURRENT_RUN_JSON_DIR}/${CURRENT_RUN_JSON_FILENAME}"

    mkdir -p "$CURRENT_RUN_JSON_DIR"
    echo "信息：中间JSON数据将存储在: $(realpath "$CURRENT_RUN_JSON_DIR" 2>/dev/null || echo "$CURRENT_RUN_JSON_DIR")" # 中文
    mkdir -p "$PDF_BASE_DIR_ARG"
    echo "信息：下载的PDF将存储在以下目录的子目录中: $(realpath "$PDF_BASE_DIR_ARG" 2>/dev/null || echo "$PDF_BASE_DIR_ARG")" # 中文

    # --- 步骤 1: 搜索 ---
    echo ""; echo "========================= 步骤 1: 正在搜索 arXiv =========================" # 中文
    echo "关键词: ${KEYWORD_ARG}, 分类: ${CATEGORY_ARG}, 最大结果数: ${MAX_RESULTS_ARG}" # 中文
    echo "JSON 输出路径: $(realpath "$FULL_JSON_OUTPUT_PATH" 2>/dev/null || echo "$FULL_JSON_OUTPUT_PATH")" # 中文
    echo "-------------------------------------------------------------------------------"
    "$PYTHON_CMD" "$SEARCH_SCRIPT_PATH" \
      --keyword "$KEYWORD_ARG" \
      --category "$CATEGORY_ARG" \
      --max-results "$MAX_RESULTS_ARG" \
      --json-dir "$CURRENT_RUN_JSON_DIR" \
      --output-json "$CURRENT_RUN_JSON_FILENAME"

    if [ ! -f "$FULL_JSON_OUTPUT_PATH" ]; then
        echo "错误：search.py 未能创建JSON文件: $FULL_JSON_OUTPUT_PATH"; exit 1 # 中文
    fi
    echo "成功：search.py 已完成。" # 中文

    # --- 步骤 2: 下载 ---
    FULL_FAILED_LOG_PATH="${PDF_BASE_DIR_ARG}/${DOWNLOAD_FAILED_LOG_FILENAME_ARG}"

    echo ""; echo "========================= 步骤 2: 正在下载 PDF ========================" # 中文
    echo "输入 JSON: $(realpath "$FULL_JSON_OUTPUT_PATH" 2>/dev/null || echo "$FULL_JSON_OUTPUT_PATH")" # 中文
    echo "PDF 输出目录: $(realpath "$PDF_BASE_DIR_ARG" 2>/dev/null || echo "$PDF_BASE_DIR_ARG")" # 中文
    echo "下载源: ${DOWNLOAD_SOURCE_ARG}" # 中文
    echo "工作线程数: ${DOWNLOAD_WORKERS_ARG}" # 中文
    echo "超时时间 (单个文件): ${DOWNLOAD_TIMEOUT_ARG}s" # 中文
    echo "失败日志将位于: $(realpath "$FULL_FAILED_LOG_PATH" 2>/dev/null || echo "$FULL_FAILED_LOG_PATH (将被创建)")" # 中文
    if [ "${SHOULD_GLOBAL_DEDUP_BE_ENABLED}" = "true" ]; then
        echo "全局去重: 已启用 (默认)" # 中文
    else
        echo "全局去重: 已禁用" # 中文
    fi
    echo "-------------------------------------------------------------------------------"

    DOWNLOAD_PY_ARGS=(
      --input-json "$FULL_JSON_OUTPUT_PATH"
      --output-dir "$PDF_BASE_DIR_ARG"
      --download-source "$DOWNLOAD_SOURCE_ARG"
      --workers "$DOWNLOAD_WORKERS_ARG"
      --timeout "$DOWNLOAD_TIMEOUT_ARG"
      --failed-log "$DOWNLOAD_FAILED_LOG_FILENAME_ARG"
    )

    if [ "${SHOULD_GLOBAL_DEDUP_BE_ENABLED}" = "true" ]; then
        DOWNLOAD_PY_ARGS+=(--global-deduplication)
    fi

    "$PYTHON_CMD" "$DOWNLOAD_SCRIPT_PATH" "${DOWNLOAD_PY_ARGS[@]}"

    echo "成功：download.py 已完成。" # 中文

    # --- 流水线结束 ---
    echo ""; echo "========================= 流水线处理完成 =========================" # 中文
    echo "搜索元数据: $(realpath "$FULL_JSON_OUTPUT_PATH" 2>/dev/null || echo "$FULL_JSON_OUTPUT_PATH")" # 中文
    echo "PDF位于以下目录的子目录中: $(realpath "$PDF_BASE_DIR_ARG" 2>/dev/null || echo "$PDF_BASE_DIR_ARG")" # 中文
    if [ -f "$FULL_FAILED_LOG_PATH" ] && [ -s "$FULL_FAILED_LOG_PATH" ]; then
        echo "警告：部分下载可能已失败。请检查日志: $(realpath "$FULL_FAILED_LOG_PATH" 2>/dev/null || echo "$FULL_FAILED_LOG_PATH")" # 中文
        echo "         你可以使用此文件作为 download.py 的 --input-json 参数进行重试。" # 中文
    else
        if [ -f "$FULL_FAILED_LOG_PATH" ]; then
          rm -f "$FULL_FAILED_LOG_PATH"
          echo "信息：所有下载成功或无新失败，空的失败日志已移除。" # 中文
        else
          echo "信息：所有下载看起来都成功了 (未生成失败日志或日志为空)。" # 中文
        fi
    fi
    echo "======================================================================"
} # --- End of main_pipeline function ---


# --- 执行主要逻辑并重定向所有输出 ---
echo "流水线运行开始于 $(date)" > "$LOG_FILE" # 中文
echo "执行命令: $0 $@" >> "$LOG_FILE" # 中文
echo "完整的日志输出将位于: $LOG_FILE" # 中文
echo "你可以使用以下命令监控日志文件: tail -f $LOG_FILE" # 中文
echo "----------------------------------------------------"
echo 

{
    main_pipeline "$@"
} >> "$LOG_FILE" 2>&1

exit 0