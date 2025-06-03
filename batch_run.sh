#!/bin/bash

# --- 固定参数 ---
MAX_RESULTS=9999999
PDF_BASE_DIR="arxiv_downloaded_papers"
DL_WORKERS=6
DL_TIMEOUT=240
# 等等...

# --- 参数组合 ---
# 可以定义数组或直接在循环中使用

# 组合 1: 关键词数组和分类数组，两两组合
KEYWORDS=("formal methods AND lean" "formal methods AND coq" "formal methods AND Isabelle" "formal methods AND HOL" "formal methods AND agda" "formal methods AND Mizar"\
    "formal verification AND lean" "formal verification AND coq" "formal verification AND Isabelle" "formal verification AND HOL" "formal verification AND agda" "formal verification AND Mizar"\
    "proof assistant AND lean" "proof assistant AND coq" "proof assistant AND Isabelle" "proof assistant AND HOL" "proof assistant AND agda" "proof assistant AND Mizar"\
    "theorem prover AND lean" "theorem prover AND coq" "theorem prover AND Isabelle" "theorem prover AND HOL" "theorem prover AND agda" "theorem prover AND Mizar"\
    "interactive theorem proving AND lean" "interactive theorem proving AND coq" "interactive theorem proving AND Isabelle" "interactive theorem proving AND HOL" "interactive theorem proving AND agda" "interactive theorem proving AND Mizar")
CATEGORIES=("cs.AI" "cs.LG" "cs.LO" "cs.PL" "math.LO")

echo "开始批量运行 run_pipeline.sh..."

for keyword_item in "${KEYWORDS[@]}"; do
    for category_item in "${CATEGORIES[@]}"; do
        echo ""
        echo "---------------------------------------------------------------------"
        echo "正在运行: 关键词='${keyword_item}', 分类='${category_item}'"
        echo "---------------------------------------------------------------------"
    
    # 构建并执行命令
        ./run_pipeline.sh \
        --keyword "${keyword_item}" \
        --category "${category_item}" \
        --max-results "${MAX_RESULTS}" \
        --pdf-base-dir "${PDF_BASE_DIR}" \
        --dl-workers "${DL_WORKERS}" \
        --dl-timeout "${DL_TIMEOUT}" \
    # --disable-global-dedup # 如果需要禁用全局去重
    # 添加其他需要的固定或可变参数
    
    # 可选：在每次运行后暂停一小段时间，避免对API请求过于频繁
    echo "运行完成，暂停5秒..."
    sleep 5 
    done
done

echo ""
echo "所有批量任务已完成！"
