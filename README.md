./run_pipeline.sh --keyword "reinforcement learning" --category "cs.LG"
python verify_pdfs.py ./arxiv_downloaded_papers --search-json-dir ./arxiv_search_data --log-dir ./log/verification_logs
python retry.py --failed-log-file ./arxiv_downloaded_papers/failed_downloads.json --output-dir ./arxiv_downloaded_papers
