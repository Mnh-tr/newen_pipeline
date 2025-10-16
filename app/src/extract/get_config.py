import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_dir)
os.chdir(project_dir)

import yaml
import pathlib
import polars as pl

from loguru import logger
from datetime import datetime, date

from src.utils.helper import get_monday_of_week, file_filter
from src.utils.gc_bigquery import GCBigquery

log_time = datetime.today().strftime("%Y-%m-%dT%H-%M-%S%z")
PARTITION_DATE = get_monday_of_week().strftime("%Y-%m-%d")
TODAY = date.today().strftime("%Y-%m-%d")
PATH_CONFIG = f"./configs/tiktok/{PARTITION_DATE}/{TODAY}"

LOG_DIR = f"./logs/{PARTITION_DATE}/scrape_tiktok_video/get_config"
def init_dir():
    pathlib.Path(PATH_CONFIG).mkdir(parents=True, exist_ok=True)
    pathlib.Path(LOG_DIR).mkdir(parents=True, exist_ok=True)


init_dir()
logger.remove()
logger.add(sys.stderr, level="DEBUG")

logger.add(
    f"{LOG_DIR}/get_config_log_{PARTITION_DATE}_{log_time}.log", 
    level="DEBUG",
    format="{time} {level} {message}", 
    filter=file_filter, 
    mode="a",
    encoding="utf-8"
)

def fetch_tiktok_config(config_path: str) -> bool:
    """Lấy config TikTok từ BigQuery và lưu vào file txt"""
    try:
        logger.bind(save=True).info("Fetching TikTok config from BigQuery...")
        
        gbq = GCBigquery()
        query = """
            SELECT *
            FROM `newen-455007.tiktok_search_keyword_re_scrape_config.all`
            WHERE job_date = CURRENT_DATE('Asia/Seoul')
            ORDER BY region, first_seen_date DESC
        """
        
        pdf = gbq.execute_query(query)
        
        if pdf is None or len(pdf) == 0:
            logger.bind(save=True).warning("No data returned from query")
            return False
        
        output_path = os.path.join(config_path, "config_tiktokweb.txt")
        pdf.write_csv(output_path, separator="\t")
        
        logger.bind(save=True).info(f"Saved {len(pdf)} rows to {output_path}")
        return True
        
    except Exception as e:
        logger.bind(save=True).error(f"Error fetching config: {e}", exc_info=True)
        return False


def extract_urls_to_yaml(config_path: str) -> bool:
    """Trích xuất share_url ra file YAML"""
    try:
        logger.bind(save=True).info("Extracting share_urls to YAML...")
        
        input_path = os.path.join(config_path, "config_tiktokweb.txt")
        
        if not os.path.exists(input_path):
            logger.bind(save=True).error(f"File not found: {input_path}")
            return False
        
        df = pl.read_csv(input_path, separator="\t")
        share_urls = df["share_url"].drop_nulls().to_list()
        
        output_path = os.path.join(config_path, "tiktok_share_urls.yaml")
        with open(output_path, 'w') as f:
            yaml.dump(share_urls, f, default_flow_style=False)
        
        logger.bind(save=True).info(f"Saved {len(share_urls)} URLs to {output_path}")
        return True
        
    except Exception as e:
        logger.bind(save=True).error(f"Error extracting URLs: {e}", exc_info=True)
        return False


def main(config_path: str) -> bool:
    """Chạy toàn bộ quy trình"""
    if not fetch_tiktok_config(config_path):
        return False
    
    if not extract_urls_to_yaml(config_path):
        return False
    
    logger.bind(save=True).info("Process completed successfully")
    return True


if __name__ == "__main__":
    main(PATH_CONFIG)