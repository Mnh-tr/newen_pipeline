import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_dir)
os.chdir(project_dir)

import csv
import pathlib
import polars as pl
import traceback
from datetime import datetime, date
from typing import Dict, List, Optional
from loguru import logger

from src.utils.helper import (
    FileFormat,
    read_file,
    save_file,
    get_monday_of_week,
    file_filter,
    
)
from src.utils.gc_storage import GCStorage
from src.utils.gc_bigquery import GCBigquery

log_time = datetime.today().strftime("%Y-%m-%dT%H-%M-%S%z")
PARTITION_DATE = get_monday_of_week().strftime("%Y-%m-%d")
TABLE_DATE = get_monday_of_week().strftime("%Y%m%d")
TODAY = date.today().strftime("%Y-%m-%d")
# TODAY = "2025-10-13"

RAW_DATA_PATH = f"./data_tiktok_video/{PARTITION_DATE}/{TODAY}/raw_data/video"
HTML_DATA_PATH = f"data_tiktok_video/{PARTITION_DATE}/{TODAY}/html"
LOG_DIR = f"./logs/{PARTITION_DATE}/scrape_tiktok_video/transform_data/{TODAY}"
CONFIG_DATA_PATH= f"./configs/tiktok/{PARTITION_DATE}/{TODAY}/config_tiktokweb.tsv"
PATH_TRANSFORM_DATA = f"data_tiktok_video/{PARTITION_DATE}/{TODAY}/transform_data"


def init_dir():
    pathlib.Path(RAW_DATA_PATH).mkdir(parents=True, exist_ok=True)
    pathlib.Path(PATH_TRANSFORM_DATA).mkdir(parents=True, exist_ok=True)
    pathlib.Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
    pathlib.Path(HTML_DATA_PATH).mkdir(parents=True, exist_ok=True)

init_dir()

logger.remove()
logger.add(sys.stderr, level="DEBUG")

logger.add(
    f"{LOG_DIR}/transform_log_{PARTITION_DATE}_{log_time}.log", 
    level="DEBUG",
    format="{time} {level} {message}", 
    filter=file_filter, 
    mode="a",
    encoding="utf-8"
)

def merge_data_transform(path_transform_data=PATH_TRANSFORM_DATA):
    """
    Gộp toàn bộ file JSON trong thư mục transform_data thành một list duy nhất.
    """
    all_data = []
    logger.bind(save=True).info(f"START: merge_data_transform from {path_transform_data}")

    for root, _, files in os.walk(path_transform_data):
        for file in files:
            if not file.endswith(".json"):
                continue

            file_path = os.path.join(root, file)
            try:
                data = read_file(file_path, FileFormat.JSON)

                # chuẩn hóa data thành list
                if isinstance(data, dict):
                    data = [data]
                elif not isinstance(data, list):
                    logger.bind(save=True).warning(f"Unexpected data type in {file_path}: {type(data)}")
                    continue

                all_data.extend(data)

            except Exception as e:
                logger.bind(save=True).error(f"Failed to read {file_path}: {e}")
                logger.debug(traceback.format_exc())

    logger.bind(save=True).info(f"TOTAL MERGED RECORDS: {len(all_data)}")
    return all_data


def main():
    try:
        # get data
        print(TODAY)
        full_data = merge_data_transform(PATH_TRANSFORM_DATA)
        schema = [
            # --- Metadata / Config ---
            ("period_web", pl.Utf8),
            ("share_url", pl.Utf8),
            ("aweme_id", pl.Utf8),
            ("job_date", pl.Utf8),
            ("period", pl.Utf8),
            ("region", pl.Utf8),
            ("first_seen_date", pl.Utf8),
            ("create_time", pl.Utf8),
            ("rescrape_3d", pl.Int8),
            ("rescrape_7d", pl.Int8),

            # --- Video Info ---
            ("createTime_web", pl.Utf8),           # vẫn là str (vì hiện bạn lưu epoch dạng chuỗi)
            ("desc", pl.Utf8),
            ("is_ads", pl.Boolean),
            ("video_cover", pl.Utf8),
            ("video_originCover", pl.Utf8),

            # --- Author Info ---
            ("author_id", pl.Utf8),
            ("author_verified", pl.Utf8),
            ("author_uniqueId", pl.Utf8),
            ("author_nickname", pl.Utf8),
            ("author_followerCount", pl.Utf8),     # hiện vẫn là string trong dữ liệu
            ("author_followingCount", pl.Utf8),
            ("author_heart", pl.Utf8),
            ("author_heartCount", pl.Utf8),
            ("author_videoCount", pl.Utf8),
            ("author_diggCount", pl.Utf8),
            ("author_friendCount", pl.Utf8),

            # --- Music Info ---
            ("music_id", pl.Utf8),
            ("music_title", pl.Utf8),
            ("music_playUrl", pl.Utf8),
            ("music_authorName", pl.Utf8),

            # --- Stats Info ---
            ("stats_diggCount", pl.Utf8),
            ("stats_shareCount", pl.Utf8),
            ("stats_commentCount", pl.Utf8),
            ("stats_playCount", pl.Utf8),
            ("stats_collectCount", pl.Utf8),
            ("stats_repostCount", pl.Utf8),
        ]

        logger.bind(save=True).info("START: UPLOAD TIKTOK WEB DATA")
        # upload bigquery
        gbg = GCBigquery()
        df = pl.DataFrame(full_data, schema=schema, strict=False)
        # ép kiểu sau khi tạo df
        df = df.with_columns([
            pl.col("job_date").str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
            pl.col("first_seen_date").str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
            pl.col("create_time").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f", strict=False),
        ])
        df_no_duplicates = df.unique()
        logger.bind(save=True).debug(f"Products to upload: {len(df_no_duplicates)} (dedup from {len(df)})")
        # comment đoạn này để test.
        gbg.upload_dataframe(
            df=df_no_duplicates,
            project="newen-455007",
            destination=f"tiktok_search_keyword_re_scrape_test.{TABLE_DATE}",
            format="parquet",
            mode="append",
            use_legacy=True
        )
        logger.bind(save=True).success("Upload BigQuery Success")
    except Exception as e:
        logger.bind(save=True).error(f"BigQuery upload failed: {e}")
        logger.debug(traceback.format_exc())
        raise  


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.debug(traceback.format_exc())
        raise