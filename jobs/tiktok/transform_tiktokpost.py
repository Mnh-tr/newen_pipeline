import sys
import os
current_dir = os.getcwd()
if "newen_pipeline" in current_dir:
    parts = current_dir.split("newen_pipeline")
    root_path = parts[0] + "newen_pipeline"
    new_path = os.path.join(root_path)
    sys.path.insert(0, new_path)
    os.chdir(new_path)
import json
from datetime import datetime, date
import asyncio
from typing import Any, Dict, List, Optional, Sequence, Union
from fake_http_header import FakeHttpHeader
# from configs.constant import KEYWORDS_BNBG

from loguru import logger
import pathlib
import polars as pl
import random
import traceback
import httpx
from src.tiktokweb import(
    get_proxy,
    reset_proxy,
    split_processed_configs,
    split_processed_configs_tiktok_post
)
from utils.helper import (
    FileFormat,
    read_file,
    save_file,
    get_monday_of_week,
    file_filter,
    
)
from utils import (
    GCStorage,
    GCBigquery
)
from urllib.parse import urlparse, parse_qs, unquote, quote
import re
import csv
from bs4 import BeautifulSoup
log_time = datetime.today().strftime("%Y-%m-%dT%H-%M-%S%z")
PARTITION_DATE = get_monday_of_week().strftime("%Y-%m-%d")
TODAY = date.today().strftime("%Y-%m-%d")

RAW_DATA_PATH = f"./data_tiktok_video/{PARTITION_DATE}/{TODAY}/raw_data/video"
HTML_DATA_PATH = f"data_tiktok_video/{PARTITION_DATE}/{TODAY}/html"
PRODUCT_IDS = read_file("configs/product_id_tiktok_video.yaml", FileFormat.YAML)
LOG_DIR = f"./logs/{PARTITION_DATE}/scrape_tiktok_video"

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

def deep_get(d, path: list, default=None):
    """Truy cập an toàn vào dict lồng nhau."""
    for key in path:
        if isinstance(d, dict):
            d = d.get(key, default)
        else:
            return default
    return d

def transform_item_data(data_raw: dict,  config_dict: Optional[dict] = None) -> dict:
    """
    Trích xuất thông tin cơ bản từ JSON TikTok, hỗ trợ cả 2 loại:
    - data.itemInfo.itemStruct
    - data.__DEFAULT_SCOPE__.webapp.video-detail.itemInfo.itemStruct
    """
    try:
        # --- Truy cập itemStruct an toàn ---
        data = deep_get(
            data_raw,
            ["data", "__DEFAULT_SCOPE__", "webapp.video-detail", "itemInfo", "itemStruct"],
            {}
        )


        # Nếu không có, fallback về cấu trúc loại 1
        if not data:
            data = deep_get(data_raw, ["data", "itemInfo", "itemStruct"], {})
        

        share_url = data_raw.get("share_url")
        config_data = config_dict.get(share_url, {})
        # Lấy thông tin 
        aweme_id = data.get("id")
        createTime = data.get("createTime")
        desc = data.get("desc")
        
        # lấy thông tin video
        video = data.get("video", {}) or {} 
        video_cover = video.get("cover")
        video_originCover = video.get("originCover")


        # Lấy thông tin tác giả
        author = data.get("author", {}) or {}
        author_id = author.get("id")
        author_uniqueId = author.get("uniqueId")
        author_nickname = author.get("nickname")

        # --- AuthorStats info ---
        author_stats = data.get("authorStatsV2", {}) or {}
        author_followerCount = author_stats.get("followerCount")
        author_followingCount = author_stats.get("followingCount")

        # --- Music info ---
        music = data.get("music", {}) or {}
        music_id = music.get("id")
        music_title = music.get("title")
        music_playUrl = music.get("playUrl")
        music_authorName = music.get("authorName")

        # --- Stats info (statsV2) ---
        stats = data.get("statsV2", {}) or {}
        stats_diggCount = stats.get("diggCount")
        stats_shareCount = stats.get("shareCount")
        stats_commentCount = stats.get("commentCount")
        stats_playCount = stats.get("playCount")
        stats_collectCount = stats.get("collectCount")
        stats_repostCount = stats.get("repostCount")


        # orther
        isAd = data.get("isAd")
        
        result = {
            "period_web": get_monday_of_week().strftime("%Y-%m-%d"),
            "share_url": data_raw.get("share_url"),
            "aweme_id": config_data.get("aweme_id", aweme_id),
            "job_date": config_data.get("job_date"),
            "period": config_data.get("period"),
            "region": config_data.get("region"),
            "first_seen_date": config_data.get("first_seen_date"),
            "create_time": config_data.get("create_time"),
            "rescrape_3d": config_data.get("rescrape_3d"),
            "rescrape_7d": config_data.get("rescrape_7d"),
            "createTime_web": str(createTime),
            "desc": desc,
            "is_ads": isAd,
            "video_cover": video_cover,
            "video_originCover": video_originCover,
            "author_id": author_id,
            "author_uniqueId": author_uniqueId,
            "author_nickname": author_nickname,
            "author_followerCount": author_followerCount,
            "author_followingCount": author_followingCount,
            "music_id": music_id,
            "music_title": music_title,
            "music_playUrl": music_playUrl,
            "music_authorName": music_authorName,
            "stats_diggCount": stats_diggCount,
            "stats_shareCount": stats_shareCount,
            "stats_commentCount": stats_commentCount,
            "stats_playCount": stats_playCount,
            "stats_collectCount": stats_collectCount,
            "stats_repostCount": stats_repostCount,

        }

        return result
    except Exception as e:
        print(f"Lỗi transform_item_data: {e}")
        return {}



def load_tiktok_config(config_path: str) -> Dict[str, Dict[str, str]]:
    """
    Đọc file tiktok_config.txt và trả về dict tra cứu theo share_url.
    """
    config_dict = {}
    with open(config_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            # Chuẩn hóa key để dễ tra
            share_url = row.get("share_url")
            if share_url:
                config_dict[share_url.strip()] = {
                    "job_date": row.get("job_date"),
                    "period": row.get("period"),
                    "region": row.get("region"),
                    "first_seen_date": row.get("first_seen_date"),
                    "create_time": row.get("create_time"),
                    "rescrape_3d": row.get("rescrape_3d"),
                    "rescrape_7d": row.get("rescrape_7d"),
                    "aweme_id": row.get("aweme_id"),
                }
    return config_dict



def transform_all_data(path_data_raw = RAW_DATA_PATH, path_transform_data = PATH_TRANSFORM_DATA) -> List[dict]:
    all_data = []
    list_files = os.listdir(path_data_raw)
    config_dict=load_tiktok_config("tiktok_config.txt")
    json_files = [f for f in list_files if f.endswith(".json")]
    print(f"Found {len(list_files)} JSON files to process.")
    for file_name in json_files:
        file_path = os.path.join(path_data_raw, file_name)
        try:
            data_raw = read_file(file_path, FileFormat.JSON)
            if not data_raw:
                logger.bind(save=True).warning(f"File {file_name} rỗng hoặc không đọc được.")
                continue
            transformed = transform_item_data(data_raw, config_dict)
            save_file(
                transformed, 
                os.path.join(path_transform_data, f"{file_name}"), 
                FileFormat.JSON
            )
            if transformed:
                all_data.append(transformed)
            else:
                logger.bind(save=True).warning(f"Transform lỗi file {file_name}.")
        except Exception as e:
            logger.bind(save=True).error(f"Lỗi xử lý file {file_name}: {e}")
    return all_data

def main():
    # transform data
    print(TODAY)
    logger.bind(save=True).info("Transforming data ....")
    full_data = transform_all_data()
    logger.bind(save=True).success("Transforming completed")


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
        ("author_uniqueId", pl.Utf8),
        ("author_nickname", pl.Utf8),
        ("author_followerCount", pl.Utf8),     # hiện vẫn là string trong dữ liệu
        ("author_followingCount", pl.Utf8),

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

    save_file(
        full_data,"./full.json", FileFormat.JSON
    )
    logger.bind(save=True).info("START: UPLOAD TIKTOK WEB DATA")
    # upload bigquery
    gbg = GCBigquery()
    try:
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
            destination="tiktok_search_keyword_re_scrape_test.20251013",
            format="parquet",
            mode="append",
            use_legacy=True
        )
        logger.bind(save=True).success("Upload BigQuery Success")
    except Exception as e:
        logger.bind(save=True).error(f"BigQuery upload failed: {e}")
        logger.debug(traceback.format_exc())



if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.debug(traceback.format_exc())
        raise