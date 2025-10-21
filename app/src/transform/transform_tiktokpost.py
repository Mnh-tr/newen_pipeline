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
            "aweme_id": config_data.get("aweme_id"),
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


def transform_all_data(path_data_raw = RAW_DATA_PATH, path_transform_data = PATH_TRANSFORM_DATA, config_dict = CONFIG_DATA_PATH) -> List[dict]:
    all_data = []
    list_files = os.listdir(path_data_raw)
    config_data = load_tiktok_config(config_dict)
    json_files = [f for f in list_files if f.endswith(".json")]
    print(f"Found {len(list_files)} JSON files to process.")
    for file_name in json_files:
        file_path = os.path.join(path_data_raw, file_name)
        try:
            data_raw = read_file(file_path, FileFormat.JSON)
            if not data_raw:
                logger.bind(save=True).warning(f"File {file_name} rỗng hoặc không đọc được.")
                continue
            transformed = transform_item_data(data_raw, config_data)
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


if __name__ == "__main__":
    try:
        transform_all_data()
    except Exception:
        logger.debug(traceback.format_exc())
        raise