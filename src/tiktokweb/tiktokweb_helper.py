import os
import re
from typing import List, Set, Tuple, Optional

def extract_video_id(url: str) -> Optional[str]:
    """Extract video ID from TikTok URL"""
    match = re.search(r"/(\d+)$", url)
    return match.group(1) if match else None



def is_config_processed_tiktok_post(product_url: str, processed_files: Set[str]) -> bool:
    """
    Kiểm tra xem product_url đã được cào (tồn tại file .json) hay chưa.
    Ví dụ:
        product_url = "https://www.tiktok.com/@halinhofficial/video/7382946652333591816"
        processed_files = {"halinhofficial_video_7382946652333591816.json"}
    """
    match = re.search(r"/(\d+)$", product_url)
    if not match:
        return False  # URL không hợp lệ

    video_id = match.group(1)
    filename = f"{video_id}.json"
    return filename in processed_files

def split_processed_configs_tiktok_post(product_urls: List[str], raw_data_path: str) -> Tuple[List[str], List[str]]:
    """
    Chia product_urls thành 2 list:
      - processed: đã có file .json trong raw_data_path
      - unprocessed: chưa có file .json
    """
    processed_files = set(os.listdir(raw_data_path))
    processed, unprocessed = [], []

    for url in product_urls:
        if is_config_processed_tiktok_post(url, processed_files):
            processed.append(url)
        else:
            unprocessed.append(url)

    return processed, unprocessed


def deep_get(d, path: list, default=None):
    """Truy cập an toàn vào dict lồng nhau."""
    for key in path:
        if isinstance(d, dict):
            d = d.get(key, default)
        else:
            return default
    return d