import os
import re
from typing import List, Set, Tuple
async def is_config_processed(product_id: str, processed_files: set) -> bool:
    """
    Kiểm tra xem product_id đã được cào (tồn tại file .json) hay chưa.
    """
    filename = f"{product_id}.json"
    return filename in processed_files


async def split_processed_configs(product_ids: list[str], raw_data_path: str):
    """
    Chia product_ids thành 2 list:
      - processed: đã có file .json trong raw_data_path
      - unprocessed: chưa có file .json
    """
    processed_files = set(os.listdir(raw_data_path))
    processed, unprocessed = [], []

    for pid in product_ids:
        if await is_config_processed(pid, processed_files):
            processed.append(pid)
        else:
            unprocessed.append(pid)

    return processed, unprocessed



async def is_config_processed_tiktok_post(product_url: str, processed_files: Set[str]) -> bool:
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

async def split_processed_configs_tiktok_post(product_urls: List[str], raw_data_path: str) -> Tuple[List[str], List[str]]:
    """
    Chia product_urls thành 2 list:
      - processed: đã có file .json trong raw_data_path
      - unprocessed: chưa có file .json
    """
    processed_files = set(os.listdir(raw_data_path))
    processed, unprocessed = [], []

    for url in product_urls:
        if await is_config_processed_tiktok_post(url, processed_files):
            processed.append(url)
        else:
            unprocessed.append(url)

    return processed, unprocessed
