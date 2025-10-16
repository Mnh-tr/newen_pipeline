import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_dir)
os.chdir(project_dir)

import re
import json
import asyncio
import pathlib
import random
import traceback
import httpx
import urllib.parse

from bs4 import BeautifulSoup
from typing import Any, Dict, Optional
from fake_http_header import FakeHttpHeader
from datetime import datetime, date
from loguru import logger

from src.utils.set_proxy import get_proxy, reset_proxy
from src.extract.tiktokweb_helper import(
    split_processed_configs_tiktok_post,
    extract_video_id,
    deep_get,
)
from src.utils.helper import (
    FileFormat,
    read_file,
    save_file,
    get_monday_of_week,
    file_filter,
    
)
from src.extract.xbogus_pure_py import encrypt as sign_bogus
from src.extract.xgnarly_pure_py import encrypt as sign_gnarly
from urllib.parse import urlparse, parse_qs, unquote, quote


# ---------------------- GLOBAL CONFIG ----------------------
log_time = datetime.today().strftime("%Y-%m-%dT%H-%M-%S%z")
PARTITION_DATE = get_monday_of_week().strftime("%Y-%m-%d")
TODAY = date.today().strftime("%Y-%m-%d")
RAW_DATA_PATH = f"./data_tiktok_video/{PARTITION_DATE}/{TODAY}/raw_data/video"
HTML_DATA_PATH = f"data_tiktok_video/{PARTITION_DATE}/{TODAY}/html/video"
CONFIGS_LINK = read_file(f"./configs/tiktok/{PARTITION_DATE}/{TODAY}/tiktok_share_urls.yaml", FileFormat.YAML)
REQUEST_URL = read_file("./configs/link_request_tiktok_photo.txt", FileFormat.TXT)
LOG_DIR = f"./logs/{PARTITION_DATE}/scrape_tiktok_video/scraper_tiktokweb"
def init_dir():
    pathlib.Path(RAW_DATA_PATH).mkdir(parents=True, exist_ok=True)
    pathlib.Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
    pathlib.Path(HTML_DATA_PATH).mkdir(parents=True, exist_ok=True)

init_dir()
import time
logger.remove()
logger.add(sys.stderr, level="DEBUG")

logger.add(
    f"{LOG_DIR}/run_log_{PARTITION_DATE}_{log_time}.log", 
    level="DEBUG",
    format="{time} {level} {message}", 
    filter=file_filter, 
    mode="a",
    encoding="utf-8"
)


def _is_captcha_html(html: str) -> bool:
    """Kiểm tra nội dung HTML có chứa CAPTCHA không"""
    return "Security Check" in html

def _extract_html_data(html: str) -> Optional[Dict[str, Any]]:
    """Trích xuất JSON data trong thẻ <script> của TikTok HTML"""
    soup = BeautifulSoup(html, "html.parser")
    script_tag = soup.find("script", {"id": "__UNIVERSAL_DATA_FOR_REHYDRATION__"})
    if not script_tag:
        return None
    return json.loads(script_tag.string)


async def build_tiktok_headers(proxy) -> Dict[str, str]:
    """Sinh header thật và hợp lệ để request TikTok"""

    # fake heade
    h = FakeHttpHeader(domain_code="vn", referer="").as_header_dict()

    #Chuẩn hóa và ép các giá trị cố định
    h["Accept-Language"] = "en-US,en;q=0.9"

    h.setdefault("Priority", "u=1, i")
    h.setdefault("Sec-Fetch-Dest", "empty")
    h.setdefault("Sec-Fetch-Mode", "cors")
    h.setdefault("Sec-Fetch-Site", "same-origin")

    #Chuẩn hóa chữ thường
    h = {k.lower(): v for k, v in h.items()}

    #Tạo client tạm để lấy cookie thực
    # client_args = {"timeout": 15.0, "headers": h, "proxy": proxy}
    # async with httpx.AsyncClient(**client_args) as client:
    #     resp = await client.get("https://www.tiktok.com")
    #     cookies = client.cookies

    # max_retries = 3
    # for attempt in range(max_retries):
    #     try:
    #         async with httpx.AsyncClient(**client_args) as client:
    #             resp = await client.get("https://www.tiktok.com/")
    #             cookies = client.cookies
    #             print(resp.status_code)
    #         break
    #     except httpx.ReadTimeout:
    #         if attempt < max_retries - 1:
    #             print(f"[Retry {attempt+1}/{max_retries}] Timeout khi connect TikTok, thử lại sau 2s...")
    #             await asyncio.sleep(2)
    #         else:
    #             raise

    # #Convert cookies -> chuỗi "a=b; c=d"
    # cookie_str = "; ".join([f"{name}={value}" for name, value in cookies.items()])

    # # Gộp cookie vào header
    # h["cookie"] = cookie_str

    #Đảm bảo có các trường quan trọng (fallback nếu thiếu)
    fallback_header = {
        'accept': '*/*',
        'user-agent': h.get('user-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'),
        'sec-ch-ua': h.get('sec-ch-ua', '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"'),
        'sec-ch-ua-mobile': h.get('sec-ch-ua-mobile', '?0'),
        'sec-ch-ua-platform': h.get('sec-ch-ua-platform', '"Windows"'),
    }

    for key, value in fallback_header.items():
        h.setdefault(key, value)

    return h

async def _fetch_with_retry(client, url, headers, retries=3):
    for i in range(retries):
        try:
            resp = await client.get(url, headers=headers)
            text = resp.text.strip()

            if resp.status_code == 200:
                if not text:
                    logger.warning(f"Empty response body (200) for URL: {url[:80]}...")
                    await asyncio.sleep(1)
                    continue
                if text.startswith("<!DOCTYPE html>") or text.startswith("<html"):
                    logger.warning(f"HTML response (Captcha or Blocked). Retrying...")
                    await asyncio.sleep(2)
                    continue

                try:
                    return resp.json()
                except Exception as e:
                    logger.warning(f"JSON parse error: {e}, first 100 chars: {text[:100]}")
                    await asyncio.sleep(1)
                    continue

            elif resp.status_code in [429, 503]:
                wait = (i + 1) * 2
                logger.warning(f"Rate limit {resp.status_code}, retry after {wait}s...")
                await asyncio.sleep(wait)
            elif resp.status_code in [401, 403]:
                logger.error(f"Auth error {resp.status_code}, stop request.")
                return None
            else:
                logger.warning(f"Unexpected status {resp.status_code}, retry {i+1}/{retries}")
                await asyncio.sleep(1)
        except httpx.RequestError as e:
            logger.warning(f"Network error: {e}, retry {i+1}/{retries}")
            await asyncio.sleep(1.5 * (i + 1))
    return None

# ---------------------- PHOTO FETCH ----------------------
async def fetch_photo_item(content_id: str, link_config: str, proxy_url: str, headers: Dict[str, str], worker_id: int):
    """Fetch dữ liệu dạng PHOTO"""
    try:
        logger.info(f"[Worker {worker_id}] Fetching PHOTO item_id={content_id}")
        params_base = {k: unquote(v[0]) for k, v in parse_qs(urlparse(REQUEST_URL).query).items()}
        params = params_base.copy()

        pop_list = ['X-Gnarly', 'X-Bogus']
        for i in pop_list:
            params.pop(i, None)

        params["itemId"] = content_id

        base_url = "https://www.tiktok.com/api/item/detail/"
        query_string = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        url_path = f"{base_url}?{query_string}"
        ua = headers["user-agent"]

        body = ""

        bogus = sign_bogus(query_string, body, ua, int(time.time()))
        complete_url = url_path + f"&X-Bogus={bogus}"
        gnarly = sign_gnarly(query_string, body, ua, 0, "5.1.1")
        complete_url = complete_url + f"&X-Gnarly={gnarly}"

        async with httpx.AsyncClient(timeout=15.0, proxy=proxy_url) as client:
            resp_json = await _fetch_with_retry(client, complete_url, headers)

        if not resp_json:
            logger.warning(f"[Worker {worker_id}] Empty PHOTO item_id={content_id}")
            return None

        return {"scrape_date": TODAY, "share_url": link_config, "data": resp_json}

    except Exception as e:
        logger.error(f"[Worker {worker_id}] PHOTO error id={content_id} -> {e}")
        return None

# ---------------------- VIDEO FETCH ----------------------
async def fetch_video_item(link_config: str, content_id: str, proxy_url: str, headers: Dict[str, str], worker_id: int):
    """Fetch dữ liệu dạng VIDEO"""
    try:
        async with httpx.AsyncClient(timeout=15.0, headers=headers, proxy=proxy_url) as client:
            response = await client.get(link_config)
            html = response.text

        if not html or _is_captcha_html(html):
            logger.warning(f"[Worker {worker_id}] Captcha/Empty for {content_id}")
            return None

        data = _extract_html_data(html)
        if not data:
            return None

        # KIỂM TRA DATA trả về có thuộc loại bắt login mới lấy được data hay không.
        item_struct = deep_get(data, ["__DEFAULT_SCOPE__", "webapp.video-detail", "itemInfo", "itemStruct"])
        if isinstance(item_struct, dict) and "serverABVersions" in item_struct:
            header = read_file(f"configs/cookies/tiktok_headers_{worker_id}.json", FileFormat.JSON)
            async with httpx.AsyncClient(timeout=15.0, headers=header, proxy=proxy_url) as client:
                response = await client.get(link_config)
                html = response.text
            logger.bind(save=True).debug(f"[Worker {worker_id}] VIDEO item_id={content_id} requires login to access data.")
            if _is_captcha_html(html):
                logger.bind(save=True).warning(f"[Worker {worker_id}] Captcha detected product_id={link_config}")
                return None
            data = _extract_html_data(html)
            if not data:
                return None

        video_id = extract_video_id(link_config)
        if not video_id:
            return None

        html_path = os.path.join(HTML_DATA_PATH, f"{video_id}.html")
        save_file(html, html_path, FileFormat.HTML)

        return {"scrape_date": TODAY, "share_url": link_config, "data": data}

    except Exception as e:
        logger.error(f"[Worker {worker_id}] VIDEO error id={content_id} -> {e}")
        return None
    

async def fetch_product(link_config: str, proxy_url: str, worker_id: int) -> Optional[Dict[str, Any]]:
    """Fetch TikTok product (photo hoặc video)"""
    url = link_config
    # print(url)
    try:
        match = re.search(r"/(video|photo)/(\d+)", link_config)
        if not match:
            return None

        content_type, content_id = match.groups()
        headers = await build_tiktok_headers(proxy_url)

        if content_type == "photo":
            return await fetch_photo_item(content_id, link_config, proxy_url, headers, worker_id)
        else:
            return await fetch_video_item(link_config, content_id, proxy_url, headers, worker_id)

    except Exception as e:
        logger.error(f"[Worker {worker_id}] Unhandled exception -> {e}")
        return None
    
async def crawl_configs(unprocessed_configs, RAW_DATA_PATH, worker_id=0, batch_size=5):
    
    proxy_url = await get_proxy(worker_id)
    # proxy_url = "http://zPtDBLxZstyle:Zf2vmdh6@139.99.36.55:8499"
    logger.bind(save=True).info(
        f"[Worker {worker_id}] START - assigned {len(unprocessed_configs)} link config"
    )
    request_count  = 0
        
    for i in range(0, len(unprocessed_configs), batch_size):
        batch = unprocessed_configs[i:i+batch_size]

        # nếu đã đủ 50 request thì đổi proxy
        if request_count > 0 and request_count % 50 == 0:
            logger.info(f"[Worker {worker_id}] Reset proxy sau {request_count} requests -> {proxy_url}")
            await reset_proxy(worker_id)
            logger.bind(save=True).info(f">>[Worker {worker_id}] Proxy reset thành công:")

        tasks = [fetch_product(link , proxy_url, worker_id) for link in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for link, data_raw in zip(batch, results):

            if isinstance(data_raw, Exception):
                logger.bind(save=True).error(f"[Worker {worker_id}] Exception for {link}: {data_raw}")
                continue

            if not data_raw:
                # Bỏ qua, không lưu
                logger.bind(save=True).warning(f"[Worker {worker_id}] Skip product_id={link} vì request thất bại.")
                continue
            
            video_id = extract_video_id(link)
            if not video_id:
                logger.bind(save=True).error(f"[Worker {worker_id}] Cannot extract ID from {link}")
                continue


            safe_filename = f"{video_id}.json"
            file_path = os.path.join(RAW_DATA_PATH, safe_filename)
            save_file(data_raw, file_path, FileFormat.JSON)
            logger.bind(save=True).success(
                f"[Worker {worker_id}] HAVE DATA - saved {file_path}"
            )

        # tăng bộ đếm sau mỗi batch
        request_count += len(batch)

        # delay nhỏ giữa các batch
        await asyncio.sleep(random.uniform(1, 2)+ batch_size * 0.1)

    logger.bind(save=True).info(f"[Worker {worker_id}] FINISHED")

async def run_workers(unprocessed_configs, RAW_DATA_PATH, n_workers=1, batch_size = 5):
    if not unprocessed_configs:
        return
    # round-robin chia config cho n_workers
    chunks = [unprocessed_configs[i::n_workers] for i in range(n_workers)]

    tasks = []
    for i, chunk in enumerate(chunks):
        tasks.append(asyncio.create_task(
            crawl_configs(chunk, RAW_DATA_PATH, worker_id=i, batch_size = batch_size)
        ))

    await asyncio.gather(*tasks)

async def main(link_configs = CONFIGS_LINK, max_attempts=20):
    FULL_CONFIG = link_configs
    logger.bind(save=True).info(f"RUN START - total_configs={len(FULL_CONFIG)}")
    processed_configs, unprocessed_configs = split_processed_configs_tiktok_post(FULL_CONFIG, RAW_DATA_PATH)

    logger.debug(f"Processed Configs: {len(processed_configs)}")
    logger.debug(f"Unprocessed Configs: {len(unprocessed_configs)}")
    if not unprocessed_configs:
        logger.success("All configs have been processed")
    
    # SỐ Luồng
    max_threads = 1

    # Số lượng request cùng lúc.
    batch_size = 5
    attempt = 0
    # run code các sử lý ở đây
    while unprocessed_configs and attempt < max_attempts:
        attempt += 1
        logger.bind(save=True).info(f"[Attempt {attempt}] RUN START - remaining configs={len(unprocessed_configs)}")

        # Chạy worker cho các config chưa xử lý
        await run_workers(unprocessed_configs, RAW_DATA_PATH, n_workers=max_threads, batch_size = batch_size)

        processed_configs, unprocessed_configs = split_processed_configs_tiktok_post(FULL_CONFIG, RAW_DATA_PATH)
        logger.debug(f"[Attempt {attempt}] Remaining configs after run: {len(unprocessed_configs)}")
        
        if unprocessed_configs:
            logger.warning(f"Configs remaining after attempt {attempt}, retrying...")

    if unprocessed_configs:
        logger.error(f"Some configs could not be processed after {max_attempts} attempts: {unprocessed_configs}")
    else:
        logger.bind(save=True).success("ALL CONFIGS PROCESSED - scraping finished")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.debug(traceback.format_exc())
        raise