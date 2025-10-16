
# import yaml
# import httpx
# import asyncio
# from loguru import logger

# SECRETS = "configs/secrets.yaml"

# async def load_proxy(file_path: str = SECRETS):
#     """Đọc file YAML config"""
#     with open(file_path, "r", encoding="utf-8") as f:
#         return yaml.safe_load(f)

# def build_proxy_url(proxy_entry: dict) -> str:
#     """Tạo proxy URL từ entry"""
#     sub_account = proxy_entry["proxy"]["sub_account"]
#     return f"http://{sub_account}"


# async def check_proxy(proxy_url: str, timeout: int = 10) -> bool:
#     """Kiểm tra proxy có hoạt động không và log ra IP"""
#     try:
#         async with httpx.AsyncClient(proxy=proxy_url, timeout=timeout) as client:
#             resp = await client.get("https://api.ipify.org?format=json")
#             if resp.status_code == 200:
#                 ip = resp.json().get("ip")
#                 logger.info(f"Proxy hoạt động, IP: {ip}")
#                 return True
#     except Exception as e:
#         logger.error(f"Proxy lỗi {proxy_url}: {e}")
#     return False

# async def reset_proxy():
#     url = "https://mproxy.vn/capi/5pOyM4OaNPAxKymWfKE37EMgU9PFr9HKYcwU7vQamr4/key/0kj90jCFRgXT0a/resetIp"
#     try:
#         r = httpx.get(url, timeout=20)
#         if "499" not in r.text and r.status_code == 200:
#             logger.info(f">> Proxy reset thành công: {r.text}")
#             return True
#         else:
#             logger.warning(f">> Lỗi reset proxy: {r.status_code} - {r.text}")
#             return False
#     except Exception as e:
#         logger.error(f">> Exception khi reset proxy: {e}")
#         return False

# async def get_proxy(index: int, file_path: str = SECRETS) -> str:
#     """
#     Lấy proxy theo index, kiểm tra hoạt động trước khi return.
#     Nếu proxy không hoạt động thì raise Exception.
#     """
#     proxy = await load_proxy(file_path)
#     accounts = proxy.get("SHOPLUS", [])

#     if index < 0 or index >= len(accounts):
#         raise IndexError(f"Index {index} ngoài phạm vi (0-{len(accounts)-1})")

#     proxy_entry = accounts[index]
#     proxy_url = build_proxy_url(proxy_entry)

#     is_alive = await check_proxy(proxy_url)
#     if not is_alive:
#         raise RuntimeError(f"Proxy {proxy_url} không hoạt động")

#     return proxy_url

import time
import httpx
import asyncio
from loguru import logger

# API reset IP
RESET_URL = ["https://api.zingproxy.com/getip/409a94e5d09508af6867f08a462d1a314f2b210a"

]

# Proxy list (có thể đưa ra file .yaml nếu nhiều)
PROXIES = [
    "139.99.36.55:8451:ZsiOashcstyle:ZYFBxLgu"
]

def build_proxy_url(raw_proxy: str) -> str:
    """
    Chuyển proxy raw thành URL dùng cho httpx.
    - ip:port:user:pass -> http://user:pass@ip:port
    - ip:port          -> http://ip:port  (Whitelist IP)
    """
    parts = raw_proxy.split(":")
    if len(parts) == 2:
        ip, port = parts
        return f"http://{ip}:{port}"
    elif len(parts) == 4:
        ip, port, user, pwd = parts
        return f"http://{user}:{pwd}@{ip}:{port}"
    else:
        raise ValueError(f"Proxy string không hợp lệ: {raw_proxy}")


async def check_proxy(proxy_url: str, timeout: int = 10, retries: int = 3, delay: int = 2) -> bool:

    """Kiểm tra proxy có hoạt động không và log ra IP"""

    for i in range(retries):
        try:
            async with httpx.AsyncClient(proxy=proxy_url, timeout=timeout) as client:
                resp = await client.get("https://api.ipify.org?format=json")
                if resp.status_code == 200:
                    ip = resp.json().get("ip")
                    logger.info(f"Proxy hoạt động, IP: {ip}")
                    return True
        except Exception as e:
            logger.error(f"Thử lần {i+1}/{retries} với proxy {proxy_url} thất bại: {e}")
            await asyncio.sleep(delay)
    return False
    
async def reset_proxy(worker_id) -> bool:
    """Reset IP bằng API của zingproxy"""
    proxy_url = build_proxy_url(PROXIES[worker_id])
    for let_try in range(10):
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(RESET_URL[worker_id], timeout=5)
                r = r.json()

            if "error" not in r:
                logger.info(f">>[Worker {worker_id}] Proxy reset thành công: {r}")
                await asyncio.sleep(2)
                
                if await check_proxy(proxy_url):
                    logger.info(f">>[Worker {worker_id}] Proxy {proxy_url} hoạt động tốt sau khi reset")
                    break
                else:
                    logger.warning(f">>[Worker {worker_id}] Proxy {proxy_url} không hoạt động, thử lại...")
                    await asyncio.sleep(5)
                    continue
            else:
                logger.warning(f">>[Worker {worker_id}] Lỗi reset proxy: {r}")
                error = r.get("error", "")
                time_sleep = int(error.split(" ")[-2]) + 2
                await asyncio.sleep(time_sleep)
               
        except Exception as e:
            logger.error(f">>[Worker {worker_id}] Exception khi reset proxy: {e}")
            await asyncio.sleep(62)
   
async def get_proxy(index: int) -> str:
    """Lấy proxy theo index, kiểm tra hoạt động trước khi return"""
    if index < 0 or index >= len(PROXIES):
        raise IndexError(f"Index {index} ngoài phạm vi (0-{len(PROXIES)-1})")

    proxy_url = build_proxy_url(PROXIES[index])

    is_alive = await check_proxy(proxy_url)
    if not is_alive:
        await reset_proxy(index)
        # raise RuntimeError(f"Proxy {proxy_url} không hoạt động")
        
    return proxy_url

