from __future__ import annotations

import json
import time
import traceback

import psutil
import undetected_chromedriver as uc
from undetected_chromedriver import ChromeOptions as Options
from loguru import logger

def stop_chrome_session(pid):
    for process in (process for process in psutil.process_iter() if process.pid == pid):
        try:
            process.kill()
            # sub_browser_count()
            break
        except Exception:
            print(traceback.format_exc())
    logger.info(f"Chrome session with PID {pid} has been stopped.")


def set_driver(proxy=None, proxy_extension_path=None, prefs=None):
    for _ in range(5):
        try:
            options = Options()
            if proxy:
                options.add_argument(f'--proxy-server=http://{proxy["https"]}')  # if use tinproxy
            elif proxy_extension_path:
                options.add_argument(f"--load-extension={proxy_extension_path}")
                
            options.add_argument("--disable-blink-features=AutomationControlled")
            
            # --- Ngăn Chrome tạm dừng khi tab mất focus ---
            options.add_argument("--disable-background-timer-throttling")
            options.add_argument("--disable-backgrounding-occluded-windows")
            options.add_argument("--disable-renderer-backgrounding")
            options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
            if prefs:
                options.add_experimental_option("prefs", prefs)
            driver = uc.Chrome(
                options=options,
                version_main=140,
            )
            driver.set_page_load_timeout(30)
            return driver
        except Exception as e:
            print(e)

def find_request(browser_log, searching_url):
    request_list = []
    for log in browser_log:
        log = json.loads(log["message"])["message"]
        try:
            if log["params"].get("request"):
                # url = log["params"]["request"]["url"]
                url = log.get("params", {}).get("request", {}).get("url")
                if url and url.find(searching_url) != -1 :
                    request_list.append({"url": url, "request_id": log["params"]["requestId"]})
        except Exception as e:
            print(f"Something wrong in find_request: {e}")
            print(traceback.format_exc())
    if request_list == []:
        print("Cant find request ID")
    return request_list


def get_traffic_network_from_driver(driver):
    browser_log = driver.get_log("performance")
    request_list = find_request(browser_log, searching_url="")
    return request_list


def get_response_of_api(driver, request_id):
    try:
        body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
        response_data = json.loads(body["body"])
        return response_data
    except Exception as e:
        return None
