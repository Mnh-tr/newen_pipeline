from __future__ import annotations

import datetime
import time

import requests
from loguru import logger


def try_checking_ip(url, proxy, response_ip_field):
    try:
        print("Checking ip...")
        response_json = requests.get(url, proxies=proxy, timeout=15).json()
        ip = response_json[response_ip_field]
    except Exception as e:
        print(e)
        ip = ""
    return ip


def set_tin_proxy(host_ip, proxy_api_key, location="vn_dn"):
    for let_try in range(30):
        try:
            proxy_api = f"https://api.tinproxy.com/proxy/get-new-proxy?api_key={proxy_api_key}&authen_ips={host_ip}&location={location}"
            proxy_data = requests.get(proxy_api).json()
            print("\n\n", proxy_data, "\n\n")
            proxy = {"http": proxy_data["data"]["http_ipv4"], "https": proxy_data["data"]["http_ipv4"]}
            break
        except Exception:
            print("Cant get proxy from Tin. Let try again ...")
            time.sleep(8)
    return proxy


def check_proxy(proxy):
    checking_status = False
    checking_url_list = ["https://api.ipify.org?format=json", "https://jsonip.com/"] #, "http://ip-api.com/json/"]
    response_ip_field_list = ["ip", "ip"] #, "query"]
    for i in range(len(checking_url_list)):
        url = checking_url_list[i]
        response_ip_field = response_ip_field_list[i]
        ip = try_checking_ip(url, proxy, response_ip_field)
        if ip != "":
            print("Proxy oki ", ip)
            checking_status = True
            break
    return checking_status


def set_mproxy(token, sub_account, is_reset=False):
    """
    mproxy's sub_account format: user:key_pass@ip.mproxy.vn:port
    If config allow_ip, then dont need specify authentication:
        proxy_auth = {
            'http': f"http://user:key_pass@ip.mproxy.vn:port",
            'https': f"http://user:key_pass@ip.mproxy.vn:port"
        }
        proxy = {
            'http': f"ip.mproxy.vn:port",
            'https': f"ip.mproxy.vn:port"
        }

    """
    if is_reset:
        key_pass = sub_account.split(":")[1].split("@")[0]
        api_url = f"https://mproxy.vn/capi/{token}/key/{key_pass}/resetIp"
        for _ in range(8):
            try:
                r = requests.get(api_url)
                response = r.json()
                if response["status"] == 1:
                    break
                else:
                    logger.debug(f'Remaining time to refresh proxy: {response["data"]["remaining_time"]}')
                    time.sleep(response["data"]["remaining_time"])
            except Exception as e:
                logger.info(f"Cannot get proxy from mproxy: {e}. Let try again!")
                time.sleep(4)

    proxy_address = sub_account.split("@")[1]
    proxy = {"http": f"{proxy_address}", "https": f"{proxy_address}"}
    return proxy


# def set_tinproxy(proxy_api_key, host_ip, location="random", is_reset=False):
#     api_url = (
#         f"https://api.tinproxy.com/proxy/get-new-proxy?api_key={proxy_api_key}&authen_ips={host_ip}&location={location}"
#     )

#     for _ in range(8):
#         try:
#             r = requests.get(api_url)
#             response = r.json()
#             proxy = {"http": response["data"]["http_ipv4"], "https": response["data"]["http_ipv4"]}
#             if is_reset:
#                 logger.debug(f'Remaining time to refresh proxy: {response["data"]["next_request"]}')
#                 time.sleep(response["data"]["next_request"])
#             else:
#                 break
#         except Exception as e:
#             logger.info(f"Cannot get proxy from tinproxy: {e}. Let try again!")
#             time.sleep(4)
#     return proxy
def set_tinproxy(proxy_api_key, host_ip, location="random", is_reset=False):
    api_url = (
        f"https://api.tinproxy.com/proxy/get-new-proxy?api_key={proxy_api_key}&authen_ips={host_ip}&location={location}"
    )

    for attempt in range(8):  
        try:
            r = requests.get(api_url)
            response = r.json()
            
            if is_reset:
                remaining_time = response["data"]["next_request"]
                logger.debug(f'Remaining time to refresh proxy: {remaining_time} seconds')
                print(f'Remaining time to refresh proxy: {remaining_time} seconds')
                time.sleep(remaining_time)
                
                r = requests.get(api_url)
                response = r.json()
                
            proxy = {"http": response["data"]["http_ipv4"], "https": response["data"]["http_ipv4"]}
            
            return proxy
        
        except Exception as e:
            logger.info(f"Cannot get proxy from tinproxy: {e}. Let try again! (Attempt {attempt + 1}/8)")
            time.sleep(4)  

    return None



def set_lunaproxy_subaccount(sub_account):
    proxy = {"http": f"{sub_account}", "https": f"{sub_account}"}
    return proxy


def init_lunaproxy_list(neek, no_ips, regions):
    api_url = f"https://tq.lunaproxy.com/getflowip?neek={neek}&num={no_ips}&type=1&sep=4&regions={regions}&ip_si=2&level=1&sb="
    current_date = datetime.datetime.today().strftime("%Y-%m-%d")
    print("Get new luna ip list ...")
    r = requests.get(api_url, timeout=4)
    ip_list = r.text.split("\n")
    with open("luna_ip_list.txt", "w", encoding="utf-8") as f:
        f.write(current_date + "\n")
        for ip in ip_list:
            f.write(str(ip) + "\n")
    time.sleep(3)


def set_lunaproxy(index):
    with open("luna_ip_list.txt") as f:
        proxy_info = f.read().split("\n")
    date = proxy_info[0]
    ip_list = proxy_info[1:]
    proxy_address = ip_list[index]
    proxy = {"http": f"{proxy_address}", "https": f"{proxy_address}"}
    return proxy

def set_proxy(provider, is_reset=False, max_retries=3, wait_time=5, **kwargs):
    attempt = 0
    while attempt < max_retries:
        try:
            proxy = None
            if provider == "mproxy":
                proxy = set_mproxy(kwargs["token"], kwargs["sub_account"], is_reset)
            elif provider == "tinproxy":
                proxy = set_tinproxy(kwargs["proxy_api_key"], kwargs["host_ip"], is_reset=is_reset)
            elif provider == "lunaproxy":
                proxy = set_lunaproxy(kwargs["index"])
            elif provider == "lunaproxy_subaccount":
                proxy = set_lunaproxy_subaccount(kwargs["sub_account"])
            else:
                logger.info("Unsupported provider!")
                raise ValueError("Unsupported provider!")

            if proxy and check_proxy(proxy):
                return proxy
            else:
                logger.info(f"Proxy check failed. Retrying ({attempt + 1}/{max_retries})...")
                attempt += 1
                time.sleep(wait_time)

        except Exception as e:
            logger.info(f"Error setting proxy with provider {provider}: {e}")
            attempt += 1
            time.sleep(wait_time)
            is_reset = True

    logger.info("Max retries reached. Failed to set a valid proxy.")
    raise Exception("Failed to set a valid proxy after multiple attempts.")
