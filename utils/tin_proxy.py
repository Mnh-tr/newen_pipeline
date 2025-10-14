import requests
import json
import time

def set_tin_proxy(proxy_api_key,host_ip):
    proxy_api = f'https://api.tinproxy.com/proxy/get-new-proxy?api_key={proxy_api_key}&authen_ips={host_ip}&&location=vn_dn'
    for refresh in range(5):
        try:
            proxy_data =requests.get(proxy_api).json()
            # print('****\n',proxy_data,'\n***\n')
            # Remember to change this after testing
            if  1==1:#proxy_data['message']=='Lấy Proxy thành công':
                user = proxy_data['data']['authentication']['username']
                pw = proxy_data['data']['authentication']['password']
                proxy ={
                    'http': proxy_data['data']['http_ipv4'],
                    'https': proxy_data['data']['http_ipv4']
                }
                print('\n -- proxy :',proxy,' \n -- \n')
            else :
                pass
        except:
            proxy ={
                'http': '',
                'https': ''
            }
            pass
        if proxy['http']!='' and proxy['http']!=':':
                break
        else:
            print('Try getting Tinproxy IP again ...')
            time.sleep(8)
    return proxy
        
def set_proxy(will_sleep=0):
    proxy_api_key= 'xczlG4HAkznFYmox4KYUkia84riwkyn9'
    host_ip= '34.143.139.44'
    for refresh in range(2):
        proxy = set_tin_proxy(proxy_api_key,host_ip)
        if proxy['http']!='':
            break
        else:
            print('Try to get Tinproxy IP again ...')
    return proxy