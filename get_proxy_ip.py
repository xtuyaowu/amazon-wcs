# -*- coding: utf-8 -*-
import json
import time
import requests
from requests.exceptions import RequestException


# 私密代理接口
ProxyUrl = 'http://dps.kuaidaili.com/api/getdps/?orderid=951080298576549&num=1000&format=json&sep=1'


# 接口取代理ip
def get_proxy_ip():
    flag = True
    while flag:
        try:
            res = requests.get(ProxyUrl, timeout=10)
            if res.status_code == 200:
                try:
                    proxy_list = list(set(json.loads(res.text)['data']['proxy_list']))
                except TypeError:
                    time.sleep(2)
                else:
                    flag = False
                    return proxy_list
        except RequestException:
            print('No ProxyIp')
            time.sleep(2)


if __name__ == '__main__':
    pass









