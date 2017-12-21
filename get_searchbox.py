import random
import re
import requests
from lxml import etree
from settings import HEADERS


def get_searchword(url):
    headers = {'user-agent': random.choice(HEADERS)}
    req = requests.get(url, headers=headers)
    if req.status_code == 200:
        sel = etree.HTML(req.text)
        options = sel.xpath('//select[@id="searchDropdownBox"]/option')
        for op in options:
            key_word = op.xpath('./text()')[0]
            parm = op.xpath('./@value')[0].split('=')[1]
            print(key_word, parm)


if __name__ == '__main__':
    start_url = 'https://www.amazon.fr/'
    get_searchword(start_url)
