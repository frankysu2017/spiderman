#!/usr/bin/env python3
# coding=utf-8

import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse
import re
import pandas as pd


def get_page(url):
    html = requests.get(url)
    html.encoding = html.apparent_encoding
    soup = BeautifulSoup(html.text, features="html.parser")
    r = soup.find_all('tr', attrs={'class': 'tdbg'})
    codelist = []
    for index, item in enumerate(r):
        lst = item.text.split('\n')
        for num, i in enumerate(lst):
            i = i.strip('\xa0').strip()
            if i:
                lst[num] = i
        if len(lst) > 3 and lst[1] != '机场代码搜索：':
            codelist.append(lst[1:8])
    return codelist


def get_page2(url):
    html = requests.get(url)
    html.encoding = html.apparent_encoding
    soup = BeautifulSoup(html.text, features="html.parser")
    tbody = soup.body.find_all('tbody')[0]
    tr = tbody.find_all('tr')
    codelist = []
    for tr_item in tr:
        row = [x.text.replace('\t', '') for x in tr_item.find_all('td')]
        rowlist = []
        for item in row:
            rowlist.extend(item.split('\n'))
        rowlist.pop(3)
        codelist.append(rowlist[1:])
    return codelist


if __name__ == '__main__':
    '''
    urllist = [r'http://www.6qt.net/index.asp?Field=&keyword=&MaxPerPage=50&page={}'.format(x) for x in range(1,49)]
    all = []
    for url in urllist:
        all.extend(get_page(url))
    df = pd.DataFrame(all, columns=['所属城市', '三字代码', '所属国家', '国家代码', '四字代码', '机场名称', '英文名称'])
    df.to_csv(r'./airport_code.csv')
    '''
    urllist = ['http://airport.anseo.cn/c-china__page-{}'.format(x) for x in range(1, 10)]
    all = []
    for url in urllist:
        all.extend(get_page2(url))
    df = pd.DataFrame(all, columns=['所属城市', '所属城市英文', '机场名称', '机场英文名称', '三字代码', '四字代码'])
    #
    df.to_csv(r'./airport_code2.csv')
