#!/usr/bin/env python3
# coding=utf-8

import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse
import re
import pandas as pd
from multiprocessing import Pool, Process, Queue
from datetime import datetime
from functools import reduce
import numpy as np

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
    try:
        html = requests.get(url, timeout=300)
        html.raise_for_status()
        html.encoding = html.apparent_encoding
    except:
        print('html not response: {}'.format(url))
        return [[None] * 6]
    soup = BeautifulSoup(html.text, features="html.parser")
    if soup.body.find_all('tbody'):
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
    else:
        print('html has no airport: {}'.format(url))
        return [[None] * 6]


def get_nationlist():
    with open(r'./HYPERLINK.mht', 'r', encoding='utf8') as f:
        content = f.read()
        urllist = re.findall('<a href=3D"(http://airport.anseo.cn/c-.+?)"', content)
        return urllist


def get_maxpage(url):
    html = requests.get(url)
    html.encoding = html.apparent_encoding
    soup = BeautifulSoup(html.text, features="html.parser")
    ul = soup.body.find('ul', attrs={'class': 'pagination pull-right'})
    page_nums = re.findall('<a href="http://airport.anseo.cn/c-\D+__page-(\d+)">', str(ul))
    page_nums = [eval(x) for x in page_nums]
    if page_nums:
        return max(page_nums)
    else:
        return 1


def get_url_list():
    urls = []
    nationlist = get_nationlist()
    pool = Pool(processes=8)
    nation_max = pool.map(get_maxpage, nationlist)
    pool.close()
    pool.join()
    for max_num, nation in zip(nation_max, nationlist):
        urls.extend(['{}__page-{}'.format(nation, x) for x in range(1, max_num+1)])
    return urls


def get_airport_code(ulist):
    pool = Pool(processes=20)
    airport_list = pool.map(get_page2, ulist)
    pool.close()
    pool.join()
    airport_list = reduce(lambda x, y: x + y, airport_list)
    return airport_list


def geocode(city, address):
    parameters = {'address': address, 'city': city, 'key': '895d8c6afa27ba2b57dd757ab490e16d'}
    base = 'http://restapi.amap.com/v3/geocode/geo'
    response = requests.get(base, parameters)
    #print(response.json())
    answer = response.json()
    return answer


def get_geoinfo(parameters):
    j_geo = geocode(parameters[0], parameters[1])
    default_geo = {'address': parameters[1], 'formatted_address': '',
                   'country': '', 'province': '',
                   'citycode': '', 'city': '',
                   'district': '', 'township': [],
                   'neighborhood': {'name': [], 'type': []}, 'building': {'name': [], 'type': []},
                   'adcode': '', 'street': [], 'number': [],
                   'location': ',', 'level': ''}
    '''
    geo_dict字典格式
    'formatted_address'   #标准化地址
    'province'            #省
    'city'                #市
    'district'            #区
    'location'            #经度,纬度
    '''
    if j_geo['status'] == '1' and j_geo['count'] != '0':
        geo_dict = j_geo['geocodes'][0]
        geo_dict['address'] = parameters[1]
        return geo_dict
    else:
        return default_geo


if __name__ == '__main__':
    '''
    #spider 1
    urllist = [r'http://www.6qt.net/index.asp?Field=&keyword=&MaxPerPage=50&page={}'.format(x) for x in range(1,49)]
    all = []
    for url in urllist:
        all.extend(get_page(url))
    df = pd.DataFrame(all, columns=['所属城市', '三字代码', '所属国家', '国家代码', '四字代码', '机场名称', '英文名称'])
    df.to_csv(r'./airport_code.csv')

    #spider 2
    print('start getting URL list')
    urllist = get_url_list()

    #save URL list
    df_page = pd.DataFrame(urllist)
    print('save the URL list')
    df_page.to_csv(r'./url.csv')
    '''

    '''
    urllist = pd.read_csv('./url.csv', index_col=0, header=0)['0']
    print('the spider is getting {} pages'.format(len(urllist)))
    all_airport = get_airport_code(urllist[:12])
    print('spider1 end, the spider has gotten {} airports'.format(len(all_airport)))
    df = pd.DataFrame(all_airport, columns=['所属城市', '所属城市英文', '机场名称', '机场英文名称', '三字代码', '四字代码'])
    df.to_csv(r'./airport.csv')
    print('airport code saved')

    df_airport = pd.read_csv('./airport.csv', index_col=0, header=0)
    df_test = df_airport[:].copy()
    df_test.fillna('', inplace=True)
    pool = Pool(processes=8)
    test = pool.map(get_geoinfo, zip(df_test['所属城市'], df_test['机场名称']))
    pool.close()
    pool.join()
    
    df_geo = pd.DataFrame([[x['formatted_address'], x['country'], x['province'], x['city'], x['location'].split(',')[0], x['location'].split(',')[1]]
                           for x in test], columns=['标准地址', '国家', '省份', '城市', '经度', '纬度'])
    df_test[['标准地址', '国家', '省份', '城市', '经度', '纬度']] = df_geo.copy()
    print(df_test)
    df_test.to_csv(r'./test.csv')

    '''
    df_test = pd.read_csv(r'./test.csv', index_col=0, header=0)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    arr = []
    for row in df_test.itertuples():
        if str(row[7]).endswith('机场'):
            arr.append(row)
    df = pd.DataFrame(arr).set_index(['Index']).reset_index(drop=True)
    print(len(df))
    df.to_csv(r'lacation.csv')
