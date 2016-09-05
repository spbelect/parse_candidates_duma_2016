#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sys
import traceback
from os.path import exists

from pandas import DataFrame, read_csv, concat
from bs4 import BeautifulSoup
from requests import get


def get_html(url):
    for retry in range(1, 10):
        print('try %d get %s' % (retry, url))
        response = get(url)
        if response.status_code == 200 and response.text:
            soup = BeautifulSoup(response.text, 'html.parser')
            if soup.find('tbody', id='test'):
                return soup
    print('Max reties exceeded')
    sys.exit(1)


def get_cells(tr):
    return [x.text.strip() for x in tr.find_all('td')]


def parse_candidates(url):
    soup = get_html(url)
    try:
        data = [get_cells(x)[:9] for x in soup.find('tbody', id='test').find_all('tr')]
    except:
        traceback.print_exc()
        sys.exit(1)

    if data:
        print('\t'.join(data[-1]))

    return DataFrame(data, columns=['ord', 'fio', 'bday', 'party', 'okrug', 'vidvinut', 'registr', 'data', 'num'])


duma_url = 'http://www.vybory.izbirkom.ru/region/region/izbirkom?action=show&root=1000275&tvd=100100067795849&vrn=100100067795849&region=%(id)s&global=true&sub_region=%(id)s&prver=0&pronetvd=0&type=220'


for region in json.load(open('regions.json')):
    print(region['name'])
    if exists('candidates-%(id)s %(name)s.csv' % region):
        continue
    try:
        dataframe = parse_candidates(duma_url % region)
    except Exception as e:
        traceback.print_exc()
        sys.exit(1)

    with open('candidates-%(id)s %(name)s.csv' % region, 'w+') as filee:
        filee.write(dataframe.set_index('ord').to_csv())


def get_all():
    for region in json.load(open('regions.json')):
        yield read_csv('candidates-%(id)s %(name)s.csv' % region, index_col='ord')

with open('candidates-all.csv', 'w+') as filee:
    filee.write(concat(list(get_all())).to_csv())
