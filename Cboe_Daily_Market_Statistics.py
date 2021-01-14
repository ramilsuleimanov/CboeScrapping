#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 13:58:23 2021

@author: ramil
"""

from collections import namedtuple
from bs4 import BeautifulSoup
import logging
import lxml
import pandas as pd
import requests
import time

logging.basicConfig(level = logging.DEBUG)
logger = logging.getLogger('cboe')

session = requests.Session()
headers = requests.utils.default_headers()
headers['User-Agent'] = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0) Gecko/20100101 Firefox/84.0'

ratio_raw_names = {'Ratio': ['TOTAL PUT/CALL RATIO',
                             'INDEX PUT/CALL RATIO',
                             'EXCHANGE TRADED PRODUCTS PUT/CALL RATIO',
                             'EQUITY PUT/CALL RATIO',
                             'CBOE VOLATILITY INDEX (VIX) PUT/CALL RATIO',
                             'SPX + SPXW PUT/CALL RATIO',
                             'OEX PUT/CALL RATIO']}

dfratios = pd.DataFrame(ratio_raw_names).set_index('Ratio')

raw_names = {'Name': ['SUM OF ALL PRODUCTS',
                      'INDEX OPTIONS',
                      'EXCHANGE TRADED PRODUCTS',
                      'EQUITY OPTIONS',
                      'CBOE VOLATILITY INDEX (VIX)',
                      'SPX + SPXW',
                      'OEX']}

dfvolumeput = pd.DataFrame(raw_names).set_index('Name')
dfvolumecall = pd.DataFrame(raw_names).set_index('Name')
dfvolumetotal = pd.DataFrame(raw_names).set_index('Name')
dfopeninterestput = pd.DataFrame(raw_names).set_index('Name')
dfopeninterestcall = pd.DataFrame(raw_names).set_index('Name')
dfopeninteresttotal = pd.DataFrame(raw_names).set_index('Name')

url = 'https://www.cboe.com/us/options/market_statistics/daily/'

params = {'mkt': 'cone'}

dtindex = pd.date_range(start='10/07/2019', end='01/08/2021', freq = "B")
#          'dt': '2021-01-07'}

for dt in dtindex:
    params['dt'] = dt.strftime('%Y-%m-%d')
    
    html = session.get(url = url, params = params, headers=headers)
    
    soup = BeautifulSoup(html.text, 'lxml')
    
    date = soup.find('h3', id='stats-date-header').get_text().split(',')[1].strip().split(' ')
    date.append(soup.find('h3', id='stats-date-header').get_text().split(',')[2].strip())
    date = ','.join(date)
    
    if dt.strftime('%B,%-d,%Y') != date:
        continue
    
    ratios = soup.find_all('table', class_ = 'bats-table bats-table--left')
    
    
    ratios_total = []
    
    for ratio in ratios[0].find_all('tr'):
        if len(ratio) == 3:
            continue
        ratio_name = ratio.find_all('td', class_='bats-td--left')[0].get_text()
        ratio_value = float(ratio.find_all('td', class_='bats-td--left')[1].get_text())
        ratio_tmp = [ratio_name, ratio_value]
        ratios_total.append(ratio_tmp)
        
    df = pd.DataFrame(ratios_total, columns=['Ratios', dt.strftime('%d-%m-%Y')]).set_index('Ratios')
    dfratios = pd.concat([dfratios, df], axis = 1)
    
    columns = [dt.strftime('%d-%m-%Y')]
    columns.insert(0, 'Name')
    
    volumeput = []
    volumecall = []
    volumetotal = []
    openinterestput = []
    openinterestcall = []
    openinteresttotal = []
           
    for ratio in ratios[1:]:
        
        name = ratio.find_all('tr')[0].find_all('th', class_ = 'bats-th--left')[0].get_text()
        
        putvalue = int(ratio.find_all('tr')[2].find_all('td')[1].get_text().replace(',','').strip())
        callvalue = int(ratio.find_all('tr')[2].find_all('td')[2].get_text().replace(',','').strip())
        totalvalue = int(ratio.find_all('tr')[2].find_all('td')[3].get_text().replace(',','').strip())
        
        volumeput.append([name, putvalue])
        volumecall.append([name, callvalue])
        volumetotal.append([name, totalvalue])
        
        putoi = int(ratio.find_all('tr')[3].find_all('td')[1].get_text().replace(',','').strip())
        calloi = int(ratio.find_all('tr')[3].find_all('td')[2].get_text().replace(',','').strip())
        totaloi = int(ratio.find_all('tr')[3].find_all('td')[3].get_text().replace(',','').strip())
        
        openinterestput.append([name, putoi])
        openinterestcall.append([name, calloi])
        openinteresttotal.append([name, totaloi])
        
    dfvolumeputtmp = pd.DataFrame(volumeput, columns=columns).set_index('Name')
    dfvolumecalltmp = pd.DataFrame(volumecall, columns=columns).set_index('Name')
    dfvolumetotaltmp = pd.DataFrame(volumetotal, columns=columns).set_index('Name')
    dfvolumeput = pd.concat([dfvolumeput, dfvolumeputtmp], axis = 1)
    dfvolumecall = pd.concat([dfvolumecall, dfvolumecalltmp], axis = 1)
    dfvolumetotal = pd.concat([dfvolumetotal, dfvolumetotaltmp], axis = 1)
        
    dfoiputtmp = pd.DataFrame(openinterestput, columns=columns).set_index('Name')
    dfoicalltmp = pd.DataFrame(openinterestcall, columns=columns).set_index('Name')
    dfoitotaltmp = pd.DataFrame(openinteresttotal, columns=columns).set_index('Name')
    dfopeninterestput = pd.concat([dfopeninterestput, dfoiputtmp], axis = 1)
    dfopeninterestcall = pd.concat([dfopeninterestcall, dfoicalltmp], axis = 1)
    dfopeninteresttotal = pd.concat([dfopeninteresttotal, dfoitotaltmp], axis = 1)
    
    time.sleep(0.1)
    
       
dfratios.to_csv('Cboe_Ratios_Daily_Market_Statistics.csv')
dfvolumeput.to_csv('Cboe_Daily_Market_Put_Volume.csv')
dfvolumecall.to_csv('Cboe_Daily_Market_Call_Volume.csv')
dfvolumetotal.to_csv('Cboe_Daily_Market_Total_Volume.csv')
dfopeninterestput.to_csv('Cboe_Daily_Market_Put_OI.csv')
dfopeninterestcall.to_csv('Cboe_Daily_Market_Call_OI.csv')
dfopeninteresttotal.to_csv('Cboe_Daily_Market_Total_OI.csv')

        
        
    
    
    