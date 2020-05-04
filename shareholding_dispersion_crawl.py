#from htmltable_df.extractor import Extractor

from bs4 import BeautifulSoup
import requests
import re
import sys
import argparse

import time
import datetime

def extract_number_text(src_str):
    return "".join(filter(lambda ch: ch in '0123456789.-', src_str))

filename_prefix = 'stock_shareholding_dispersion'

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('stock_number')
    parser.add_argument('start_date', nargs = '?', default = '')
    parser.add_argument('end_date', nargs = '?', default = '')
    parsed_args = parser.parse_args(None)
    
    delay_time = 7
    
    url='https://www.tdcc.com.tw/smWeb/QryStockAjax.do'
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Content-Type': 'application/x-www-form-urlencoded',
        'X-Requested-With': 'XMLHttpRequest',
    }
    
    date_format_data = {
        'REQ_OPR':'qrySelScaDates'
    }
    
    date_html = requests.post(url, data=date_format_data, headers=headers)

    #print(date_html.text)
    
    lines = date_html.text.split(',')
    
    if parsed_args.start_date:
        start_date = datetime.datetime.strptime(parsed_args.start_date, "%Y%m%d").date()
    else:
        start_date = datetime.date.min
    
    if parsed_args.end_date:
        end_date = datetime.datetime.strptime(parsed_args.end_date, "%Y%m%d").date()
    else:
        end_date = datetime.date.max
    
    if start_date > end_date:
        print('end_date must be greater than or equal to start_date.')

    query_dates = []
    for line in lines:
        date_str = "".join(filter(lambda ch : ch in '0123456789', line))
        current_date = datetime.datetime.strptime(date_str, "%Y%m%d").date()
        if current_date >= start_date and current_date <= end_date:
            query_dates.append(date_str)
    
    f = open(filename_prefix + '_' + parsed_args.stock_number + '.csv','w', encoding='utf-8')
    #﻿資料日期 證券代號 持股分級 人數 股數 占集保庫存數比例%
    f.write('﻿資料日期' + ',')
    f.write('證券代號' + ',')
    f.write('持股分級' + ',')
    f.write('人數' + ',')
    f.write('股數' + ',')
    f.write('占集保庫存數比例%' + ',')
    f.write('\n')
    
    for query_date in query_dates:
        stock_format_data = {
            'scaDates' : query_date,
            'scaDate' : query_date,
            'SqlMethod' : 'StockNo',
            'StockNo' : '',
            'radioStockNo' : parsed_args.stock_number,
            'StockName' : '',
            'REQ_OPR' : 'SELECT',
            'clkStockNo' : parsed_args.stock_number,
            'clkStockName' : ''
        }

        ret = False
        while not ret:
            print('===================' + query_date + '=====================================')
            try:
                html=requests.post(url, data=stock_format_data, headers=headers)
            except:
                # sys.exc_info()[0] 就是用來取出except的錯誤訊息的方法
                print("Unexpected error:", sys.exc_info()[0])
                print('start_date: {}, end_date: {}, query_date: {}'.format(start_date, end_date, query_date))
                break
            print('query finish...')
            
            #parsed_html = BeautifulSoup(html.text, 'html.parser', from_ncoding = html.encoding)
            parsed_html = BeautifulSoup(html.text, 'html.parser')
            print(parsed_html.text)
            tb = parsed_html.select('.mt')[1]
            first_element_text = tb.select('tr')[1].select('td')[0].text
            if first_element_text == '無此資料':
                print('request is empty. start_date: {}, end_date: {}, query_date: {}'.format(start_date, end_date, query_date))
                time.sleep(delay_time)
            else:
                ret = True
            
        other_row = tb.select('tr')[1:]
        print(other_row)
        has_row_16 = False
        for tr in other_row:
            row_text = extract_number_text(tr.select('td')[0].text)
            if row_text:
                if row_text == '16':
                    has_row_16 = True
                f.write(query_date + ',')
                f.write(parsed_args.stock_number + ',')
                f.write(extract_number_text(tr.select('td')[0].text) + ',')
                f.write(extract_number_text(tr.select('td')[2].text) + ',')
                f.write(extract_number_text(tr.select('td')[3].text) + ',')
                f.write(extract_number_text(tr.select('td')[4].text) + ',')
            else:
                if not has_row_16:
                    f.write(query_date + ',')
                    f.write(parsed_args.stock_number + ',')
                    f.write('16,0,0,0\n')
                f.write(query_date + ',')
                f.write(parsed_args.stock_number + ',')
                f.write('17,')
                f.write(extract_number_text(tr.select('td')[2].text) + ',')
                f.write(extract_number_text(tr.select('td')[3].text) + ',')
                f.write(extract_number_text(tr.select('td')[4].text) + ',')
            f.write('\n')
        time.sleep(delay_time)
    f.close()
        