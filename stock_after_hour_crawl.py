import requests
import time
import datetime
import argparse
import pandas as pd
from io import StringIO

def get_day_nday_ago(date,n):
    t = time.strptime(date, "%Y-%m-%d")
    y, m, d = t[0:3]
    Date = str(datetime.datetime(y, m, d) - datetime.timedelta(n)).split()
    return Date[0]
	
def get_nday_list(date, n):
	before_n_days = []
	for i in range(1, n + 1)[::-1]:
		n_date = date - datetime.timedelta(days=i)
		print(n_date.weekday())
		if n_date.weekday() != 5 and n_date.weekday() != 6:
			before_n_days.append(str(n_date).replace('-', ''))
	return before_n_days

def gen_dates(start_date, end_date):
	days = (end_date - start_date).days + 1
	day = datetime.timedelta(days=1)
	for i in range(days):
		yield start_date + day*i

def get_date_list(start_date, end_date):
	if start_date > end_date:
		print('end_date must be after start_date.')
		return [];
	date_list = gen_dates(start_date, end_date)
	before_n_days = []
	for d in date_list:
		if d.weekday() != 5 and d.weekday() != 6:
			before_n_days.append(str(d).replace('-', ''))
	return before_n_days

def download_exchange_report(save_path, report_url, format_data, col_count):
	url = report_url + '?' + format_data
	
	res = requests.get(url)
	if len(res.text) == 0:
		print('report url:' + report_url + ' request error.')
		return -1
		
	lines = res.text.split('\n')
	newlines = []
	row_count = 0
	for line in lines:
		if len(line.split('",')) == col_count:
			newlines.append(line)
			row_count += 1

	if row_count < 2:
		print('report url:' + report_url + ' empty error.')
		return -1

	merge_str = "\n".join(newlines).replace('=', '')

	if len(merge_str) == 0:
		print('report url:' + report_url + ' error.')
		return -1

	df = pd.read_csv(StringIO(merge_str))
	df = df.astype(str)  # 轉換成字串
	df = df.apply(lambda s: s.str.replace('nan', ''))  # 匿名函式
	df = df.apply(lambda s: s.str.replace(',', ''))  # 匿名函式
	df = df[df.columns[df.isnull().sum() != len(df)]]  # 刪除掉不要的表格

	df.to_csv(save_path, encoding='utf_8_sig')
	
if __name__ == "__main__":
	delay_time = 7
	web_url = 'https://www.twse.com.tw/'
	
	parser = argparse.ArgumentParser()
	parser.add_argument('start_date')
	parser.add_argument('end_date')
	parsed_args = parser.parse_args(None)
	
	start_date = datetime.datetime.strptime(parsed_args.start_date, "%Y%m%d").date()
	end_date = datetime.datetime.strptime(parsed_args.end_date, "%Y%m%d").date()
	date_list = get_date_list(start_date, end_date)
	
	for date in date_list:
		print(date)
		
		report_type = 'MI_INDEX' # 每日收盤行情
		query_url = web_url + '{0}/{1}'.format('exchangeReport', report_type)
		format_data = 'response=csv&date={0}&type={1}'.format(date, 'ALLBUT0999')
		save_path = './' + report_type + '/' + report_type + '_' + date + '.csv'
		download_exchange_report(save_path, query_url, format_data, 17)
		
		time.sleep(delay_time)
		
		report_type = 'TWTB4U' # 當日沖銷交易標的及成交量值
		query_url = web_url + '{0}/{1}'.format('exchangeReport', report_type)
		format_data = 'response=csv&date={0}&selectType={1}'.format(date, 'All')
		save_path = './' + report_type + '/' + report_type + '_' + date + '.csv'
		download_exchange_report(save_path, query_url, format_data, 7)
		
		time.sleep(delay_time)
		
		report_type = 'TWT53U' # 零股交易行情單
		query_url = web_url + '{0}/{1}'.format('exchangeReport', report_type)
		format_data = 'response=csv&date={0}&selectType={1}'.format(date, 'ALLBUT0999')
		save_path = './' + report_type + '/' + report_type + '_' + date + '.csv'
		download_exchange_report(save_path, query_url, format_data, 11)
		
		time.sleep(delay_time)
		
		report_type = 'BFT41U' # 盤後定價交易
		query_url = web_url + '{0}/{1}'.format('exchangeReport', report_type)
		format_data = 'response=csv&date={0}&selectType={1}'.format(date, 'ALLBUT0999')
		save_path = './' + report_type + '/' + report_type + '_' + date + '.csv'
		download_exchange_report(save_path, query_url, format_data, 9)
		
		time.sleep(delay_time)
		
		report_type = 'T86' # 三大法人買賣超日報
		query_url = web_url + '{0}/{1}'.format('fund', report_type)
		format_data = 'response=csv&date={0}&selectType={1}'.format(date, 'ALLBUT0999')
		save_path = './' + report_type + '/' + report_type + '_' + date + '.csv'
		download_exchange_report(save_path, query_url, format_data, 20)
		
		time.sleep(delay_time)
		
		report_type = 'TWT72U' # 證交所借券系統與證商/證金營業處所借券餘額合計表
		query_url = web_url + '{0}/{1}'.format('exchangeReport', report_type)
		format_data = 'response=csv&date={0}&selectType={1}'.format(date, 'SLBNLB')
		save_path = './' + report_type + '/' + report_type + '_' + date + '.csv'
		download_exchange_report(save_path, query_url, format_data, 10)
		
		time.sleep(delay_time)
		
		report_type = 'TWT93U' # 信用額度總量管制餘額表
		query_url = web_url + '{0}/{1}'.format('exchangeReport', report_type)
		format_data = 'response=csv&date={0}&selectType={1}'.format(date, 'ALLBUT0999')
		save_path = './' + report_type + '/' + report_type + '_' + date + '.csv'
		download_exchange_report(save_path, query_url, format_data, 16)
		
		time.sleep(delay_time)
		
		report_type = 'MI_MARGN' # 融資融券彙總
		query_url = web_url + '{0}/{1}'.format('exchangeReport', report_type)
		format_data = 'response=csv&date={0}&selectType={1}'.format(date, 'ALL')
		save_path = './' + report_type + '/' + report_type + '_' + date + '.csv'
		download_exchange_report(save_path, query_url, format_data, 17)
		
		time.sleep(delay_time)
	
	
	
	
	
	
	