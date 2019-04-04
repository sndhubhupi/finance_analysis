# import external pandas_datareader library with alias of web
import os as os
import proj_constant_var as const
import requests as request
import csv
import random
import time
import datetime

default_files_dir = os.getcwd()+ const.folder_to_process_file
download_folder = os.getcwd()+ const.downloaded_csv_folder
stock_list_file_name =  default_files_dir + const.stock_list_file
file_exists = os.path.isfile(stock_list_file_name)
#stock_list = [];

#def load_stock_list():
#    with open(stock_list_file_name, "r") as csv_file:
#        csv_reader = csv.DictReader(csv_file, delimiter=',')
#        for lines in csv_reader:
#            stock_list.append(lines['stock_ticker']);

def create_url(stock_ticker):
    api_key = random.choice(const.api_keys);
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=' + stock_ticker + '&apikey=' + api_key + '&datatype=csv'
    return url;

def load_data_from_url_to_csv(url,stock_ticker):
    #url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=TCS.NSE&apikey=8EL6PXCIYJD56OE8&datatype=csv"
    data = request.get(url);
    decoded_content = data.content.decode('utf-8')
    csv_file = stock_ticker+'.csv';
    file_path = download_folder+csv_file;
    if os.path.exists(file_path):
        os.remove(file_path)
    open(file_path, 'w').write(decoded_content);

def run_load_for_stock_list(stock_list):
    for stock_ticker in stock_list:
        print 'Data Load Started for ' + stock_ticker + ' at ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        url = create_url(stock_ticker);
        print url
        load_data_from_url_to_csv(url, stock_ticker);
        time.sleep(12)
        print 'Data Load Finished for ' + stock_ticker + ' at ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')



