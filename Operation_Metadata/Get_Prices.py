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
    for stock_ticker  in stock_list:
        stock_ticker = str(stock_ticker)
        stock = stock_ticker.replace("('","");
        stock = stock.replace("',)","");
        print  datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' Data Load Started for ' + stock + '
        url = create_url(stock);
        print  datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' ' + url
        load_data_from_url_to_csv(url, stock);
        time.sleep(12)
        print  datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' Data Load Finished for ' + stock 



