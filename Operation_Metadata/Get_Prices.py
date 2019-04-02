# import external pandas_datareader library with alias of web
import os as os
import proj_constant_var as const
import requests as request
import csv

default_files_dir = os.getcwd()+ const.folder_to_process_file
download_folder = os.getcwd()+ const.downloaded_csv_folder
stock_list_file_name =  default_files_dir + const.stock_list_file
file_exists = os.path.isfile(stock_list_file_name)
stock_list = [];

def load_stock_list():
    with open(stock_list_file_name, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        for lines in csv_reader:
            stock_list.append(lines['stock_ticker']);

def create_url(stock_ticker, api_key):
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

def run_load_for_stock_list(count):
    for i in range(10):
        for j in range(5):
            if(count < len(stock_list)):
                stock_ticker  = stock_list[count]
                print "apikey " + const.api_keys[i] + " j value " + str(j) + " ticker " + stock_ticker;
                url = create_url(stock_ticker,const.api_keys[i]);
                load_data_from_url_to_csv(url,stock_ticker);
                count = count + 1;
            else:
                break;
    return count;

load_stock_list()
run_load_for_stock_list(0)



