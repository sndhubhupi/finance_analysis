# import external pandas_datareader library with alias of web
import pandas_datareader as web
import os as os

# import datetime internal datetime module
# datetime is a Python module
import datetime

def get_stock_price_dt_range(stock_ticker,start_date,end_date):



    # DataReader method name is case sensitive
    df = web.DataReader(stock_ticker, 'yahoo', start_date, end_date)
    #df = web.DataReader('TCS', 'google', start, end)
    #df = web.DataReader('TCS', 'morningstar', start, end)
    #df = web.DataReader('TCS', 'iex', start, end)
    # invoke to_csv for df dataframe object from
    # DataReader method in the pandas_datareader library

    # ..\first_yahoo_prices_to_csv_demo.csv must not
    # be open in another app, such as Excel
    default_dir = os.getcwd()+'/Files_To_Process/'
    file_name = stock_ticker+'_'+str(start_date).replace("-","")[0:8]+'_'+str(end_date).replace("-","")[0:8]+'.csv'
    file_path = default_dir+file_name
    if os.path.exists(file_path):
        os.remove(file_path)
    df.to_csv(file_path)



get_stock_price_dt_range('MARUTI.NS',datetime.datetime(2009, 9, 1),datetime.datetime(2019, 03, 16))
get_stock_price_dt_range('TCS.NS',datetime.datetime(2018, 9, 1),datetime.datetime(2019, 03, 16))
get_stock_price_dt_range('INFY.NS',datetime.datetime(2018, 9, 1),datetime.datetime(2019, 03, 16))

