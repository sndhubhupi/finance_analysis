# import external pandas_datareader library with alias of web
import pandas_datareader as web
import os as os
import proj_constant_var as const

# import datetime internal datetime module
# datetime is a Python module
import datetime

def get_stock_price_dt_range(stock_ticker,start_date,end_date):
    # DataReader method name is case sensitive
    try :
        df = web.DataReader(stock_ticker, 'yahoo', start_date, end_date)
    #df = web.DataReader('TCS', 'google', start, end)
    #df = web.DataReader('TCS', 'morningstar', start, end)
    #df = web.DataReader('TCS', 'iex', start, end)
    # invoke to_csv for df dataframe object from
    # DataReader method in the pandas_datareader library

    # ..\first_yahoo_prices_to_csv_demo.csv must not
    # be open in another app, such as Excel
        default_dir = os.getcwd()+ const.folder_to_process_file
        file_name = stock_ticker+'_'+str(start_date).replace("-","")[0:8]+'_'+str(end_date).replace("-","")[0:8]+'.csv'
        file_path = default_dir+file_name
        if os.path.exists(file_path):
            os.remove(file_path)
        df.to_csv(file_path)
        return file_path
    except :
        print "Chill bro, some issue with data fetching from yahoo " + stock_ticker
        return "NA"



