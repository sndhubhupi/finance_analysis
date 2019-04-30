import cx_Oracle
import pandas as pd
import csv
import os as os
import proj_constant_var as const
import datetime

default_files_dir = os.getcwd()+ const.folder_to_process_file
conn_str = cx_Oracle.connect(const.database_connection)

def fetch_stock_list():
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '-- Fetching stock list to load data --'
    from_oracle_cursr = conn_str.cursor();
    from_oracle_cursr.execute("select * from table(finance_analysis.out_stock_list())");
    stock_list = from_oracle_cursr.fetchall();
    from_oracle_cursr.close()
    #conn_str.commit()
    #conn_str.close()
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '-- Fetching completed --'
    return stock_list

def fetch_candlestick_findings():
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '-- fetch_candlestick_findings --'
    from_oracle_cursr = conn_str.cursor();
    from_oracle_cursr.execute("select * from table(finance_analysis.out_candle_stick_pattern())");
    findings = from_oracle_cursr.fetchall();
    from_oracle_cursr.close()
    #conn_str.commit()
    #conn_str.close()
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '-- fetch_candlestick_findings completed --'
    return findings

def get_previous_date():
    from_oracle_cursr = conn_str.cursor();
    from_oracle_cursr.execute("select max(business_date)-1 from  stock_price_data");
    previous_date = from_oracle_cursr.fetchall();
    from_oracle_cursr.close()
    return previous_date[0][0].strftime('%d-%m-%Y')

def get_price_data_create_file(stock_ticker):
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + 'fetching price history data for : ' + stock_ticker
    from_oracle_cursr = conn_str.cursor();
    from_oracle_cursr.execute("select business_date,price_close,price_high,price_low,price_open,volume "
                              "from stock_price_data where stock_ticker = :stock order by business_date asc", stock = stock_ticker);
    labels = ['Business_date', 'price_close' ,'price_high', 'price_low', 'price_open','Volume']
    stock_price_data = from_oracle_cursr.fetchall();
    df = pd.DataFrame.from_records(stock_price_data, columns=labels)
    price_file = const.stock_price_folder + stock_ticker + const.csv_extension
    df.to_csv(price_file,header=False,index=False)
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + 'File Created : ' + price_file
    return price_file
