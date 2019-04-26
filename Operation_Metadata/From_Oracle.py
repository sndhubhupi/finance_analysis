import cx_Oracle
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