import cx_Oracle
import csv
import os as os
import proj_constant_var as const

default_files_dir = os.getcwd()+ const.folder_to_process_file
conn_str = cx_Oracle.connect(const.database_connection)

def fetch_stock_list():
    print '-- Fetching stock list with date range --'
    from_oracle_cursr = conn_str.cursor();
    from_oracle_cursr.execute("select * from table(finance_analysis.out_stock_list())");
    stock_list = from_oracle_cursr.fetchall();
    from_oracle_cursr.close()
    #conn_str.commit()
    #conn_str.close()
    print '-- Fetching completed --'
    return stock_list

def fetch_candlestick_findings():
    print '-- fetch_candlestick_findings --'
    from_oracle_cursr = conn_str.cursor();
    from_oracle_cursr.execute("select * from table(finance_analysis.out_candle_stick_pattern())");
    findings = from_oracle_cursr.fetchall();
    from_oracle_cursr.close()
    #conn_str.commit()
    #conn_str.close()
    print '-- fetch_candlestick_findings completed --'
    return findings
