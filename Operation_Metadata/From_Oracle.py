import cx_Oracle
import csv
import os as os
import proj_constant_var as const

default_files_dir = os.getcwd()+ const.folder_to_process_file
conn_str = cx_Oracle.connect(const.database_connection)

def fetch_stock_list_dt_range():
    cursr = conn_str.cursor();
    cursr.execute("select * from table(finance_analysis.out_stock_list_dt_range())");
    stock_dt_range = cursr.fetchall();
    return stock_dt_range
