import cx_Oracle
import csv
import os as os

default_files_dir = os.getcwd()+'/Files_To_Process/'
stock_list_file_name =  default_files_dir + 'stock_list.csv'
file_exists = os.path.isfile(stock_list_file_name)
conn_str = cx_Oracle.connect('db_hist_data/db_hist_data@localhost/orcl')

def insert_data_to_stock_list():
    cursr = conn_str.cursor()
    if file_exists :
        with open(stock_list_file_name, "r") as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=',')
            for lines in csv_reader:
                try:
                    cursr.execute(
                        "insert into stock_info_list (stock_ticker, stock_name, nifty50, sensex, other) values (:1, :2, :3, :4, :5)",
                        (lines['stock_ticker'], lines['stock_name'], lines['nifty50'], lines['sensex'], lines['other']))
                except:
                    print("Something went wrong when writing to the database")
    else:
        print "File not exists : " + stock_list_file_name
    cursr.close()
    conn_str.commit()
    conn_str.close()

insert_data_to_stock_list()