import cx_Oracle
import csv
import os as os
import proj_constant_var as const
import glob

conn_str = cx_Oracle.connect(const.database_connection)

def insert_data_to_stock_list():
    print 'insert_data_to_stock_list started'
    to_oracle_cursr = conn_str.cursor()
    to_oracle_cursr.callproc('finance_analysis.truncate_table',['stg_stock_info_list'])
    default_files_dir = os.getcwd() + const.folder_to_process_file
    stock_list_file_name = default_files_dir + const.stock_list_file
    file_exists = os.path.isfile(stock_list_file_name)
    if file_exists :
        with open(stock_list_file_name, "r") as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=',')
            for lines in csv_reader:
                try:
                    to_oracle_cursr.execute(
                        "insert into stg_stock_info_list (stock_ticker, stock_name, nifty50, sensex, other) values (:1, :2, :3, :4, :5)",
                        (lines['stock_ticker'], lines['stock_name'], lines['nifty50'], lines['sensex'], lines['other']))
                except:
                    print("Something went wrong when writing to the database")
    else:
        print "File not exists : " + stock_list_file_name
    to_oracle_cursr.callproc('finance_analysis.load_stock_list_from_stg')
    to_oracle_cursr.close()
    conn_str.commit()
    #conn_str.close()
    print 'insert_data_to_stock_list finshed'


def load_stock_price_file_to_db(stock_ticker_csv):
    stock_ticker_file = os.path.basename(stock_ticker_csv)
    stock_ticker = stock_ticker_file.replace(const.csv_extention, '')
    print 'load_stock_prize_to_db started for: ' + stock_ticker
    cur = conn_str.cursor()
    cur.callproc('finance_analysis.truncate_table',['stg_stock_price_data'])
    with open(stock_ticker_csv, "r") as csv_file:
        fields = ['timestamp','open','high','low','close','volume']
        csv_reader = csv.DictReader(csv_file, fieldnames=fields, delimiter=',')
        next(csv_reader)
        for lines in csv_reader:
            try :
                cur.execute(
                    "insert into stg_stock_price_data (stock_ticker,business_date,price_high,price_low,price_open,"
                    "price_close,volume) values (:1, to_date(:2,'YYYY-MM-DD'), :3, :4, :5, :6, :7)",
                                (stock_ticker, lines['timestamp'], lines['high'], lines['low'],lines['open'], lines['close'], lines['volume']))
            except :
                break
    os.remove(stock_ticker_csv)
    cur.callproc('finance_analysis.load_price_data_from_stg')
    cur.close()
    conn_str.commit()
    #conn_str.close()
    print 'load_stock_prize_to_db finshed for: ' + stock_ticker

def load_all_download_price_to_db():
    get_all_file_list = glob.glob("/Users/sandhu/PycharmProjects/Finance_Analysis/Operation_Metadata/Download_csv/*.csv")
    print get_all_file_list
    for stock_ticker_csv in get_all_file_list:
        print 'Calling Function load_stock_price_file_to_db for :' + stock_ticker_csv
        load_stock_price_file_to_db(stock_ticker_csv)

def calc_moving_average():
    cur = conn_str.cursor()
    
    print 'Moving Average 200 calculation started '
    cur.callproc('calculation.calc_moving_average_200')
    print 'Moving Average calculation finished'

    print 'Moving Average 50 calculation started'
    cur.callproc('calculation.calc_moving_average_50')
    print 'Moving Average calculation finished'

    print 'Moving Average 10 calculation started'
    cur.callproc('calculation.calc_moving_average_10')
    print 'Moving Average calculation finished'

    print 'Moving Average 8 calculation started'
    cur.callproc('calculation.calc_moving_average_8')
    print 'Moving Average calculation finished'
    
    cur.close()
    conn_str.commit()
    #conn_str.close()

def update_earliest_latest_dt():
        print 'update_earliest_latest_dt calculation started'
        cur = conn_str.cursor()
        cur.callproc('calculation.update_earliest_latest_dt')
        cur.close()
        conn_str.commit()
        # conn_str.close()
        print 'update_earliest_latest_dt calculation finished'


def find_candle_stick_pattern():
    print 'find_candle_stick_pattern calculation started'
    cur = conn_str.cursor()
    cur.callproc('finance_analysis.find_candle_stick_pattern')
    cur.close()
    conn_str.commit()
    # conn_str.close()
    print 'find_candle_stick_pattern calculation finished'

