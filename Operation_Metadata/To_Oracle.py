import cx_Oracle
import csv
import os as os
import proj_constant_var as const
import Get_Prices
import From_Oracle

default_files_dir = os.getcwd()+ const.folder_to_process_file
stock_list_file_name =  default_files_dir + const.stock_list_file
file_exists = os.path.isfile(stock_list_file_name)
conn_str = cx_Oracle.connect(const.database_connection)

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


def load_stock_prize_to_db():
    stock_dt_range = From_Oracle.fetch_stock_list_dt_range()
    for record in stock_dt_range:
        stock_ticker =  record[0];
        stock_from_dt = record[1];
        stock_to_dt = record[2];
        file_generated = Get_Prices.get_stock_price_dt_range(stock_ticker,stock_from_dt,stock_to_dt);
        if file_generated == 'NA':
            continue
        else:
            cur = conn_str.cursor()
            cur.execute("truncate table stg_stock_price_data");
            with open(file_generated, "r") as csv_file:
                fields = ['Date','High','Low','Open','Close','Volume','Adj Close']
                csv_reader = csv.DictReader(csv_file, fieldnames=fields, delimiter=',')
                next(csv_reader)
                for lines in csv_reader:
                    cur.execute(
                        "insert into stg_stock_price_data (stock_ticker,business_date,price_high,price_low,price_open,"
                        "price_close,volume,adj_close) values (:1, to_date(:2,'YYYY-MM-DD'), :3, :4, :5, :6, :7, :8)",
                        (stock_ticker, lines['Date'], lines['High'], lines['Low'],lines['Open'], lines['Close'], lines['Volume'], lines['Adj Close']))
            cur.close()
            conn_str.commit()
            conn_str.close()

load_stock_prize_to_db()