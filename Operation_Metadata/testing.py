import cx_Oracle
import csv
import os as os
import proj_constant_var as const
import glob

conn_str = cx_Oracle.connect(const.database_connection)

stock_ticker_file = os.path.basename('/Users/sandhu/PycharmProjects/Finance_Analysis/Operation_Metadata/Download_csv/INFY.NSE.csv')
stock_ticker = stock_ticker_file.replace(const.csv_extention, '')
print 'load_stock_prize_to_db started for: ' + stock_ticker
cur = conn_str.cursor()
cur.callproc('finance_analysis.truncate_table', ['stg_stock_price_data'])
with open('/Users/sandhu/PycharmProjects/Finance_Analysis/Operation_Metadata/Download_csv/INFY.NSE.csv', "r") as csv_file:
    fields = ['dates', 'open', 'high', 'low', 'close', 'volume']
    csv_reader = csv.DictReader(csv_file, fieldnames=fields, delimiter=',')
    next(csv_reader)
    for lines in csv_reader:
        cur.execute(
            "insert into stg_stock_price_data (stock_ticker,business_date,price_high,price_low,price_open,"
            "price_close,volume) values (:1, to_date(:2,'YYYY-MM-DD'), :3, :4, :5, :6, :7)",
            (stock_ticker, lines['dates'], lines['high'], lines['low'], lines['open'], lines['close'], lines['volume']))

# os.remove(stock_ticker_csv)
cur.callproc('finance_analysis.load_price_data_from_stg')
cur.close()
conn_str.commit()