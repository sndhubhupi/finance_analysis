import To_Oracle
import From_Oracle
import To_Telegram
import Get_Prices
import pandas as pd

def load_daily_price_data() :
    #To_Oracle.insert_data_to_stock_list()
    #Get_Prices.load_stock_list()
    #Get_Prices.run_load_for_stock_list()
    #To_Oracle.load_all_download_price_to_db()
    #To_Oracle.update_earliest_latest_dt()
    #To_Oracle.calc_moving_average()
    #To_Oracle.find_candle_stick_pattern()
    findings = From_Oracle.fetch_candlestick_findings()
    for x in findings:
        To_Telegram.sendTelegram(x,464308445)
        To_Telegram.sendTelegram(x, 506426930)
    labels = ['Stock Ticker', 'Date', 'Finding_Type', 'Discription']
    df = pd.DataFrame.from_records(findings, columns=labels)
    df.to_csv('Findings_20190403.csv')


load_daily_price_data()

