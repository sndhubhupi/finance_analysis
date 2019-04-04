import To_Oracle
import From_Oracle
import To_Telegram
import Get_Prices
import pandas as pd
import proj_constant_var as const
import datetime

def send_text_to_telegram(findings):
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') +" : Sending Text to telegram Started" 
    for id in const.telegram_id_list:
        for record in findings:
            To_Telegram.sendTelegram(record,id)
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') +" : Sending Text to telegram Finished" 

def load_daily_price_data() :
    To_Oracle.insert_data_to_stock_list()
    stock_list = From_Oracle.fetch_stock_list()
    Get_Prices.run_load_for_stock_list(stock_list)
    To_Oracle.load_all_download_price_to_db()
    To_Oracle.update_earliest_latest_dt()
    To_Oracle.calc_moving_average()
    To_Oracle.find_candle_stick_pattern()
    findings = From_Oracle.fetch_candlestick_findings()
    send_text_to_telegram(findings)
    labels = ['Stock Ticker', 'Date', 'Finding_Type', 'Discription']
    df = pd.DataFrame.from_records(findings, columns=labels)
    df.to_csv('Findings.csv')


load_daily_price_data()
