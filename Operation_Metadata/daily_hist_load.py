import To_Oracle
import From_Oracle
import To_Telegram
import Get_Prices
import pandas as pd
import proj_constant_var as const
import datetime
import os

def load_daily_price_data() :
    To_Oracle.insert_data_to_stock_list()
    #load in case of failure while downloading
    To_Oracle.load_all_download_price_to_db()
    To_Oracle.update_earliest_latest_dt()
    #finished
    stock_list = From_Oracle.fetch_stock_list()
    Get_Prices.run_load_for_stock_list(stock_list)
    To_Oracle.load_all_download_price_to_db()
    To_Oracle.update_earliest_latest_dt()
    To_Oracle.calc_moving_average()
    To_Oracle.calc_pivot_demark()
    To_Oracle.find_candle_stick_pattern(None)
    findings = From_Oracle.fetch_candlestick_findings()
    labels = ['Stock Ticker', 'Date', 'Finding_Type', 'Discription']
    df = pd.DataFrame.from_records(findings, columns=labels)
    findings_file = const.findings_folder + const.finding_file + '_' + max(df['Date']).strftime('%Y%m%d') + const.csv_extension
    df.to_csv(findings_file)
    To_Telegram.send_text_to_telegram(findings)
    # Get data for previous date
    previous_date = From_Oracle.get_previous_date()
    To_Oracle.find_candle_stick_pattern(previous_date)
    findings_prev = From_Oracle.fetch_candlestick_findings()
    df = pd.DataFrame.from_records(findings_prev, columns=labels)
    findings_prev_file = const.findings_folder + const.finding_file + '_' + max(df['Date']).strftime('%Y%m%d') + const.csv_extension
    df.to_csv(findings_prev_file)
    # finish


load_daily_price_data()

