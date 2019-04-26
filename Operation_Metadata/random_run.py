import To_Oracle
import From_Oracle
import To_Telegram
import Get_Prices
import pandas as pd
import proj_constant_var as const
import datetime
import os


labels = ['Stock Ticker', 'Date', 'Finding_Type', 'Discription']
    # Get data for previous date
previous_date = From_Oracle.get_previous_date()
To_Oracle.find_candle_stick_pattern(previous_date)
findings_prev = From_Oracle.fetch_candlestick_findings()
df = pd.DataFrame.from_records(findings_prev, columns=labels)
findings_prev_file = const.findings_folder + const.finding_file + '_' + max(df['Date']).strftime('%Y%m%d') + const.csv_extension
df.to_csv(findings_prev_file)
    # finish




