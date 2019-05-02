import To_Oracle
import From_Oracle
import To_Telegram
import Get_Prices
import Maintenance
import pandas as pd
import proj_constant_var as const
import create_pdf
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

def find_candlestick(num):
    labels = ['Stock Ticker', 'Date', 'Finding_Type', 'Discription']
    if num == 0 or num == None:
        Header_Text = 'Latest Day Findings'
        Date = None
    else:
        Header_Text = 'Previous ' + str(num) + 'th Day Findings for Analysis '
        Date = From_Oracle.get_previous_date(num)
    To_Oracle.find_candle_stick_pattern(Date)
    findings = From_Oracle.fetch_candlestick_findings()
    df = pd.DataFrame.from_records(findings, columns=labels)
    findings_file = const.findings_folder + const.finding_file + '_' + max(df['Date']).strftime('%Y%m%d') + const.csv_extension
    df.to_csv(findings_file,header=False,index=False)
    pdf_file_name = create_pdf.create_pdf(findings_file,Header_Text)
    #To_Telegram.send_text_to_telegram(findings)
    To_Telegram.send_pdf_to_user(pdf_file_name)


#load_daily_price_data()
#find_candlestick(None)
#find_candlestick(2)
#find_candlestick(3)

#Maintenance.cleanup(0,'/Users/sandhu/PycharmProjects/Finance_Analysis/Operation_Metadata/price_data_files/')

To_Oracle.update_earliest_latest_dt()
To_Oracle.calc_moving_average()
To_Oracle.calc_pivot_demark()