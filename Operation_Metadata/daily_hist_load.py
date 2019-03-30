import To_Oracle
import From_Oracle

def load_daily_price_data() :
    To_Oracle.insert_data_to_stock_list()
    To_Oracle.update_earliest_latest_dt()
    stock_dt_range = From_Oracle.fetch_stock_list_dt_range()
    To_Oracle.load_stock_prize_to_db(stock_dt_range)
    To_Oracle.update_earliest_latest_dt()
    To_Oracle.calc_moving_average()
    To_Oracle.update_earliest_latest_dt()



load_daily_price_data()
To_Oracle.find_candle_stick_pattern()
findings = From_Oracle.fetch_candlestick_findings()
print findings

