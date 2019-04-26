import datetime
import os

def validate(date_text):
    try:
        datetime.datetime.strptime(date_text, '%d-%m-%Y')
    except ValueError:
        raise ValueError("Incorrect data format, should be DD-MM-YYYY")

folder_to_process_file = '/Files_To_Process/' ;
findings_folder = os.getcwd()+ '/Finding/'
finding_file = 'Finding'
downloaded_csv_folder = '/Download_csv/';
database_connection = 'db_hist_data/db_hist_data@localhost/orcl' ;
csv_extention = '.csv'
csv_extension = '.csv'
stock_list_file = 'stock_list.csv' ;
telegram_id_list = ['464308445','506426930']
api_keys = ['TJFXVG58IVH4N7OM','8EL6PXCIYJD56OE8','2DEF4M6SGR4S5H0B','6VWDXQX7F23UYOV8','A5R41DURGC8MQJHM',
            'XE3BFLI3SBKRONE4','VNHUMPT6WPDFSHS2','QFQITXIAWTZBDUAM','20UMVZQY1T4PPB8Q','K28LMSFMYGL45IL2'];
