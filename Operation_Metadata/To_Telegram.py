import telegram
import requests
import time
import datetime

def sendTelegram(totalResponse, chatId):
    try:
        #print("in sendTelegram", totalResponse)
        bot_id = "bot564398612:AAEXUIfrJVFHfBnxS4Uot0Ob5vDPN8Ws69I"
        url = "https://api.telegram.org/" + bot_id + "/sendMessage?chat_id=" + str(chatId) + "&text= " + str(totalResponse)
        requests.get(url)
        return True
    except Exception as e:
        print(e)
        time.sleep(30)
        sendTelegram(totalResponse, chatId)

def send_text_to_telegram(findings):
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') +" : Sending Text to telegram Started"
    for id in const.telegram_id_list:
        for record in findings:
            sendTelegram(record,id)
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') +" : Sending Text to telegram Finished"
