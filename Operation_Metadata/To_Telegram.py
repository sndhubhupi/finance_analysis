import telegram
import requests
import time


def sendTelegram(totalResponse, chatId):
    try:
        print("in sendTelegram", totalResponse)
        bot_id = "bot564398612:AAEXUIfrJVFHfBnxS4Uot0Ob5vDPN8Ws69I"
        url = "https://api.telegram.org/" + bot_id + "/sendMessage?chat_id=" + str(chatId) + "&text= " + str(totalResponse)
        requests.get(url)
        return True
    except Exception as e:
        print(e)
        time.sleep(30)
        sendTelegram(totalResponse, chatId)

