import telegram
import requests
import time
import datetime
import proj_constant_var as const

def sendDocument():
    bot_id = "bot564398612:AAEXUIfrJVFHfBnxS4Uot0Ob5vDPN8Ws69I"
    urlText = "https://api.telegram.org/" + bot_id + "/sendPhoto"
    urlUpload = "https://api.telegram.org/" + bot_id + "/uploadFile"
    imageAdd = "https://images.theconversation.com/files/123290/original/image-20160520-4478-ziwr7.png?ixlib=rb-1.1.0&q=45&auto=format&w=1000&fit=clip"
    formData = {
        "chat_id":464308445,
        "photo":requests.get(imageAdd)
    };
    formDataUpload ={
        "type":"unknown",
        "mtime":1,
        "byte":open("pdf_file_name.pdf","r+")
    };
    data = requests.post(urlUpload,formDataUpload);
    print data.text

#sendDocument();

def delete_blank_lines(file_name):
    temp_file_name = file_name + '_temp'
    with open(file_name) as input, open(temp_file_name, 'w') as output:
        non_blank = (line for line in input if line.strip())
        output.writelines(non_blank)


delete_blank_lines('/Users/sandhu/PycharmProjects/Finance_Analysis/Operation_Metadata/Finding/Finding_20190430.csv')
