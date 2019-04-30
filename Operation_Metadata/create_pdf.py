from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.pagesizes import landscape
from reportlab.platypus import Image
import csv
import From_Oracle
import create_chart
import matplotlib.dates as mdates
import numpy as np
import datetime

def create_pdf(finding_file):
    pdf_file_name = 'pdf_file_name.pdf'
    c = canvas.Canvas(pdf_file_name, pagesize=landscape(letter))
    c.setFont('Helvetica-Bold', 35, leading=None)
    c.drawCentredString(400,500,'TODAY FINDING')
    c.showPage()

    finding_data = csv.reader(open(finding_file,'rb'))
    for row in finding_data:
        stock_ticker = row[0];
        business_date = row[1];
        finding_type = row[2];
        price_data_file =  From_Oracle.get_price_data_create_file(stock_ticker);
        chart_file = create_chart.graph_data(stock_ticker,price_data_file);
        # Stock Name and business date and finding type
        c.setFont('Helvetica-Bold', 15, leading=None)
        c.drawString(5, 580, 'Stock Name : ' + stock_ticker)
        c.setFont('Helvetica-Bold', 15, leading=None)
        c.drawString(5, 560, 'Finding Type : ' + finding_type)
        c.setFont('Helvetica-Bold', 15, leading=None)
        c.drawString(5, 540, 'Date  : ' + business_date)
        # chart file
        c.drawImage(chart_file, 5, 25, width=790, height=500)
        c.showPage()
    c.save()

create_pdf('/Users/sandhu/PycharmProjects/Finance_Analysis/Operation_Metadata/Finding/Finding_20190430.csv')