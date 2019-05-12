#988934615 - Sameer Khan
#989212531 - Nikhat Khan

import numpy as np
import datetime
import proj_constant_var as const
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
plt.rcParams.update({'figure.max_open_warning': 0})
import matplotlib.ticker as mticker
import matplotlib.dates as mdates
from mpl_finance import candlestick_ohlc
matplotlib.rcParams.update({'font.size': 9})

def bytespdate2num(fmt, encoding='utf-8'):
    strconverter = mdates.strpdate2num(fmt)
    def bytesconverter(b):
        s = b.decode(encoding)
        return strconverter(s)
    return bytesconverter


def graph_data(stock, stock_file):
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '     Creating Chart for : ' + stock
    date, closep, highp, lowp, openp, volume, dma50, dma10 = np.genfromtxt(stock_file, delimiter=',', unpack=True, usecols=[0,1,2,3,4,5,6,7],
                                                          converters={0: mdates.strpdate2num('%Y-%m-%d')})
    ymin = min(lowp)
    ymax = max(highp)

    candleAr = zip(range(len(date)), openp, highp, lowp, closep, dma50, dma10)
    class Jackarow(mdates.DateFormatter):
        def __init__(self, fmt):
            mdates.DateFormatter.__init__(self, fmt)
        def __call__(self, x, pos=0):
            # This gets called even when out of bounds, so IndexError must be prevented.
            if x < 0:
                x = 0
            elif x >= len(date):
                x = -1
            return mdates.DateFormatter.__call__(self, date[int(x)], pos)

    fig = plt.figure()
    ax1 = plt.subplot2grid((5, 4), (0, 0), rowspan=4, colspan=4)
    candlestick_ohlc(ax1, candleAr, width=.4, colorup='#77d879', colordown='#db3f3f')
    plt.ylabel('Stock Price')
    ax1.grid(True)
    ax1.plot(range(len(date)),dma50, color= 'r', label = 'DMA50')
    #ax1.plot(range(len(date)), dma200, color='red', label='DMA200')
    ax1.plot(range(len(date)), dma10, color='g', label='DMA10')

    ax2 = plt.subplot2grid((5, 4), (4, 0), sharex=ax1, rowspan=1, colspan=4)
    ax2.bar(range(len(date)), volume)
    ax2.axes.yaxis.set_ticklabels([])
    plt.ylabel('Volume')
    ax2.grid(True)
    for label in ax2.xaxis.get_ticklabels():
        label.set_rotation(45)

    ax1.xaxis.set_major_locator(mticker.MaxNLocator(10))
    ax1.yaxis.set_major_locator(mticker.MaxNLocator(20))
    ax1.xaxis.set_major_formatter(Jackarow('%Y-%m-%d'))

    plt.subplots_adjust(left=.10, bottom=.19, right=.93, top=.95, wspace=.20, hspace=0)
    # plt.ylim([ymin, ymax])
    plt.suptitle(stock + ' Stock Price')
    plt.setp(ax1.get_xticklabels(), visible=False)
    #a=datetime.datetime(2019,1,1)
    #b=datetime.datetime(2019,3,9)
    #ax1.axvspan(a,b,color='yellow', alpha=0.5)

    image_file = const.stock_price_folder + stock + const.png_extention
    plt.savefig(image_file,bbox_inches='tight', dpi = 500)
    #plt.show()
    print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '     Chart created and saved in : ' + image_file
    return image_file



#graph_data('ABCAPITAL.NSE','/Users/sandhu/PycharmProjects/Finance_Analysis/Operation_Metadata/price_data_files/ABCAPITAL.NSE.csv')