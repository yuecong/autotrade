from __future__ import print_function
import logging
import requests,json
import argparse
import glob

log = logging.getLogger(__name__)
print = log.info
logging.getLogger("requests").setLevel(logging.WARNING)
logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(levelname)s] %(message)s', datefmt = '%Y-%m-%d %H:%M:%S')

PRIVATE_KEY = 'ffcf6286cb14d46ea3c14f659f8b5d14-3ab610cb0f3a4e6494362960ab73d04f'
AUTH_TOKEN = {'Authorization':'Bearer '+ PRIVATE_KEY}

def get_account_info():
    "Get Account infomation."
    url = 'https://api-fxpractice.oanda.com/v1/accounts'
    output = requests.get(url, headers = AUTH_TOKEN)
    return output.text

def store_price_info_into_disk(instrument,start_time,end_time,granularity):
    """
 Get price info and dump it into local json file. 

 Parameter:
  instrument: Currency instrument. e.g. EUR_USD, USD_JPY, etc
  start_time: start time of price info dump. format: 2012-01-01T00%3A00%3A00Z
  end_time : end time for price info dump
  granularity: time period. e.g. H1, M30,M15,D, etc 

 Output:
  a json file with the above parameter price info is created. json file name is as price_<instrument>_<start_time>_<granularity>.json

 GET "https://api-fxpractice.oanda.com/v1/candles?instrument=EUR_USD&start=2012-01-01T00%3A00%3A00Z&end=2012-05-31T23%3A59%3A59Z&granularity=H1"
    """
    url = "https://api-fxpractice.oanda.com/v1/candles?" + \
          "instrument=" + instrument + \
          "&start=" + start_time + \
          "&end=" + end_time + \
          "&granularity=" + granularity
    #print(url)
    output = requests.get(url, headers = AUTH_TOKEN)
    json_filename = "price_" + instrument + "_" + start_time + "_" + granularity +".json"
    f = open(json_filename,'w')
    f.write(output.text)
    f.close()

def store_price_into_memory(instrument, granularity):
    """
 Store the price info from the dumped file in the same folder to memory 
    """
    price =[]
    path = "./price_" + instrument + "_*_" + granularity + ".json"
    files=glob.glob(path)
    for name in files:
        with open(name,'r') as data_file:
            data = json.load(data_file)
        price +=data['candles']
    #print(price[10]['time'])
    new_price_list = sorted(price, key=lambda k:k['time'])
    #print (len(new_price_list))
    return price

def parse_input():
    """Parses command line input."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-d', '--dump_price', action = 'store_true',
                        help="dump price info into local json file")

    parser.add_argument('-s', '--save_csv', action = 'store_true',
                        help="convert all json data files into a csv file")

    return parser.parse_args()

def simpleTrade1():
    """
    bid(sell) at the openning, ask(buy) at the closing for each hour
    """
    price_list_eur_usd_h1 = store_price_into_memory(instrument="EUR_USD", granularity="H1")
    profit = 0.0
    for price in price_list_eur_usd_h1:
        profit += (price['openBid'] - price['closeAsk'])
    print("Profit of simple trade 1 for EUR_USD is %f" % profit)
    
    price_list_usd_jpy_h1 = store_price_into_memory(instrument="USD_JPY", granularity="H1")
    profit = 0.0
    for price in price_list_usd_jpy_h1:
        profit += (price['openBid'] - price['closeAsk'])
    print("Profit of simple trade 1 for USD_JPY is %f" % profit)

def simpleTrade2():
    """
    Ask(Buy) at the openning, Bid(Sell) at the closing for each hour
    """
    price_list_eur_usd_h1 = store_price_into_memory(instrument="EUR_USD", granularity="H1")
    profit = 0.0
    for price in price_list_eur_usd_h1:
        profit += (price['closeBid'] - price['openAsk'])
    print("Profit of simple trade 2 for EUR_USD is %f" % profit)

    price_list_usd_jpy_h1 = store_price_into_memory(instrument="USD_JPY", granularity="H1")
    profit = 0.0
    for price in price_list_usd_jpy_h1:
        profit += (price['closeBid'] - price['openAsk'])
    print("Profit of simple trade 2 for USD_JPY is %f" % profit)

def simpleTrade3():
    """
    Ask(Buy) at the openning, Bid(Sell) at the closing for each hour
    """
    price_list_eur_usd_h1 = store_price_into_memory(instrument="EUR_USD", granularity="H1")
    profit = 0.0
    for price in price_list_eur_usd_h1:
        if (price['closeBid'] - price['openAsk'] > 0):
            profit += (price['closeBid'] - price['openAsk'])
        else:
            profit += (price['openBid'] - price['closeAsk'])
    print("Profit of simple trade 3 for EUR_USD is %f" % profit)

    price_list_usd_jpy_h1 = store_price_into_memory(instrument="USD_JPY", granularity="H1")
    profit = 0.0
    for price in price_list_usd_jpy_h1:
        if (price['closeBid'] - price['openAsk'] > 0):
            profit += (price['closeBid'] - price['openAsk'])
        else:
            profit += (price['openBid'] - price['closeAsk'])
    print("Profit of simple trade 2 for USD_JPY is %f" % profit)

def convert_to_csv():
    path = "./price_*.json"
    files=glob.glob(path)
    for name in files:
        with open(name,'r') as data_file:
            data = json.load(data_file)
            prices = data['candles']
            f = open(name[:len(name)-4] + 'csv','w')
            f.write('year,month,day,hour,minute,second,openBid,openAsk,highBid,highAsk,lowBid,lowAsk,closeBid,closeAsk,volume,complete\n')
            for price in prices:
                year = price['time'][0:4]
                month = price['time'][5:7]
                day = price['time'][8:10]
                hour = price['time'][11:13]
                minute = price['time'][14:16]
                second = price['time'][17:19]
                price_str = ( year +',' + month +','  + day +',' + hour +',' + minute +',' + second +',' +  
                              str(price['openBid']) +',' +
                              str(price['openAsk']) +',' + 
                              str(price['highBid']) +',' + 
                              str(price['highAsk']) +',' + 
                              str(price['lowBid']) +',' +
                              str(price['lowAsk']) +',' + 
                              str(price['closeBid']) +',' + 
                              str(price['closeAsk']) +',' +
                              str(price['volume']) +',' +  
                              str(price['complete'])  + '\n') 
                f.write(price_str)
            f.close()

if __name__ == '__main__':
    print("Start simple automatic trading tool...")
    args = parse_input()
    account_info = get_account_info()
    if args.dump_price:
        print("Dumping price info into local file...")
        #store_price_info_into_disk('EUR_USD','2001-01-01T00%3A00%3A00Z','2001-06-30T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the first half year of 2001
        #store_price_info_into_disk('EUR_USD','2001-07-01T00%3A00%3A00Z','2001-12-31T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the second half year of 2001
        store_price_info_into_disk('EUR_USD','2002-01-01T00%3A00%3A00Z','2002-06-30T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the first half year of 2002
        store_price_info_into_disk('EUR_USD','2002-07-01T00%3A00%3A00Z','2002-12-31T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the second half year of 2002 
        store_price_info_into_disk('EUR_USD','2003-01-01T00%3A00%3A00Z','2003-06-30T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the first half year of 2003
        store_price_info_into_disk('EUR_USD','2003-07-01T00%3A00%3A00Z','2003-12-31T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the second half year of 2003 
        store_price_info_into_disk('EUR_USD','2004-01-01T00%3A00%3A00Z','2004-06-30T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the first half year of 2004
        store_price_info_into_disk('EUR_USD','2004-07-01T00%3A00%3A00Z','2004-12-31T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the second half year of 2004 
        store_price_info_into_disk('EUR_USD','2005-01-01T00%3A00%3A00Z','2005-06-30T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the first half year of 2005
        store_price_info_into_disk('EUR_USD','2005-07-01T00%3A00%3A00Z','2005-12-31T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the second half year of 2005 
        store_price_info_into_disk('EUR_USD','2006-01-01T00%3A00%3A00Z','2006-06-30T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the first half year of 2006
        store_price_info_into_disk('EUR_USD','2006-07-01T00%3A00%3A00Z','2006-12-31T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the second half year of 2006 
        store_price_info_into_disk('EUR_USD','2007-01-01T00%3A00%3A00Z','2007-06-30T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the first half year of 2007
        store_price_info_into_disk('EUR_USD','2007-07-01T00%3A00%3A00Z','2007-12-31T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the second half year of 2007
        store_price_info_into_disk('EUR_USD','2008-01-01T00%3A00%3A00Z','2008-06-30T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the first half year of 2008
        store_price_info_into_disk('EUR_USD','2008-07-01T00%3A00%3A00Z','2008-12-31T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the second half year of 2008
        store_price_info_into_disk('EUR_USD','2009-01-01T00%3A00%3A00Z','2009-06-30T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the first half year of 2009
        store_price_info_into_disk('EUR_USD','2009-07-01T00%3A00%3A00Z','2009-12-31T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the second half year of 2009 
        store_price_info_into_disk('EUR_USD','2010-01-01T00%3A00%3A00Z','2010-06-30T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the first half year of 2010
        store_price_info_into_disk('EUR_USD','2010-07-01T00%3A00%3A00Z','2010-12-31T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the second half year of 2010
        store_price_info_into_disk('EUR_USD','2011-01-01T00%3A00%3A00Z','2011-06-30T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the first half year of 2011
        store_price_info_into_disk('EUR_USD','2011-07-01T00%3A00%3A00Z','2011-12-31T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the second half year of 2011 
        store_price_info_into_disk('EUR_USD','2012-01-01T00%3A00%3A00Z','2012-06-30T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the first half year of 2012
        store_price_info_into_disk('EUR_USD','2012-07-01T00%3A00%3A00Z','2012-12-31T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the second half year of 2012 
        store_price_info_into_disk('EUR_USD','2013-01-01T00%3A00%3A00Z','2013-06-30T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the first half year of 2013
        store_price_info_into_disk('EUR_USD','2013-07-01T00%3A00%3A00Z','2013-12-31T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the second half year of 2013 
        store_price_info_into_disk('EUR_USD','2014-01-01T00%3A00%3A00Z','2014-06-30T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the first half year of 2014
        store_price_info_into_disk('EUR_USD','2014-07-01T00%3A00%3A00Z','2014-12-31T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the second half year of 2014 
        store_price_info_into_disk('EUR_USD','2015-01-01T00%3A00%3A00Z','2015-06-30T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the first half year of 2015
        store_price_info_into_disk('EUR_USD','2015-07-01T00%3A00%3A00Z','2015-12-31T23%3A59%3A59Z','H1') # dump H1 EUR_USD price for the second half year of 2015


        #store_price_info_into_disk('USD_JPY','2001-01-01T00%3A00%3A00Z','2001-06-30T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the first half year of 2001
        #store_price_info_into_disk('USD_JPY','2001-07-01T00%3A00%3A00Z','2001-12-31T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the second half year of 2001
        store_price_info_into_disk('USD_JPY','2002-01-01T00%3A00%3A00Z','2002-06-30T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the first half year of 2002
        store_price_info_into_disk('USD_JPY','2002-07-01T00%3A00%3A00Z','2002-12-31T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the second half year of 2002
        store_price_info_into_disk('USD_JPY','2003-01-01T00%3A00%3A00Z','2003-06-30T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the first half year of 2003
        store_price_info_into_disk('USD_JPY','2003-07-01T00%3A00%3A00Z','2003-12-31T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the second half year of 2003
        store_price_info_into_disk('USD_JPY','2004-01-01T00%3A00%3A00Z','2004-06-30T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the first half year of 2004
        store_price_info_into_disk('USD_JPY','2004-07-01T00%3A00%3A00Z','2004-12-31T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the second half year of 2004
        store_price_info_into_disk('USD_JPY','2005-01-01T00%3A00%3A00Z','2005-06-30T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the first half year of 2005
        store_price_info_into_disk('USD_JPY','2005-07-01T00%3A00%3A00Z','2005-12-31T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the second half year of 2005
        store_price_info_into_disk('USD_JPY','2006-01-01T00%3A00%3A00Z','2006-06-30T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the first half year of 2006
        store_price_info_into_disk('USD_JPY','2006-07-01T00%3A00%3A00Z','2006-12-31T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the second half year of 2006
        store_price_info_into_disk('USD_JPY','2007-01-01T00%3A00%3A00Z','2007-06-30T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the first half year of 2007
        store_price_info_into_disk('USD_JPY','2007-07-01T00%3A00%3A00Z','2007-12-31T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the second half year of 2007
        store_price_info_into_disk('USD_JPY','2008-01-01T00%3A00%3A00Z','2008-06-30T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the first half year of 2008
        store_price_info_into_disk('USD_JPY','2008-07-01T00%3A00%3A00Z','2008-12-31T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the second half year of 2008
        store_price_info_into_disk('USD_JPY','2009-01-01T00%3A00%3A00Z','2009-06-30T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the first half year of 2009
        store_price_info_into_disk('USD_JPY','2009-07-01T00%3A00%3A00Z','2009-12-31T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the second half year of 2009
        store_price_info_into_disk('USD_JPY','2010-01-01T00%3A00%3A00Z','2010-06-30T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the first half year of 2010
        store_price_info_into_disk('USD_JPY','2010-07-01T00%3A00%3A00Z','2010-12-31T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the second half year of 2010
        store_price_info_into_disk('USD_JPY','2011-01-01T00%3A00%3A00Z','2011-06-30T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the first half year of 2011
        store_price_info_into_disk('USD_JPY','2011-07-01T00%3A00%3A00Z','2011-12-31T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the second half year of 2011
        store_price_info_into_disk('USD_JPY','2012-01-01T00%3A00%3A00Z','2012-06-30T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the first half year of 2012
        store_price_info_into_disk('USD_JPY','2012-07-01T00%3A00%3A00Z','2012-12-31T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the second half year of 2012 
        store_price_info_into_disk('USD_JPY','2013-01-01T00%3A00%3A00Z','2013-06-30T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the first half year of 2013
        store_price_info_into_disk('USD_JPY','2013-07-01T00%3A00%3A00Z','2013-12-31T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the second half year of 2013 
        store_price_info_into_disk('USD_JPY','2014-01-01T00%3A00%3A00Z','2014-06-30T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the first half year of 2014
        store_price_info_into_disk('USD_JPY','2014-07-01T00%3A00%3A00Z','2014-12-31T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the second half year of 2014 
        store_price_info_into_disk('USD_JPY','2015-01-01T00%3A00%3A00Z','2015-06-30T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the first half year of 2015
        store_price_info_into_disk('USD_JPY','2015-07-01T00%3A00%3A00Z','2015-12-31T23%3A59%3A59Z','H1') # dump H1 USD_JPY price for the second half year of 2015
        exit(0)
    if args.save_csv:
        convert_to_csv()
        exit(0)


    #price_list_eur_usd_h1 = store_price_into_memory(instrument="EUR_USD", granularity="H1")
    #price_gap_sort_eur_usd_h1 = sorted( price_list_eur_usd_h1, key=lambda k: k['highBid'] -k['lowBid'],reverse = True)
    #for price in price_gap_sort_eur_usd_h1[0:20]:
    #    print((price['highBid'] - price['lowBid'],price['time']))

    #price_list_usd_jpy_h1 = store_price_into_memory(instrument="USD_JPY", granularity="H1")
    #price_gap_sort_usd_jpy_h1 = sorted( price_list_usd_jpy_h1, key=lambda k: k['highBid'] -k['lowBid'],reverse = True)

    #for price in price_gap_sort_usd_jpy_h1[0:20]:
    #    print((price['highBid'] - price['lowBid'],price['time']))
    #simpleTrade1()
    #simpleTrade2()
    #simpleTrade3()

