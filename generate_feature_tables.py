from __future__ import print_function
import logging

log = logging.getLogger(__name__)
print = log.info
logging.getLogger("requests").setLevel(logging.WARNING)
logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(levelname)s] %(message)s', datefmt = '%Y-%m-%d %H:%M:%S')


def generate_feature_table(currency_pair,input_csv,output_csv):
    """
    Generate label/features from 1h forex data
    """
    #input_csv format
    #year,month,day,hour,minute,second,openBid,openAsk,highBid,highAsk,lowBid,lowAsk,closeBid,closeAsk,volume,complete

    #Dump all data into memory 
    with:       

if __name__ == '__main__':
    pass

