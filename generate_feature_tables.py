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
    #output_csv format
    #date,currency_pair,next_day_prediction_action,day_open,day_close,day_high,day_low,day_avg,

    #Dump all data into memory on day base 
    input_price_h1 = {}
    with open(input_csv,'r') as f:
        lines = f.readlines()
        for line in lines:
            m_list = line.split(',')
            if m_list[0] == 'year': #CSV header
                continue
            key = m_list[0] +'-' + m_list[1] + '-' + m_list[2]
            if input_price_h1.has_key(key):#append data into the same date
                input_price_h1[key].append(m_list[6:15])
            else:
                input_price_h1[key] = []
                input_price_h1[key].append(m_list[6:15])
    
        print(len(input_price_h1))
        print(input_price_h1['2012-01-01'])
    #generate price info to day base
    day_price_info = {}
    for key in sorted(input_price_h1.keys()):
        data = [0] *10
        data[0] = key #date
        data[1] = currency_pair # currency_pair
        data[2] = 'hold' #next_day_prediction_action
        data[3] = (float(input_price_h1[key][0][0]) + float(input_price_h1[key][0][1])) /2.0 # day_open (Openbid + OpenAsk )/2
        data[4] = (float(input_price_h1[key][-1][6]) + float(input_price_h1[key][-1][7])) /2.0 # day_close (closebid + closeAsk )/2
        #day_high,day_low,day_avg
        day_high = (float(input_price_h1[key][0][2]) + float(input_price_h1[key][0][3])) / 2.0 #use the first highBid/highAsk as initial value
        day_low = (float(input_price_h1[key][0][4]) + float(input_price_h1[key][0][5])) / 2.0 #use the first lowBid/lowAsk as initial value
        day_avg =0.0
        day_volume_total =0.0
        day_price_total =0.0
        for h1_price in input_price_h1[key]:
            if day_high < (float(h1_price[2]) + float(h1_price[3])) /2.0:
                day_high = (float(h1_price[2]) + float(h1_price[3])) /2.0
            if day_low > (float(h1_price[4]) + float(h1_price[5])) /2.0:
                day_low = (float(h1_price[4]) + float(h1_price[5])) /2.0
            day_price_total += ((float(h1_price[6]) +float(h1_price[7]) ) /2.0) * float(h1_price[8])
            day_volume_total += float(h1_price[8])
        day_avg = day_price_total / day_volume_total
        #print(day_volume_total)
        data[5] = day_high
        data[6] = day_low
        data[7] = day_avg
        day_price_info[key] = data
        #print(data[5])
        #print(data[6])
        #print(data[7])
        #break
    print(len(day_price_info))
    print(day_price_info['2012-01-01'])


if __name__ == '__main__':
    generate_feature_table(currency_pair= 'EUR_USD',input_csv="price_EUR_USD_2012-01-01T00%3A00%3A00Z_H1.csv",output_csv="price_EUR_USD_2012-01-01_D1.csv")

