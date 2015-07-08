from __future__ import print_function
import logging
import datetime

log = logging.getLogger(__name__)
print = log.info
logging.getLogger("requests").setLevel(logging.WARNING)
logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(levelname)s] %(message)s', datefmt = '%Y-%m-%d %H:%M:%S')

def calculate_Momentum_roc(day_price_info,date_str,n_day):
    momentum =0.0
    roc =0.0
    current_date = datetime.datetime.strptime(date_str,'%Y-%m-%d')
    cal_date = current_date -datetime.timedelta(days= n_day)
    print(cal_date)
    cal_date_str = datetime.date.strftime(cal_date,'%Y-%m-%d')
    print(cal_date_str)
    if day_price_info.has_key(cal_date_str): #only calculate when there is history data
        momentum = day_price_info[date_str][4] - day_price_info[cal_date_str][4]  # day_close - n_day_close
        roc = (day_price_info[date_str][4] - day_price_info[cal_date_str][4]) /day_price_info[cal_date_str][4] #(day_close - n_day_close)/ n_day_close
    print(momentum)
    print(roc)
    return momentum,roc

def generate_day_price_info(currency_pair,input_csv,output_csv):
    """
    Generate label/features(only day price part) from 1h forex data
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
    
    #generate price info to day base
    with open(output_csv,'w') as f:
        day_price_info = {}
        for key in sorted(input_price_h1.keys()):
            data = [0] *8
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
            f.write(str(data).strip('[]')+ '\n')
            #print(data[5])
            #print(data[6])
            #print(data[7])
            #break


def cal_day_price_info():
    '''
      Read all day price info and calculate 
    '''
    #Calculate Momentum and ROC 
    #3-day info
    
    momrntum_roc_3day = calculate_Momentum_roc(day_price_info,'2012-01-04',3)
    
if __name__ == '__main__':
    generate_day_price_info(currency_pair= 'EUR_USD',input_csv="price_EUR_USD_2012-01-01T00%3A00%3A00Z_H1.csv",output_csv="price_EUR_USD_2012-01-01_D1.csv")
    generate_day_price_info(currency_pair= 'EUR_USD',input_csv="price_EUR_USD_2012-07-01T00%3A00%3A00Z_H1.csv",output_csv="price_EUR_USD_2012-07-01_D1.csv")
    generate_day_price_info(currency_pair= 'EUR_USD',input_csv="price_EUR_USD_2013-01-01T00%3A00%3A00Z_H1.csv",output_csv="price_EUR_USD_2013-01-01_D1.csv")
    generate_day_price_info(currency_pair= 'EUR_USD',input_csv="price_EUR_USD_2013-07-01T00%3A00%3A00Z_H1.csv",output_csv="price_EUR_USD_2013-07-01_D1.csv")
    generate_day_price_info(currency_pair= 'EUR_USD',input_csv="price_EUR_USD_2014-01-01T00%3A00%3A00Z_H1.csv",output_csv="price_EUR_USD_2014-01-01_D1.csv")
    generate_day_price_info(currency_pair= 'EUR_USD',input_csv="price_EUR_USD_2014-07-01T00%3A00%3A00Z_H1.csv",output_csv="price_EUR_USD_2014-07-01_D1.csv")
    generate_day_price_info(currency_pair= 'EUR_USD',input_csv="price_EUR_USD_2015-01-01T00%3A00%3A00Z_H1.csv",output_csv="price_EUR_USD_2015-01-01_D1.csv")

