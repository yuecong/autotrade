# -*- coding: utf-8 -*-
from __future__ import print_function
import logging
import datetime

log = logging.getLogger(__name__)
print = log.info
logging.getLogger("requests").setLevel(logging.WARNING)
logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(levelname)s] %(message)s', datefmt = '%Y-%m-%d %H:%M:%S')
MINMUM_PIP =5
AVOID_ZERO_DIVISION = 0.0000001

#year,month,day,hour,minute,second,openBid,openAsk,highBid,highAsk,lowBid,lowAsk,closeBid,closeAsk,volume,complete
#define column number in the array of source csv
I_YEAR = 0
I_MONTH = 1
I_DAY = 2
I_OPENBID = 6
I_OPENASK = 7
I_HIGHBID = 8
I_HIGHASK = 9
I_LOWBID = 10
I_LOWASK = 11
I_CLOSEBID = 12
I_CLOSEASK = 13
I_VOLUME = 14

#define column number in the arrary for generated label/features table
N_DATE = 0
N_CURRENCY_PAIR = 1
N_PREDICTION_ACTION = 2
N_DAY_OPEN = 3
N_DAY_CLOSE = 4
N_DAY_HIGH = 5
N_DAY_LOW = 6
N_DAY_AVG = 7
N_MOMENTUM_3 = 8
N_MOMENTUM_4 = 9
N_MOMENTUM_5 = 10
N_MOMENTUM_8 = 11
N_MOMENTUM_9 = 12
N_MOMENTUM_10 = 13
N_ROC_3 = 14
N_ROC_4 = 15
N_ROC_5 = 16
N_ROC_8 = 17
N_ROC_9 = 18
N_ROC_10 = 19
N_FAST_K_3 = 20
N_FAST_D_3 = 21
N_FAST_K_4 = 22
N_FAST_D_4 = 23
N_FAST_K_5 = 24
N_FAST_D_5 = 25
N_FAST_K_8 = 26
N_FAST_D_8 = 27
N_FAST_K_9 = 28
N_FAST_D_9 = 29
N_FAST_K_10 = 30
N_FAST_D_10 = 31
N_PROC_12 = 32
N_PROC_13 = 33
N_PROC_14 = 34
N_PROC_15 = 35
N_WEIGHTED_CLOSE_PRICE = 36
N_WILLIAM_A_D = 37
N_ADOSC_1 = 38
N_ADOSC_2 = 39
N_ADOSC_3 = 40
N_ADOSC_4 = 41
N_ADOSC_5 = 42



N_PIP = 91
N_VOLUME = 92


MAXIMUM_COLUMN = 100

def calculate_adosc(day_price_info,date_str,n_day):
    adosc = 0
    current_date = datetime.datetime.strptime(date_str,'%Y-%m-%d')
    cal_date = current_date - datetime.timedelta(days= n_day)
    cal_date_str = datetime.date.strftime(cal_date,'%Y-%m-%d')
    if day_price_info.has_key(cal_date_str): #only calculate when there is history data
        price = day_price_info[cal_date_str]
        day_close = float(price[N_DAY_CLOSE])
        day_high = float(price[N_DAY_HIGH])
        day_low  = float(price[N_DAY_LOW])
        volume = float(price[N_VOLUME])
        adosc = int(( (day_close - day_low) - ( day_high - day_close) ) / (day_high - day_low + AVOID_ZERO_DIVISION) * volume)
    return adosc

def calculate_day_a_d(day_price_info,date_str):
    day_a_d = 0.0
    price = day_price_info[date_str]
    current_date = datetime.datetime.strptime(date_str,'%Y-%m-%d')
    cal_date = current_date - datetime.timedelta(days= 1)
    cal_date_str = datetime.date.strftime(cal_date,'%Y-%m-%d')
    if day_price_info.has_key(cal_date_str): #only calculate when there is history data
        price_yesterday = day_price_info[cal_date_str]
        true_high = max(float(price[N_DAY_HIGH]), float(price_yesterday[N_DAY_HIGH]))
        true_low =  min(float(price[N_DAY_LOW]), float(price_yesterday[N_DAY_LOW]))
        today_close = float(price[N_DAY_CLOSE])
        yesterday_close = float(price_yesterday[N_DAY_CLOSE])
        if today_close > yesterday_close :
            day_a_d = today_close - true_high
        elif today_close < yesterday_close :
            day_a_d = today_close - true_low
        elif today_close == yesterday_close :
            day_a_d = 0.0
    return day_a_d

def calculate_william_a_d(day_price_info,date_str):
    current_date = datetime.datetime.strptime(date_str,'%Y-%m-%d')
    yesterday_date = current_date - datetime.timedelta(days= 1)
    yesterday_date_str = datetime.date.strftime(yesterday_date,'%Y-%m-%d')
    if day_price_info.has_key(yesterday_date_str): #only calculate when there is history data
        william_a_d = calculate_day_a_d(day_price_info,date_str) + calculate_day_a_d(day_price_info,yesterday_date_str)
    else:
        william_a_d = calculate_day_a_d(day_price_info,date_str)
    return william_a_d

def calculate_proc(day_price_info,date_str,n_day):
    proc = 0.0
    price = day_price_info[date_str]
    current_date = datetime.datetime.strptime(date_str,'%Y-%m-%d')
    cal_date = current_date - datetime.timedelta(days= n_day)
    cal_date_str = datetime.date.strftime(cal_date,'%Y-%m-%d')
    if day_price_info.has_key(cal_date_str): #only calculate when there is history data
        price_n_day = day_price_info[cal_date_str]
        proc = (float(price[N_DAY_CLOSE]) - float(price_n_day[N_DAY_CLOSE])) / float(price_n_day[N_DAY_CLOSE])
    return proc
 
def calculate_fast_k_d(day_price_info,date_str,n_day):
   price = day_price_info[date_str]
   #100 * [( C - L (n) ) / ( H (n) – L (n) )] . Use the data in same day as initial value
   fast_k = 100.0 * (float(price[4]) - float(price[6])) /(float(price[5]) - float(price[6]) + AVOID_ZERO_DIVISION)   
   fast_d = fast_k
   current_date = datetime.datetime.strptime(date_str,'%Y-%m-%d')

   #calculate fast_k
   cal_date = current_date - datetime.timedelta(days= n_day)
   cal_date_str = datetime.date.strftime(cal_date,'%Y-%m-%d')
   if day_price_info.has_key(cal_date_str): #only calculate when there is history data
       price_n_day = day_price_info[cal_date_str]
       #100 * [( C – L (n) ) / ( H (n) – L (n) )]
       fast_k = 100.0 * (float(price[4]) - float(price_n_day[6])) /(float(price_n_day[5]) - float(price_n_day[6]) + AVOID_ZERO_DIVISION)
       fast_d = fast_k

   #calculate fast_d  (3-period average of fask_k) 
   cal_date_1 = current_date - datetime.timedelta(days= n_day +1) #one day before
   cal_date_str_1 = datetime.date.strftime(cal_date_1,'%Y-%m-%d')
   cal_date_2 = current_date - datetime.timedelta(days= n_day +2) # two day before
   cal_date_str_2 = datetime.date.strftime(cal_date_2,'%Y-%m-%d')
   if day_price_info.has_key(cal_date_str) and day_price_info.has_key(cal_date_str_1) and day_price_info.has_key(cal_date_str_2): #only calculate when there is history data
       price_n_day = day_price_info[cal_date_str]
       price_n_day_1 = day_price_info[cal_date_str_1]
       price_n_day_2 = day_price_info[cal_date_str_2]
       fast_d = ( (100.0 * (float(price[4]) - float(price_n_day[6])) /(float(price_n_day[5]) - float(price_n_day[6]) + AVOID_ZERO_DIVISION))
                 + (100.0 * (float(price[4]) - float(price_n_day_1[6])) /(float(price_n_day_1[5]) - float(price_n_day_1[6]) + AVOID_ZERO_DIVISION))
                 + (100.0 * (float(price[4]) - float(price_n_day_2[6])) /(float(price_n_day_2[5]) - float(price_n_day_2[6]) + AVOID_ZERO_DIVISION))
                 ) / 3.0

   return fast_k,fast_d


def calculate_pridiction_action_next_day(day_price_info,date_str,pip_unit = 10000):
    action = 'hold'
    current_date = datetime.datetime.strptime(date_str,'%Y-%m-%d')
    cal_date = current_date + datetime.timedelta(days= 1)
    cal_date_str = datetime.date.strftime(cal_date,'%Y-%m-%d')
    if day_price_info.has_key(cal_date_str): #only calculate when there is feature data
        pips = (float(day_price_info[cal_date_str][4]) - float(day_price_info[cal_date_str][3])) *pip_unit # (day_close - day_open ) for the next day
        #print(pips)
        if int(pips) > MINMUM_PIP: #Buy
            action = 'buy'
        elif int(pips) < -1 * MINMUM_PIP: #Sell
            action ='sell'
    return action

def calculate_Momentum_roc(day_price_info,date_str,n_day):
    momentum =0.0
    roc =0.0
    #print(date_str)
    current_date = datetime.datetime.strptime(date_str,'%Y-%m-%d')
    cal_date = current_date -datetime.timedelta(days= n_day)
    #print(cal_date)
    cal_date_str = datetime.date.strftime(cal_date,'%Y-%m-%d')
    #print(cal_date_str)
    if day_price_info.has_key(cal_date_str): #only calculate when there is history data
        momentum = float(day_price_info[date_str][4]) - float(day_price_info[cal_date_str][4])  # day_close - n_day_close
        roc = (float(day_price_info[date_str][4]) - float(day_price_info[cal_date_str][4])) /float(day_price_info[cal_date_str][4]) #(day_close - n_day_close)/ n_day_close
    #print(momentum)
    #print(roc)
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
            if m_list[I_YEAR] == 'year': #CSV header
                continue
            key = m_list[I_YEAR] +'-' + m_list[I_MONTH] + '-' + m_list[I_DAY]
            if input_price_h1.has_key(key):#append data into the same date
                input_price_h1[key].append(m_list[I_OPENBID:I_VOLUME +1])
            else:
                input_price_h1[key] = []
                input_price_h1[key].append(m_list[I_OPENBID:I_VOLUME +1])
    
    #generate price info to day base
    with open(output_csv,'w') as f:
        day_price_info = {}
        for key in sorted(input_price_h1.keys()):
            data = [0] *MAXIMUM_COLUMN
            data[N_DATE] = key #date
            data[N_CURRENCY_PAIR] = currency_pair # currency_pair
            data[N_PREDICTION_ACTION] = 'hold' #next_day_prediction_action
            data[N_DAY_OPEN] = (float(input_price_h1[key][0][I_OPENBID - I_OPENBID]) + float(input_price_h1[key][0][I_OPENASK - I_OPENBID])) /2.0 # day_open (Openbid + OpenAsk )/2
            data[N_DAY_CLOSE] = (float(input_price_h1[key][-1][I_CLOSEBID - I_OPENBID]) + float(input_price_h1[key][-1][I_CLOSEASK -I_OPENBID])) /2.0 # day_close (closebid + closeAsk )/2
            #day_high,day_low,day_avg
            day_high = (float(input_price_h1[key][0][I_HIGHBID - I_OPENBID]) + float(input_price_h1[key][0][ I_HIGHASK - I_OPENBID])) / 2.0 #use the first highBid/highAsk as initial value
            day_low = (float(input_price_h1[key][0][I_LOWBID - I_OPENBID]) + float(input_price_h1[key][0][ I_LOWASK - I_OPENBID])) / 2.0 #use the first lowBid/lowAsk as initial value
            day_avg =0.0
            day_volume_total =0.0
            day_price_total =0.0
            for h1_price in input_price_h1[key]:
                if day_high < (float(h1_price[I_HIGHBID - I_OPENBID]) + float(h1_price[I_HIGHASK - I_OPENBID])) /2.0:
                    day_high = (float(h1_price[I_HIGHBID - I_OPENBID]) + float(h1_price[I_HIGHASK - I_OPENBID])) /2.0
                if day_low > (float(h1_price[I_LOWBID - I_OPENBID]) + float(h1_price[I_LOWASK - I_OPENBID])) /2.0:
                    day_low = (float(h1_price[I_LOWBID - I_OPENBID]) + float(h1_price[I_LOWASK - I_OPENBID])) /2.0
                day_price_total += ((float(h1_price[I_CLOSEBID - I_OPENBID]) +float(h1_price[I_CLOSEASK - I_OPENBID]) ) /2.0) * float(h1_price[8])
                day_volume_total += float(h1_price[I_VOLUME - I_OPENBID])
            day_avg = day_price_total / day_volume_total
            data[N_DAY_HIGH] = day_high
            data[N_DAY_LOW] = day_low
            data[N_DAY_AVG] = day_avg
            data[N_VOLUME] = day_volume_total
            day_price_info[key] = data
            f.write(str(data).strip('[]')+ '\n')


def update_day_price_info(update_csv_lists,source_csv_lists,currency_pair):
    '''
      Update price info for specified csv file.
      Parameter:
        - update_csv_lists: The files need to be updated
        - source_csv_lists: The files used for information update
        - currency_pair : Use it to decide the pip unit. 100 or 10000
    '''
    #read all csv and get a integrated day_price_info
    source_day_price_info ={}
    for source_csv in source_csv_lists:
         with open(source_csv,'r') as f:
             lines = f.readlines()
             for line in lines:
                 line = line[:-2] #remove return line character
                 line =line.translate(None,"\'")
                 m_list = line.split(',')
                 source_day_price_info[m_list[N_DATE]] = m_list

    #Update price info for the files indicated by update_csv_files
    for update_csv in update_csv_lists:
         update_day_price_info = {}
         with open(update_csv,'r') as f:
             lines = f.readlines()
             for line in lines:
                 line = line[:-2] #remove return line character
                 line =line.translate(None,"\'")
                 m_list = line.split(',')
                 #m_list +=([0]*30) #expand column to add more indicators
                 key = m_list[N_DATE]

                 #Calculate Momentum and ROC 
                 #3-day info    
                 (momrntum_3day,roc_3day) = calculate_Momentum_roc(source_day_price_info,key,3)
                 m_list[N_MOMENTUM_3] =  momrntum_3day
                 m_list[N_ROC_3] = roc_3day

                 #4-day info    
                 (momrntum_4day,roc_4day) = calculate_Momentum_roc(source_day_price_info,key,4)
                 m_list[N_MOMENTUM_4] =  momrntum_4day
                 m_list[N_ROC_4] = roc_4day
    
                 #5-day info    
                 (momrntum_5day,roc_5day) = calculate_Momentum_roc(source_day_price_info,key,5)
                 m_list[N_MOMENTUM_5] =  momrntum_5day
                 m_list[N_ROC_5] = roc_5day


                 #8-day info    
                 (momrntum_8day,roc_8day) = calculate_Momentum_roc(source_day_price_info,key,8)
                 m_list[N_MOMENTUM_8] =  momrntum_8day
                 m_list[N_ROC_8] = roc_8day

                 #9-day info    
                 (momrntum_9day,roc_9day) = calculate_Momentum_roc(source_day_price_info,key,9)
                 m_list[N_MOMENTUM_9] =  momrntum_9day
                 m_list[N_ROC_9] = roc_9day


                 #10-day info    
                 (momrntum_10day,roc_10day) = calculate_Momentum_roc(source_day_price_info,key,10)
                 m_list[N_MOMENTUM_10] =  momrntum_10day
                 m_list[N_ROC_10] = roc_10day

                 #predic_action (next_day)
                 pip_unit = 10000
                 if 'JPY' in currency_pair:
                     pip_unit = 100
                 m_list[N_PREDICTION_ACTION] = calculate_pridiction_action_next_day(source_day_price_info,key,pip_unit)
                 m_list[N_PIP] = (float(m_list[N_DAY_CLOSE]) - float(m_list[N_DAY_OPEN]) ) * float(pip_unit)

                 #FAST_K & FAST_D
                 (fast_k_3day,fast_d_3day) = calculate_fast_k_d(source_day_price_info,key,3)
                 (fast_k_4day,fast_d_4day) = calculate_fast_k_d(source_day_price_info,key,4)
                 (fast_k_5day,fast_d_5day) = calculate_fast_k_d(source_day_price_info,key,5)
                 (fast_k_8day,fast_d_8day) = calculate_fast_k_d(source_day_price_info,key,8)
                 (fast_k_9day,fast_d_9day) = calculate_fast_k_d(source_day_price_info,key,9)
                 (fast_k_10day,fast_d_10day) = calculate_fast_k_d(source_day_price_info,key,10)
                 m_list[N_FAST_K_3]= fast_k_3day
                 m_list[N_FAST_D_3]= fast_d_3day
                 m_list[N_FAST_K_4]= fast_k_4day
                 m_list[N_FAST_D_4]= fast_d_4day
                 m_list[N_FAST_K_5]= fast_k_5day
                 m_list[N_FAST_D_5]= fast_d_5day
                 m_list[N_FAST_K_8]= fast_k_8day
                 m_list[N_FAST_D_8]= fast_d_8day
                 m_list[N_FAST_K_9]= fast_k_9day
                 m_list[N_FAST_D_9]= fast_d_9day
                 m_list[N_FAST_K_10]= fast_k_10day
                 m_list[N_FAST_D_10]= fast_d_10day

                 #PROC 12/13/14/15 days
                 m_list[N_PROC_12] = calculate_proc(source_day_price_info,key,12)
                 m_list[N_PROC_13] = calculate_proc(source_day_price_info,key,13)
                 m_list[N_PROC_14] = calculate_proc(source_day_price_info,key,14)
                 m_list[N_PROC_15] = calculate_proc(source_day_price_info,key,15)
                 #Weighted Close Price
                 m_list[N_WEIGHTED_CLOSE_PRICE] = (float(m_list[N_DAY_CLOSE])*2.0 + float(m_list[N_DAY_HIGH]) + float(m_list[N_DAY_LOW]))/4.0
      
                 #WILLIAM_A_D
                 m_list[N_WILLIAM_A_D] = calculate_william_a_d(source_day_price_info,key)

                 #ADOSC
                 m_list[N_ADOSC_1] = calculate_adosc(source_day_price_info,key,0)
                 m_list[N_ADOSC_2] = calculate_adosc(source_day_price_info,key,1)
                 m_list[N_ADOSC_3] = calculate_adosc(source_day_price_info,key,2)
                 m_list[N_ADOSC_4] = calculate_adosc(source_day_price_info,key,3)
                 m_list[N_ADOSC_5] = calculate_adosc(source_day_price_info,key,4)
 
                 update_day_price_info[key] = m_list

         #write updated info into the csv file
         with open(update_csv,'w') as f:
             for key in sorted(update_day_price_info.keys()):
                 f.write(str(update_day_price_info[key]).strip('[]')+ '\n')
             f.truncate()

if __name__ == '__main__':
    generate_day_price_info(currency_pair= 'EUR_USD',input_csv="price_EUR_USD_2012-01-01T00%3A00%3A00Z_H1.csv",output_csv="price_EUR_USD_2012-01-01_D1.csv")
    generate_day_price_info(currency_pair= 'EUR_USD',input_csv="price_EUR_USD_2012-07-01T00%3A00%3A00Z_H1.csv",output_csv="price_EUR_USD_2012-07-01_D1.csv")
    generate_day_price_info(currency_pair= 'EUR_USD',input_csv="price_EUR_USD_2013-01-01T00%3A00%3A00Z_H1.csv",output_csv="price_EUR_USD_2013-01-01_D1.csv")
    generate_day_price_info(currency_pair= 'EUR_USD',input_csv="price_EUR_USD_2013-07-01T00%3A00%3A00Z_H1.csv",output_csv="price_EUR_USD_2013-07-01_D1.csv")
    generate_day_price_info(currency_pair= 'EUR_USD',input_csv="price_EUR_USD_2014-01-01T00%3A00%3A00Z_H1.csv",output_csv="price_EUR_USD_2014-01-01_D1.csv")
    generate_day_price_info(currency_pair= 'EUR_USD',input_csv="price_EUR_USD_2014-07-01T00%3A00%3A00Z_H1.csv",output_csv="price_EUR_USD_2014-07-01_D1.csv")
    generate_day_price_info(currency_pair= 'EUR_USD',input_csv="price_EUR_USD_2015-01-01T00%3A00%3A00Z_H1.csv",output_csv="price_EUR_USD_2015-01-01_D1.csv")
    update_day_price_info(['price_EUR_USD_2012-01-01_D1.csv',
                          'price_EUR_USD_2012-07-01_D1.csv',
                          'price_EUR_USD_2013-01-01_D1.csv',
                          'price_EUR_USD_2013-07-01_D1.csv',
                          'price_EUR_USD_2014-01-01_D1.csv',
                          'price_EUR_USD_2014-07-01_D1.csv',
                          'price_EUR_USD_2015-01-01_D1.csv',
                          ],
                          ['price_EUR_USD_2012-01-01_D1.csv',
                          'price_EUR_USD_2012-07-01_D1.csv',
                          'price_EUR_USD_2013-01-01_D1.csv',
                          'price_EUR_USD_2013-07-01_D1.csv',
                          'price_EUR_USD_2014-01-01_D1.csv',
                          'price_EUR_USD_2014-07-01_D1.csv',
                          'price_EUR_USD_2015-01-01_D1.csv',
                          ],'EUR_USD')

  
    generate_day_price_info(currency_pair= 'USD_JPY',input_csv="price_USD_JPY_2012-01-01T00%3A00%3A00Z_H1.csv",output_csv="price_USD_JPY_2012-01-01_D1.csv")
    generate_day_price_info(currency_pair= 'USD_JPY',input_csv="price_USD_JPY_2012-07-01T00%3A00%3A00Z_H1.csv",output_csv="price_USD_JPY_2012-07-01_D1.csv")
    generate_day_price_info(currency_pair= 'USD_JPY',input_csv="price_USD_JPY_2013-01-01T00%3A00%3A00Z_H1.csv",output_csv="price_USD_JPY_2013-01-01_D1.csv")
    generate_day_price_info(currency_pair= 'USD_JPY',input_csv="price_USD_JPY_2013-07-01T00%3A00%3A00Z_H1.csv",output_csv="price_USD_JPY_2013-07-01_D1.csv")
    generate_day_price_info(currency_pair= 'USD_JPY',input_csv="price_USD_JPY_2014-01-01T00%3A00%3A00Z_H1.csv",output_csv="price_USD_JPY_2014-01-01_D1.csv")
    generate_day_price_info(currency_pair= 'USD_JPY',input_csv="price_USD_JPY_2014-07-01T00%3A00%3A00Z_H1.csv",output_csv="price_USD_JPY_2014-07-01_D1.csv")
    generate_day_price_info(currency_pair= 'USD_JPY',input_csv="price_USD_JPY_2015-01-01T00%3A00%3A00Z_H1.csv",output_csv="price_USD_JPY_2015-01-01_D1.csv")

    update_day_price_info(['price_USD_JPY_2012-01-01_D1.csv',
                          'price_USD_JPY_2012-07-01_D1.csv',
                          'price_USD_JPY_2013-01-01_D1.csv',
                          'price_USD_JPY_2013-07-01_D1.csv',
                          'price_USD_JPY_2014-01-01_D1.csv',
                          'price_USD_JPY_2014-07-01_D1.csv',
                          'price_USD_JPY_2015-01-01_D1.csv',
                          ],
                          ['price_USD_JPY_2012-01-01_D1.csv',
                          'price_USD_JPY_2012-07-01_D1.csv',
                          'price_USD_JPY_2013-01-01_D1.csv',
                          'price_USD_JPY_2013-07-01_D1.csv',
                          'price_USD_JPY_2014-01-01_D1.csv',
                          'price_USD_JPY_2014-07-01_D1.csv',
                          'price_USD_JPY_2015-01-01_D1.csv',
                          ],'USD_JPY')

