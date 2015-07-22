# -*- coding: utf-8 -*-
from __future__ import print_function
import logging
import datetime
import glob

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
N_EMA_12 =43
N_EMA_26 =44
N_MACD =45
N_CCI =46
N_BOLLINGER_BANDS_LOW = 47
N_BOLLINGER_BANDS_HIGH = 48
N_HEIKIN_ASHI_XCLOSE = 49
N_HEIKIN_ASHI_XOPEN = 50
N_HEIKIN_ASHI_XHIGH = 51
N_HEIKIN_ASHI_XLOW = 52
N_2DAY_HIGH = 53
N_2DAY_LOW =54
N_DAY_HIGH_LOW_AVG =55
N_2DAY_HIGH_LOW_AVG =56
N_DAY_HIGH_SLOPE_3 = 57
N_DAY_HIGH_SLOPE_4 = 58
N_DAY_HIGH_SLOPE_5 = 59
N_DAY_HIGH_SLOPE_8 = 60
N_DAY_HIGH_SLOPE_10= 61
N_DAY_HIGH_SLOPE_12 = 62
N_DAY_HIGH_SLOPE_15= 63
N_DAY_HIGH_SLOPE_20 = 64
N_DAY_HIGH_SLOPE_25 = 65
N_DAY_HIGH_SLOPE_30 = 66
N_PIP = 67
N_PIP_NEXT_DAY = 68
N_VOLUME = 69
MAXIMUM_COLUMN = 70
CSV_HEADER = ("Date,Currency_pair,Prediction_action,"
              "Day Open,Day Close,Day High,Day Low,Day Average,"
              "Momentum_3day,Momentum_4day,Momentum_5day,Momentum_8day,Momentum_9day,Momentum_10day,"
              "Roc_3day,Roc_4day,Roc_5day,Roc_8day,Roc_9day,Roc_10day,"
              "Fast_k_3day,Fast_d_3day,Fast_k_4day,Fast_d_4day,Fast_k_5day,Fast_d_5day,Fast_k_8day,Fast_d_8day,"
              "Fast_k_9day,Fast_d_9day,Fast_k_10day,Fast_d_10day,"
              "PROC_12day,PROC_13day,PROC_14day,PROC_15day,"
              "Weighted_Close_Price,WILLIAM_A_D,"
              "ADOSC_1day,ADOSC_2day,ADOSC_3day,ADOSC_4day,ADOSC_5day,"
              "EMA_12Day,EMA_26Day,MACD,"
              "CCI,BOLLINGER_BANDS_LOW,BOLLINGER_BANDS_HIGH,"
              "HEIKIN_ASHI_XCLOSE,HEIKIN_ASHI_XOPEN,HEIKIN_ASHI_XHIGH,HEIKIN_ASHI_XLOW,"
              "2DAY_HIGH,2DAY_LOW,1DAY_HIGH_LOW_AVG,2DAY_HIGH_LOW_AVG,"
              "High_slope_3day,High_slope_4day,High_slope_5day,High_slope_8day,High_slope_10day,"
              "High_slope_12day,High_slope_15day,High_slope_20day,High_slope_25day,High_slope_30day,"
              "Pips,Prediction_Pips,Volume"
             ) 

def calculate_high_slope(day_price_info,date_str,n_day):
    keys_sorted = sorted(day_price_info.keys())
    date_order = keys_sorted.index(date_str)
    start_date_order = max(date_order - n_day,0)
    current_price = day_price_info[date_str]
    slope_adjust_factor = 1000.0
    start_price = day_price_info[keys_sorted[start_date_order]]
    slope =( float(current_price[N_DAY_HIGH]) - float(start_price[N_DAY_HIGH]) ) / float(1+ date_order - start_date_order)*slope_adjust_factor
    return slope


def calculate_2day_high_low(day_price_info,date_str):
    price = day_price_info[date_str]
    last_price = price
    keys_sorted = sorted(day_price_info.keys())
    date_order = keys_sorted.index(date_str)
    if date_order >0:
        last_price = day_price_info[keys_sorted[date_order -1]]
    high_2day = max( float(price[N_DAY_HIGH]), float(last_price[N_DAY_HIGH]) )
    low_2day  = min (float(price[N_DAY_LOW]) , float(last_price[N_DAY_LOW]) )
    high_low_avg = ( float(price[N_DAY_HIGH]) + float(price[N_DAY_LOW]) /2.0)
    high_low_avg_2day = ( float(price[N_DAY_HIGH]) + float(price[N_DAY_LOW]) +  float(last_price[N_DAY_HIGH]) + float(last_price[N_DAY_LOW]) ) /4.0
    return high_2day, low_2day, high_low_avg, high_low_avg_2day

def calculate_heikin_ashi(day_price_info,date_str):
    price = day_price_info[date_str]
    xclose = (float(price[N_DAY_CLOSE]) + float(price[N_DAY_OPEN]) + float(price[N_DAY_HIGH]) + float(price[N_DAY_LOW]) ) /4.0
    keys_sorted = sorted(day_price_info.keys())
    date_order = keys_sorted.index(date_str)
    if date_order >0:
        last_price = day_price_info[keys_sorted[date_order -1]]
        xopen = ( float(last_price[N_DAY_OPEN]) + float(last_price[N_DAY_CLOSE])) /2.0
    else:
        xopen = ( float(price[N_DAY_OPEN]) + float(price[N_DAY_CLOSE])) /2.0
    xhigh = max( float(price[N_DAY_HIGH]), xclose, xopen)
    xlow = min( float(price[N_DAY_LOW]), xclose, xopen)
    return xclose, xopen, xhigh, xlow



def calculate_Bollinger_Bands(day_price_info,date_str):
    bb_low = float(day_price_info[date_str][N_DAY_LOW]) #Use low/high value as the initial value for Bollinger_Bands 
    bb_high = float(day_price_info[date_str][N_DAY_HIGH]) #Use low/high value as the initial value for Bollinger_Bands 
    keys_sorted = sorted(day_price_info.keys())
    date_order = keys_sorted.index(date_str)
    if date_order >= 20 -1: #At least need 20-day period to calculate cci
        #20 day simple moving average
        sma_total = 0.0 #20-day Simple moving average in total     
        for key in keys_sorted[date_order -19:date_order+1]:
            price = day_price_info[key]
            sma_total += float(price[N_DAY_CLOSE])
        sma = sma_total /20.0
        #20 day standard deviation
        std_dev_total = 0.0
        for key in keys_sorted[date_order -19:date_order+1]:
            price = day_price_info[key]
            std_dev_total += abs(sma - float(price[N_DAY_CLOSE]) )
        std_dev = std_dev_total /20.0
        bb_low = sma - std_dev*2
        bb_high = sma + std_dev*2
    return bb_low,bb_high


def calculate_cci(day_price_info,date_str):
    cci = 0.0
    keys_sorted = sorted(day_price_info.keys())
    date_order = keys_sorted.index(date_str)
    if date_order >= 20 -1: #At least need 20-day period to calculate cci
        smatp_total = 0.0 #Simple Moving Average of the Typical Price in total
        for key in keys_sorted[date_order -19:date_order+1]:
            price = day_price_info[key]
            smatp_total += (float(price[N_DAY_HIGH]) + float(price[N_DAY_LOW]) + float(price[N_DAY_CLOSE]) )/3.0        
        smatp = smatp_total /20.0
        mean_deviation_in_total = 0.0
        for key in keys_sorted[date_order -19:date_order+1]:
            price = day_price_info[key]
            mean_deviation_in_total += abs(smatp -((float(price[N_DAY_HIGH]) + float(price[N_DAY_LOW]) + float(price[N_DAY_CLOSE]) )/3.0) )
        mean_deviation = mean_deviation_in_total /20.0
        price = day_price_info[date_str]
        typical_price = (float(price[N_DAY_HIGH]) + float(price[N_DAY_LOW]) + float(price[N_DAY_CLOSE]) )/3.0 
        cci = (typical_price - smatp)/ (0.015 * mean_deviation)
    return cci

def calculate_ema(day_price_info,date_str,n_day):
    EMA_COL_NUM = N_EMA_12
    if n_day !=12 and n_day!=26:
        print("Wrong ema parameter!")
        return 0.0
    if n_day == 12:
        EMA_COL_NUM = N_EMA_12
    elif n_day == 26:
        EMA_COL_NUM = N_EMA_26

    ema = 0.0
    keys_sorted = sorted(day_price_info.keys()) 
    date_order = keys_sorted.index(date_str)

    if date_order < n_day: #For 12/26 day EMA, the first 12/26 days are just the average of close price
       n_value =0.0
       for date in keys_sorted[:date_order+1]:
           n_value += float(day_price_info[date][N_DAY_CLOSE])
       ema = n_value / (date_order+1) 
    else:
        yesterday_date = keys_sorted[date_order-1]
        ema = float(day_price_info[yesterday_date][N_DAY_CLOSE]) * (2.0/(n_day +1.0)) + float(day_price_info[yesterday_date][EMA_COL_NUM]) *(1-(2.0/(n_day+1.0)))

    return ema

def calculate_adosc(day_price_info,date_str,n_day):
    adosc = 0
    keys_sorted = sorted(day_price_info.keys())
    date_order = keys_sorted.index(date_str)
    if date_order  > n_day -1: 
        cal_date_str = keys_sorted[date_order - n_day]
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
    keys_sorted = sorted(day_price_info.keys())
    date_order = keys_sorted.index(date_str)
    if date_order  > 1 - 1:
        cal_date_str = keys_sorted[date_order - 1]
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
    keys_sorted = sorted(day_price_info.keys())
    date_order = keys_sorted.index(date_str)
    if date_order  > 1 -1:
        yesterday_date_str = keys_sorted[date_order - 1]
        william_a_d = calculate_day_a_d(day_price_info,date_str) + calculate_day_a_d(day_price_info,yesterday_date_str)
    else:
        william_a_d = calculate_day_a_d(day_price_info,date_str)
    return william_a_d

def calculate_proc(day_price_info,date_str,n_day):
    proc = 0.0
    price = day_price_info[date_str]
    keys_sorted = sorted(day_price_info.keys())
    date_order = keys_sorted.index(date_str)
    if date_order  > n_day -1:
        cal_date_str = keys_sorted[date_order - n_day]
        price_n_day = day_price_info[cal_date_str]
        proc = (float(price[N_DAY_CLOSE]) - float(price_n_day[N_DAY_CLOSE])) / float(price_n_day[N_DAY_CLOSE])
    return proc
 
def calculate_fast_k_d(day_price_info,date_str,n_day):
   price = day_price_info[date_str]
   #100 * [( C - L (n) ) / ( H (n) – L (n) )] . Use the data in same day as initial value
   fast_k = 100.0 * (float(price[4]) - float(price[6])) /(float(price[5]) - float(price[6]) + AVOID_ZERO_DIVISION)   
   fast_d = fast_k

   #calculate fast_k
   keys_sorted = sorted(day_price_info.keys())
   date_order = keys_sorted.index(date_str)
   if date_order  > n_day -1:
       cal_date_str = keys_sorted[date_order - n_day]
       price_n_day = day_price_info[cal_date_str]
       #100 * [( C – L (n) ) / ( H (n) – L (n) )]
       fast_k = 100.0 * (float(price[4]) - float(price_n_day[6])) /(float(price_n_day[5]) - float(price_n_day[6]) + AVOID_ZERO_DIVISION)
       fast_d = fast_k

   #calculate fast_d  (3-period average of fask_k) 
   if date_order  > n_day+2 -1 :
       cal_date_str = keys_sorted[date_order - n_day]
       cal_date_str_1 = keys_sorted[date_order - n_day +1]
       cal_date_str_2 = keys_sorted[date_order - n_day + 2]
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
    pips = 0.0
    keys_sorted = sorted(day_price_info.keys())
    date_order = keys_sorted.index(date_str)
    if date_order  < len(keys_sorted) -1:
        cal_date_str = keys_sorted[date_order +1]
        pips = (float(day_price_info[cal_date_str][4]) - float(day_price_info[cal_date_str][3])) *pip_unit # (day_close - day_open ) for the next day
        #print(pips)
        if int(pips) > MINMUM_PIP: #Buy
            action = 'buy'
        elif int(pips) < -1 * MINMUM_PIP: #Sell
            action ='sell'
    return pips,action

def calculate_Momentum_roc(day_price_info,date_str,n_day):
    momentum =0.0
    roc =0.0
    keys_sorted = sorted(day_price_info.keys())
    date_order = keys_sorted.index(date_str)
    if date_order  > n_day -1:
        cal_date_str = keys_sorted[date_order - n_day]
        momentum = float(day_price_info[date_str][4]) - float(day_price_info[cal_date_str][4])  # day_close - n_day_close
        roc = (float(day_price_info[date_str][4]) - float(day_price_info[cal_date_str][4])) /float(day_price_info[cal_date_str][4]) #(day_close - n_day_close)/ n_day_close
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
                 (m_list[N_PIP_NEXT_DAY],m_list[N_PREDICTION_ACTION]) = calculate_pridiction_action_next_day(source_day_price_info,key,pip_unit)
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

                 #MACD
                 m_list[N_EMA_12] = calculate_ema(source_day_price_info,key,12)
                 m_list[N_EMA_26] = calculate_ema(source_day_price_info,key,26)
                 m_list[N_MACD] = m_list[N_EMA_12] - m_list[N_EMA_26]  

                 #CCI
                 m_list[N_CCI] = calculate_cci(source_day_price_info,key)

                 #Bollinger Bands
                 (m_list[N_BOLLINGER_BANDS_LOW] , m_list[N_BOLLINGER_BANDS_HIGH]) = calculate_Bollinger_Bands(source_day_price_info,key)

                 #Heikin-Ashi
                 (m_list[N_HEIKIN_ASHI_XCLOSE],m_list[N_HEIKIN_ASHI_XOPEN],m_list[N_HEIKIN_ASHI_XHIGH],m_list[N_HEIKIN_ASHI_XLOW]) = calculate_heikin_ashi(source_day_price_info,key)

                 # 2day high, 2day low, high/low average, 2day high/low average
                 (m_list[N_2DAY_HIGH],m_list[N_2DAY_LOW], m_list[N_DAY_HIGH_LOW_AVG], m_list[N_2DAY_HIGH_LOW_AVG]) = calculate_2day_high_low(source_day_price_info,key)

                 #high price slope (3/4/5/8/10/12/15/20/25/30 days)
                 m_list[N_DAY_HIGH_SLOPE_3] = calculate_high_slope(source_day_price_info,key,3)
                 m_list[N_DAY_HIGH_SLOPE_4] = calculate_high_slope(source_day_price_info,key,4)
                 m_list[N_DAY_HIGH_SLOPE_5] = calculate_high_slope(source_day_price_info,key,5)
                 m_list[N_DAY_HIGH_SLOPE_8] = calculate_high_slope(source_day_price_info,key,8)
                 m_list[N_DAY_HIGH_SLOPE_10] = calculate_high_slope(source_day_price_info,key,10)
                 m_list[N_DAY_HIGH_SLOPE_12] = calculate_high_slope(source_day_price_info,key,12)
                 m_list[N_DAY_HIGH_SLOPE_15] = calculate_high_slope(source_day_price_info,key,15)
                 m_list[N_DAY_HIGH_SLOPE_20] = calculate_high_slope(source_day_price_info,key,20)
                 m_list[N_DAY_HIGH_SLOPE_25] = calculate_high_slope(source_day_price_info,key,25)
                 m_list[N_DAY_HIGH_SLOPE_30] = calculate_high_slope(source_day_price_info,key,30)
                 update_day_price_info[key] = m_list
                 
         #write updated info into the csv file
         with open(update_csv,'w') as f:
             for key in sorted(update_day_price_info.keys()):
                line = update_day_price_info[key]
                avail_fraction_str = '%.5f'
                if 'JPY' in line[N_CURRENCY_PAIR]: avail_fraction_str ='%.2f'
                line_str =''
                for i in range(0,len(line)): # format each element
                    each_element_str = str(line[i])
                    if i>= N_DAY_OPEN and i<=N_MOMENTUM_10: each_element_str = avail_fraction_str %float(line[i])
                    if i>=N_ROC_3 and i<=N_ROC_10: each_element_str = '%.2f' %float(line[i])
                    if i>=N_FAST_K_3 and i<=N_FAST_D_10: each_element_str = '%d' %int(line[i])
                    if i>=N_PROC_12 and i<=N_PROC_15: each_element_str = '%.2f' %float(line[i])
                    if i>=N_WEIGHTED_CLOSE_PRICE and i<=N_WILLIAM_A_D: each_element_str = avail_fraction_str %float(line[i])
                    if i>=N_ADOSC_1 and i<=N_ADOSC_5: each_element_str = '%d' %int(line[i])
                    if i>=N_EMA_12 and i<=N_MACD: each_element_str = avail_fraction_str %float(line[i])
                    if i==N_CCI : each_element_str = '%.2f' %float(line[i])
                    if i>=N_BOLLINGER_BANDS_LOW and i<=N_2DAY_HIGH_LOW_AVG: each_element_str = avail_fraction_str %float(line[i])
                    if i>=N_DAY_HIGH_SLOPE_3 and i<=N_DAY_HIGH_SLOPE_30: each_element_str = '%.4f' %float(line[i])
                    if i>=N_PIP and i<=N_PIP_NEXT_DAY: ach_element_str = '%.1f' %float(line[i])
                    if i==N_VOLUME: each_element_str = '%d' %int(float(line[i]))
                    if i <  len(line) -1 : end_str = ','
                    else: end_str = '\n'
                    each_element_str +=end_str
                    line_str +=each_element_str
                f.write(line_str)
                #print(line_str)
             f.truncate()

    
def generate_seperate_feature_tables():
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

def merge_all_feature_tables(currency_pair):
    source_file_list = sorted(glob.glob("price_%s_*_D1.csv" % currency_pair))
    meger_file_name = 'price_%s_D1_merge.csv' % currency_pair
    with open(meger_file_name,'w') as merge_f:
        merge_f.write(CSV_HEADER + '\n')
    for source_file in source_file_list:
        with open(source_file,'r') as source_f:
            lines = source_f.readlines()
            with open(meger_file_name,'a') as merge_f:
                merge_f.writelines(lines)

if __name__ == '__main__':
    generate_seperate_feature_tables()
    merge_all_feature_tables('EUR_USD')
    merge_all_feature_tables('USD_JPY')

