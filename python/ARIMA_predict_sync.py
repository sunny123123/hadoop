# -*- coding: utf-8 -*-
"""
Created on Sat Jun  4 17:55:52 2016

@author: panzha
"""

import numpy as np
import pylab as pl
from scipy.optimize import leastsq
import pandas as pd
import statsmodels.api as sm
from numpy import genfromtxt
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import acf, pacf
from statsmodels.tsa.stattools import adfuller
#tmp = np.loadtxt("part-r-00000.csv", dtype=np.str, delimiter=",")
#
#data = tmp[1:,1:].astype(np.str)
#
#print(data)


#with open("part-r-00000.csv") as f:
#    for line in f:
#        print (line),

#f = open("part-r-00000.csv");
#str = f.readline();
#print(str.split(','))
#print (f)
day = 183
origin_data_file = 'part-r-00000_singer'
output_file = 'p2_mars_tianchi_artist_plays_predict0612_singer.csv'
start_index = 1
row_total = 22603
error_log = []
start_row = 0
end_row = 100
def readCsv():
   
    #df = genfromtxt('part-r-00000.csv', delimiter=',',header=None)
    df = pd.read_csv(origin_data_file,sep=',',header=None)
    #print(df.head());
    #print(df[0:1])
    #x = range(0,50);
    data = pd.DataFrame(data=df)
    #data.iloc[0:1,0:] # the 1st row
    #data.iloc[0:,0:1] # the 1st col
    #print(data)
    return data
    
def test_stationarity(timeseries):
    
    #Determing rolling statistics
    rolmean = pd.rolling_mean(timeseries, window=12)
    rolstd = pd.rolling_std(timeseries, window=12)

    #Plot rolling statistics:
    orig = plt.plot(timeseries, color='blue',label='Original')
    mean = plt.plot(rolmean, color='red', label='Rolling Mean')
    std = plt.plot(rolstd, color='black', label = 'Rolling Std')
    plt.legend(loc='best')
    plt.title('Rolling Mean & Standard Deviation')
    plt.show(block=False)
    
    #Perform Dickey-Fuller test:
    print ('Results of Dickey-Fuller Test:')
    dftest = adfuller(timeseries, autolag='AIC')
    dfoutput = pd.Series(dftest[0:4], index=['Test Statistic','p-value','#Lags Used','Number of Observations Used'])
    for key,value in dftest[4].items():
        dfoutput['Critical Value (%s)'%key] = value
    print(dfoutput)
    
def plotACF(timeSeries):
    lag_acf = acf(timeSeries, nlags=20)
    plt.subplot(121) 
    plt.plot(lag_acf)
    print(lag_acf)
    plt.axhline(y=0,linestyle='--',color='gray')
    plt.axhline(y=-1.96/np.sqrt(len(timeSeries)),linestyle='--',color='gray')
    plt.axhline(y=1.96/np.sqrt(len(timeSeries)),linestyle='--',color='gray')
    plt.title('Autocorrelation Function')
    
def plotPACF(timeSeries):
    lag_pacf = pacf(timeSeries, nlags=20, method='ols')
    plt.subplot(122)
    plt.plot(lag_pacf)
    plt.axhline(y=0,linestyle='--',color='gray')
    plt.axhline(y=-1.96/np.sqrt(len(timeSeries)),linestyle='--',color='gray')
    plt.axhline(y=1.96/np.sqrt(len(timeSeries)),linestyle='--',color='gray')
    plt.title('Partial Autocorrelation Function')
    plt.tight_layout()
'''
data DataFrame
k  whick row you want to get ,k = 0,1,2......
'''
def getlist(data,k):
    list = []
    d = data.values
    l = len(data.columns)
    i = start_index;
    while i<l:
        list.append(d[k,i]+0.1)
        #list.append(d[k,i])
        i=i+1
    return d[k,0],list
'''
row 1,2,3
'''
def plotByRow(row):
    plt.figure(1)
    global day
    x = range(1,day+1)
    global data
    singer,playNums = getlist(data,row-1)
    plt.plot(x,playNums)
    title = 'Plot of '+str(singer)
    pl.title(title)
'''
mode woth aci
'''
def forcestByAci(dta):
    dta=pd.Series(dta)
    dta.index = pd.Index(sm.tsa.datetools.dates_from_range('2001','2183'))
    
    dta_diff= dta.diff(1)
    #print(dta)
    res  = sm.tsa.arma_order_select_ic(dta_diff[1:], ic=['aic', 'bic'], trend='nc')
    aic = res.aic_min_order
    bic = res.bic_min_order;
    model = sm.tsa.ARIMA(dta,order=(aic[0],1,aic[1]))
    
    results_AR = model.fit(disp=-1)
        
    
    #fig, ax = plt.subplots(figsize=(12, 8))
    #results_AR.fittedvalues.ix['2001':].plot(ax=ax)#plot
    
    predict_rs = results_AR.predict('2184','2243')
    
    #predict_rs.ix['2183':].plot(ax=ax)#plot

    return predict_rs
'''
model with bci
'''
def forcestByBci(dta):
    dta=pd.Series(dta)
    dta.index = pd.Index(sm.tsa.datetools.dates_from_range('2001','2183'))
    #dta.index =  pd.date_range("3/1/2015",periods=183,freq='D')
    dta_diff= dta.diff(1)
    res  = sm.tsa.arma_order_select_ic(dta_diff[1:], ic=['aic', 'bic'], trend='nc')
    aic = res.aic_min_order
    bic = res.bic_min_order;
    model = sm.tsa.ARIMA(dta,order=(bic[0],1,bic[1]))
    
    results_AR = model.fit(disp=-1)    
    
    #fig, ax = plt.subplots(figsize=(12, 8))
    #results_AR.fittedvalues.ix['2001':].plot(ax=ax)#plot
    
    predict_rs = results_AR.predict('2184','2243') 
    #predict_rs.ix['2183':].plot(ax=ax)#plot

    return predict_rs
    
#op(); 
'''
convert time servers to string list
'''
def convert(ser):
    time_list = []
    for s in ser:
        time_list.append(s.strftime('%Y%m%d'))
    return time_list
    
#singer,da1 = getlist(data,0)
def outputrs1(data):
    f = open(output_file, 'wt')
    for x in range(start_row,end_row):
        singer,da1 = getlist(data,x)
        da_log = np.log(da1) #if da1[i]==0,error 
        #da_log = da1
        try:
            z=forcestByAci(da_log) #if mode with aci error,exec model with bci
        except (ValueError,np.linalg.linalg.LinAlgError,BaseException):
            try:
                z=forcestByBci(da_log)
            except(ValueError,np.linalg.linalg.LinAlgError,BaseException):
				print(singer+" ERROR")
				error_log.append(singer+" ERROR")
				continue
		#except(BaseException):
		#	print(singer+" baseexception")
		#	error_log.append(singer+" baseException")
        foc = np.exp(z.cumsum()+da_log[len(da_log)-1])
        #foc = z.cumsum()+da_log[len(da_log)-1]
       
        rs = ""
        try:
            for k in da1:
                rs = rs+str(int(k))+","
            for k in range(0,len(foc)):
                rs = rs+str(int(foc[k]))+","
        except(BaseException):
            print(singer +"converException")
            error_log.append(singer+"converException")
        rs = singer.strip()+","+rs[0:len(rs)-1]+"\n"
        #print(rs)
        print("row "+str(x)+" finished")
        print("\n")
        f.write(rs)
    f.flush()
    f.close()
    print("FINISH")
    
def outputrs2(data):
    f = open(output_file, 'wt')
    ser = pd.date_range("9/1/2015",periods=60,freq='D')
    timest = convert(ser)
    for x in range(0,row_total):
        singer,da1 = getlist(data,x)
        da_log = np.log(da1) #if da1[i]==0,error 
        try:
            z=forcestByAci(da_log) #if mode with aci error,exec model with bci
        except (ValueError,np.linalg.linalg.LinAlgError):
            try:
                z=forcestByBci(da_log)
            except (ValueError,np.linalg.linalg.LinAlgError):
				print(singer+" ERROR")
				error_log.append(singer+" ERROR")
				continue
        foc = np.exp(z.cumsum()+da_log[len(da_log)-1])
        #global timest;
        for k in range(0,len(foc)):
            rs = singer.strip()+","+str(int(foc[k]))+","+timest[k]+"\n"
            #print(rs)
            f.write(rs)
        
        f.flush()
        print(singer+"finish compute")
    f.close()
    print("FINISH")


def demo():
    '''
    load data from csv file
    '''
    data = readCsv()
    '''
    generate a time seres with length 60ll
    '''
    #ser = pd.date_range("9/1/2015",periods=60,freq='D')
    #timest = convert(ser)
    '''
    compute and output result to file
    '''
    outputrs1(data)
    for err in error_log:
        print(err)
def test2():
    singer,t = getlist(data,0)
    dta=pd.Series(t)
    dta.index =  pd.date_range("3/1/2015",periods=183,freq='D')
    #timest = convert(ser)
    print(dta)
def test1():
    data = readCsv()
    singer,t = getlist(data,221)
    dta=pd.Series(t)
    dta.index =  pd.date_range("3/1/2015",periods=183,freq='D')
    t_log = np.log(dta)
    t_diff = t_log.diff(1)
    #for v in t_diff:
    #   print(v)
    res  = sm.tsa.arma_order_select_ic(t_diff[1:], ic=['aic', 'bic'], trend='nc')
    #res  = sm.tsa.x13_arima_select_order(t)
    print(res)
    print(res.aic_min_order)
    print(res.bic_min_order)
    ts = forcestByAci(t_log)
    rs = np.exp(ts.cumsum()+t_log[len(t)-1])
    print(rs)
    #plotACF(t[1:])
    #plotPACF(t[1:])
    #test_stationarity(t[1:])
def test3():
    data = readCsv()
    singer,t = getlist(data,1)
    dta=pd.Series(t)
    dta.index =  pd.date_range("3/1/2015",periods=183,freq='D')
    t = np.log(dta)
    t = t.diff(1)
    test_stationarity(t[1:])
    #ts = forcestByAci(t)
    #rs = np.exp((ts.cumsum()+t[len(t)-1]))
    
    
    #print(rs)
if __name__=='__main__':
    demo()
	#test1()

#x,y = getlist(data,0)
#z = pd.Series(y)
#z.to_csv('a.csv')

#plotByRow(1)
