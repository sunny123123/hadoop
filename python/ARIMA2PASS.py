# -*- coding: utf-8 -*-
"""
Created on Mon Jun 13 12:09:40 2016

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

day = 183
origin_data_file = 'p2_mars_tianchi_artist_plays_predict0612_song.csv'
output_file = 'rspass.csv'
start_index = 2
row_total = 22603
error_log = []
start_row = 0
end_row = 100

def readCsv():
   
    df = pd.read_csv(origin_data_file,sep=',',header=None)
    data = pd.DataFrame(data=df)
    return data
    
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
    
def forcestByAci(dta,di=1,aic=None,bic=None):
    dta=pd.Series(dta)
    dta.index = pd.Index(sm.tsa.datetools.dates_from_range('2001','2183'))
    
    dta_diff= dta.diff(di)
    #print(dta)
    if aic is None and bic is None:
        res  = sm.tsa.arma_order_select_ic(dta_diff[di:], ic=['aic', 'bic'], trend='nc')
        aic = res.aic_min_order
        bic = res.bic_min_order
    model = sm.tsa.ARIMA(dta,order=(aic[0],di,aic[1]))
    
    results_AR = model.fit(disp=-1)
        
    
    #fig, ax = plt.subplots(figsize=(12, 8))
    #results_AR.fittedvalues.ix['2001':].plot(ax=ax)#plot
    
    predict_rs = results_AR.predict('2184','2243')
    
    #predict_rs.ix['2184':].plot(ax=ax)#plot

    return predict_rs
    
def singleDeal_1():
    data = readCsv()
    f = open(output_file, 'wt')
    f_err = open("error_log","rt")
    for line in f_err:
        ls = line.strip().replace("\t","").split(" ")
        #print(getlist(data,ls[3]))
        x = ls[3] #  ls[3] is row
        singer,da1 = getlist(data,x)
        da_log = np.log(da1)
        z=forcestByAci(da_log,di=1,aic=(0,0),bic=(0,0))
        #z=forcestByBci(da_log)
        foc = np.exp(z.cumsum()+da_log[len(da_log)-1])
            #global timest;
        rs = ""
        #print(foc)
        for k in da1:
            rs = rs+str(int(k))+","
        for k in range(0,len(foc)):
            rs = rs+str(int(foc[k]))+","
            
        rs = singer.strip()+","+rs[0:len(rs)-1]+"\n"
        print("row "+str(x)+" finished")
        #print(rs)
        f.write(rs)
    f.flush()
    f.close()
    f_err.close()

def singleDeal_2(x):
    data = readCsv()
    #f = open(output_file, 'wt')
    ser = pd.date_range("9/1/2015",periods=60,freq='D')
    timest = convert(ser)
    singer,da1 = getlist(data,x)
    da_log = np.log(da1)
    z=forcestByAci(da_log,di=1,aic=(0,0),bic=(0,0))
    #z=forcestByBci(da_log)
    foc = np.exp(z.cumsum()+da_log[len(da_log)-1])
        #global timest;
    for k in range(0,len(foc)):
        rs = singer.strip()+","+str(int(foc[k]))+","+timest[k]+"\n"
        #print(rs)
        f.write(rs)
    
    
    print(singer+"finish compute")
    f.flush()
    f.close()
def test():
     data = readCsv()
     singleDeal_1(20998)
def demo():
    data = readCsv()
    f = open("error_log","rt")
    f = open(output_file, 'wt')
    for line in f:
        ls = line.strip().replace("\t","").split(" ")
        print(getlist(data,ls[3]))
if __name__=='__main__':
   singleDeal_1()
    
    