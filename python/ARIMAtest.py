# -*- coding: utf-8 -*-
"""
Created on Sat Jun  4 17:39:57 2016

@author: panzha
"""
from __future__ import print_function
import pandas as pd
import numpy as np
from scipy import  stats
import matplotlib.pyplot as plt
import statsmodels.api as sm
from statsmodels.graphics.api import qqplot
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import acf, pacf
#import airpass
#dta=[10930,10318,10595,10972,7706,6756,9092,10551,9722,10913,11151,8186,6422, 
#6337,11649,11652,10310,12043,7937,6476,9662,9570,9981,9331,9449,6773,6304,9355, 
#10477,10148,10395,11261,8713,7299,10424,10795,11069,11602,11427,9095,7707,10767, 
#12136,12812,12006,12528,10329,7818,11719,11683,12603,11495,13670,11337,10232, 
#13261,13230,15535,16837,19598,14823,11622,19391,18177,19994,14723,15694,13248, 
#9543,12872,13101,15053,12619,13749,10228,9725,14729,12518,14564,15085,14722, 
#11999,9390,13481,14795,15845,15271,14686,11054,10395]
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
    lag_acf = acf(timeSeries, nlags=40)
    plt.subplot(121) 
    plt.plot(lag_acf)
    plt.axhline(y=0,linestyle='--',color='gray')
    plt.axhline(y=-1.96/np.sqrt(len(timeSeries)),linestyle='--',color='gray')
    plt.axhline(y=1.96/np.sqrt(len(timeSeries)),linestyle='--',color='gray')
    plt.title('Autocorrelation Function')
    
def plotPACF(timeSeries):
    lag_pacf = pacf(timeSeries, nlags=40, method='ols')
    plt.subplot(122)
    plt.plot(lag_pacf)
    plt.axhline(y=0,linestyle='--',color='gray')
    plt.axhline(y=-1.96/np.sqrt(len(timeSeries)),linestyle='--',color='gray')
    plt.axhline(y=1.96/np.sqrt(len(timeSeries)),linestyle='--',color='gray')
    plt.title('Partial Autocorrelation Function')
    plt.tight_layout()
    plt.show()  
dta =[508,508,535,506,464,473,652,548,464,539,647,834,820,630,
       867,682,682,467,566,496,760,930,797,707,962,766,937,724,
       870,956,851,870,1154,952,777,811,897,950,966,1057,1037,
       1048,1169,816,828,775,1003,944,914,1024,861,1028,980,822,880,
       670,732,668,661,666,642,537,529,581,584,529,639,606,573,507,479,
       478,602,575,807,761,668,782,550,533,598,507,656,480,476,500,630,
       406,521,506,513,507,540,483,500,518,491,408,446,506,589,455,541,
       447,434,437,471,361,519,464,432,430,556,424,456,544,494,501,449,
       444,441,473,488,618,423,421,466,477,477,464,465,395,467,446,468,
       538,473,501,600,393,380,364,333,409,471,407,364,416,434,455,448,
       692,592,598,461,555,743,481,683,570,465,546,503,530,442,452,566,
       411,444,449,493,582,581,512,503,470,525,599,385,493,466,463,520]


dta=pd.Series(dta)
dta.index = pd.Index(sm.tsa.datetools.dates_from_range('2001','2183'))
#dta.plot(figsize=(12,8))
#
#fig = plt.figure(figsize=(12,8))
#ax1= fig.add_subplot(111)
#diff1 = dta.diff(1)
#diff1.plot(ax=ax1)
#
#fig = plt.figure(figsize=(12,8))
#ax2= fig.add_subplot(111)
#diff2 = dta.diff(2)
#diff2.plot(ax=ax2)
#dta = np.log(dta)
dta_diff= dta.diff(1)#我们已经知道要使用一阶差分的时间序列，之前判断差分的程序可以注释掉

#test_stationarity(dta_diff[1:])
#print(dta)

#fig = plt.figure(figsize=(12,8))
#ax1=fig.add_subplot(312)
#fig = sm.graphics.tsa.plot_acf(dta_diff,lags=40,ax=ax1)
#ax2 = fig.add_subplot(313)
#fig = sm.graphics.tsa.plot_pacf(dta_diff,lags=40,ax=ax2)
#

#plotACF(dta_diff[1:])

#plotPACF(dta_diff[1:])
fig, ax = plt.subplots(figsize=(12, 8))

dta_diff.ix['2001':].plot(ax=ax)#plot

model = sm.tsa.ARIMA(dta,order=(7,1,1))
results_AR = model.fit(disp=-1)

results_AR.fittedvalues.ix['2001':].plot(ax=ax)#plot

predict_rs = results_AR.predict('2184','2243')

predict_rs.ix['2184':].plot(ax=ax)#plot

print(predict_rs)

plt.show()













