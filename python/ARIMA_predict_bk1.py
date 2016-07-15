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
def readCsv():
   
    #df = genfromtxt('part-r-00000.csv', delimiter=',',header=None)
    df = pd.read_csv('./dataset/part-r-00000.csv',sep=',',header=None)
    #print(df.head());
    #print(df[0:1])
    #x = range(0,50);
    data = pd.DataFrame(data=df)
    #data.iloc[0:1,0:] # the 1st row
    #data.iloc[0:,0:1] # the 1st col
    #print(data)
    return data

'''
data DataFrame
k  whick row you want to get ,k = 0,1,2......
'''
def getlist(data,k):
    list = []
    d = data.values
    l = len(data.columns)
    i = 2;
    while i<l:
        list.append(d[k,i])
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
    
def forcest(dta):
    dta=pd.Series(dta)
    dta.index = pd.Index(sm.tsa.datetools.dates_from_range('2001','2183'))
    dta_diff= dta.diff(1)
    
    model = sm.tsa.ARIMA(dta,order=(7,1,1))
    
    results_AR = model.fit(disp=-1)
    
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
def outputrs1():
    f = open('mars_tianchi_artist_plays_predict.csv', 'wt')
    for x in range(0,50):
        singer,da1 = getlist(data,x)
        z=forcest(da1)
        foc = z.cumsum()+da1[182]
        rs = ""
        for k in range(0,len(foc)):
            rs = rs+str(int(foc[k]))+","
        rs = singer.strip()+","+rs[0:len(rs)-1]+"\n"  
        print(rs)
        f.write(rs)
    f.flush()
    f.close()
    print("FINISH")
    
def outputrs2():
    f = open('mars_tianchi_artist_plays_predict.csv', 'wt')
    for x in range(0,50):
        singer,da1 = getlist(data,x)
        z=forcest(da1)
        foc = z+da1[182]
        global timest;
        for k in range(0,len(foc)):
            rs = singer.strip()+","+str(int(foc[k]))+","+timest[k]+"\n"
            #print(rs)
            f.write(rs)
        
        f.flush()
        print(singer+"finish compute")
    f.close()
    print("FINISH")
'''
load data from csv file
'''
data = readCsv()
'''
generate a time seres with length 60ll
'''
ser = pd.date_range("9/1/2015",periods=60,freq='D')
timest = convert(ser)
'''
compute and output result to file
'''
outputrs2()
#x,y = getlist(data,0)
#z = pd.Series(y)
#z.to_csv('a.csv')

#plotByRow(1)

