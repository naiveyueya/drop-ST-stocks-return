# -*- coding:utf-8 -*-
#@author yueya

import pandas as pd
import numpy as np
import re
import gc
import functions as fc#import for multiprocessing, but now these code are not useful
import multiprocessing#
from functools import partial
#function blocks
def takemonth(x):#change ST data's time to 2018/1 like
    gc.collect()
    pattern = re.compile('.*/\d+/')
    m = re.search(pattern, x)
    xnew = x[0:(m.end()-1)]
    return xnew

def stTime(x,dataframe):#find in ST time and out of ST time
    tempdata = dataframe[dataframe['Stkcd']==x]
    rows = np.shape(tempdata)[0]
    stlist={'Stockid':x,'Intime':[],'Outtime':[]}
    for i in range(rows):
        calframe = tempdata.iloc[i]
        stind = calframe['Chgtype']
        if stind[0] == 'A':#if change type is A* then it become ST and worse than ST situation
            stlist['Intime'].append(calframe['Annoudt'])
        elif stind[1] == 'A':#if change type is *A then it become normal stocks
            stlist['Outtime'].append(calframe['Annoudt'])
    return stlist

def dropSTtimeReturn(dataframe,sttimelist,timelist):#let every stock is nan in ST time
    #datafram for returndata
    #sttimelist contains {stockid,In ST time,Out of ST time}
    #timelist for the whole trading month from 97/1-18/11
    for stlist in sttimelist:
        stockidtemp = stlist['Stockid']
        print(stockidtemp)
        timeend = '2018/11'
        Insttime = stlist['Intime']
        Outtime = stlist['Outtime']
        Instnum = len(Insttime)
        Outstnum = len(Outtime)
        if Instnum-Outstnum == 1:#this shows that it maybe ST in 2017/11 and ST until 2018/11
            Outtime.append(timeend)
            Instnum = len(Insttime)
            Outstnum = len(Outtime)
        elif Instnum-Outstnum > 1:#this for the stock like AB,AX,that is in the middle time those stocks maybe change their names of other situaion
            Insttime = [Insttime[0]]
            Outtime = [timeend]
            Instnum = len(Insttime)
            Outstnum = len(Outtime)
            print('the stock is ST til out of A-stock-market')
        if Instnum != Outstnum:#raise an error but not stop the code
            print('stock'+str(stockidtemp)+'numerror')
        else:
            for j in range(Instnum):
                ak = dataframe.loc[dataframe['Stkcd'] == stockidtemp]['Trdmnt']
                ui =  ak.loc[ak == Insttime[j]].index
                if len(ui) == 0:# some stocks may don't trade in ST time, the find a time that after ST time but before End ST time
                    jumpint = True
                    indlist = np.where(timelist == Insttime[j])[0]
                    while jumpint:
                        indlist += 1
                        try:
                            q = timelist.values[indlist][0][0]
                        except IndexError:
                            print('the stock is in ' + ak.iloc[-1] + ' out of A-stock-market')
                            jumpint = False
                        ui = ak.loc[ak == q].index
                        if len(ui) != 0:
                            jumpint = False
                if len(ui) == 0:
                    continue
                beginindex = ui[0]
                u = ak.loc[ak == Outtime[j]].index
                if len(u) == 0:#some stocks maybe into B-market, and in B-market out of ST, but in A-market data,we think it ST till it out of A-market
                    #eg. 2015/5 into B market,then ST time from Begin time to 2015/5
                    endindex = ak.index[-1]
                    print('the stock is in '+ak.iloc[-1]+' out of market')
                else:
                    endindex = u[0]
                dataframe.iloc[beginindex:(endindex + 1), 2] = np.nan
        gc.collect()#release memory
    return dataframe

def fillIntheBlank(returnData,blankFrame,stocksid):
    #fill data into blankdata
    #I should Use mutilprocessing, but this mission may need mutilprocess.Queue, which I haven't learn how to use it
    #this is a very stupid way to do this job.
    #so it may use a long time to run this function
    #returnData:returndata
    #blankFrame: aimming dataframe
    for i in stocksid:
        stockreturn = returnData[returnData['Stkcd']==i]
        blankFrame.loc[stockreturn.iloc[:,1],i] = list(stockreturn.iloc[:,2])
        print('stock '+str(i)+' finished')
    return 'mission finished'


####Aim: dropping ST stocks######
####first step: find in ST time and out out of ST time###
##inputting data
basedata = pd.read_csv('./data/ST.csv')
rturnsdata = pd.read_csv('./data/TD.csv')
timelist = pd.read_csv('./data/timelist.csv')
timelist['tm'] = list(map(lambda x:x[0:-3],timelist['tm'] ))
# pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
# pool.map(fc.takemonth,list(basedata['Annoudt']))
monthid = list(map(takemonth,list(basedata['Annoudt'])))#it takes long time
basedata['Annoudt'] = monthid
ststockid = np.sort(list(set(basedata['Stkcd'])))
stnew = partial(stTime,dataframe = basedata)
sttimeresult = list(map(stnew,ststockid))
####second step: let return data = np.nan, if stock is in ST time##
rturnsdata[rturnsdata['Stkcd']==ststockid[0]]
oldTrdate = rturnsdata['Trdmnt']#save old trading date as backup
newTrdate = list(map(lambda x:x[0:-2],rturnsdata['Trdmnt']))
rturnsdata['Trdmnt'] = newTrdate

dropSTtimeReturn(rturnsdata,sttimeresult,timelist)
########let the return dataframe become like this type#######
#            stock1 stock2 stock3
#     time1   r11     r21     r31
#     time2   r12     r22     r32
#     time3   r13     r23     r33
#
wholestockid = np.sort(list(set(rturnsdata['Stkcd'])))
blankdataframe = pd.DataFrame(index=timelist['tm'],columns=wholestockid)

fillIntheBlank(rturnsdata,blankdataframe,wholestockid)
blankdataframe.to_csv('./output/returnnew.csv')





# debug code, needn't to look at it
#
# i=2
# stockreturn = rturnsdata[rturnsdata['Stkcd'] == i]
# blankdataframe.loc[stockreturn.iloc[:, 1], i] = list(stockreturn.iloc[:, 2])
# print('stock ' + str(i) + ' finished')

# stockidtemp = sttimeresult[7]['Stockid']
# Insttime =  sttimeresult[7]['Intime']
# Outtime = sttimeresult[7]['Outtime']
# print(stockidtemp)
# timeend = '2018/11'
# Instnum = len(Insttime)
# Outstnum = len(Outtime)
# if Instnum - Outstnum == 1:
#     print('1')
#     Outtime.append(timeend)
#     Instnum = len(Insttime)
#     Outstnum = len(Outtime)
# elif Instnum - Outstnum > 1:
#     print('2')
#     Insttime = [Insttime[0]]
#     Outtime = [timeend]
#     Instnum = len(Insttime)
#     Outstnum = len(Outtime)
# if Instnum != Outstnum:
#     print('stock' + str(stockidtemp) + 'numerror')
#
# j=0
# ak = rturnsdata.loc[rturnsdata['Stkcd'] == stockidtemp]['Trdmnt']
# beginindex = ak.loc[ak == Insttime[j]].index[0]
# endindex = ak.loc[ak == Outstnum[j]].index[0]
#
# indlist = 2
# indlist +=1
# type(timelist.loc[indlist].values[0])
# indlist = np.where(timelist == '2005/6')[0]
# jumpint = True
# while jumpint:
#     indlist += 1
#     q = timelist.values[indlist][0][0]
#     ui = ak.loc[ak == q].index
#     if len(ui) != 0:
#         jumpint = False

# new = pd.read_csv('./data/BBB.csv')
# new.set_index('Unnamed: 0',inplace=True)
# new.loc['1997/1/31','2']=5