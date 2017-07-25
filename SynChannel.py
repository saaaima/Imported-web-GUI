import pandas as pd
import time
import numpy as np
from collections import Counter
import os
import datetime
import calendar
import pytz


time_format="%Y-%m-%d %H:%M:%S"


#read the file
#############################################################################
df=pd.read_csv('/Users/masai/Downloads/SynChannel.csv')
df['clean channel']=df['tv channel name'].apply(lambda x: x.rsplit(' ',1)[0])
df['utc hour']=df['airing time utc'].apply(lambda x: time.strptime(x,time_format)[3])
name_lst=df['clean channel'].unique().tolist()

#############################################################################
def syn(name):
    lst_syn=list()
    lst_offset=list()
    east=df[(df['clean channel']==name) & (df['tv channel name'].str.contains('EAST'))]
    pacific=df[(df['clean channel']==name) & (df['tv channel name'].str.contains('PACIFIC'))]
    east=east.sort_values(['airing time utc'])
    pacific=pacific.sort_values(['airing time utc'])


    if len(east)==len(pacific):
        result=np.array(pacific['utc hour'])-np.array(east['utc hour'])
        count=Counter(result)
        if 3 in count.keys() and 0 in count.keys():
            if count[3]>count[0]:
                lst_offset.append(name)
            else:
                lst_syn.append(name)
        elif 3 in count.keys() and 0 not in count.keys():
            lst_offset.append(name)
        elif 3 not in count.keys() and 0 in count.keys():
            lst_syn.append(name)
    elif len(east)>len(pacific):
         pacific_array=np.lib.pad(np.array(pacific['utc hour']),(0,len(east)-len(pacific)),'constant',constant_values=50)
         result=np.array(east['utc hour'])-pacific_array
         count=Counter(result)
         if 3 in count.keys() and 0 in count.keys():
             if count[3]>count[0]:
                lst_offset.append(name)
             else:
                lst_syn.append(name)
         elif 3 in count.keys() and 0 not in count.keys():
             lst_offset.append(name)
         elif 3 not in count.keys() and 0 in count.keys():
             lst_syn.append(name)
    elif len(east)<len(pacific):
         east_array=np.lib.pad(np.array(east['utc hour']),(0,len(pacific)-len(east)),'constant',constant_values=50)
         result=np.array(pacific['utc hour'])-east_array
         count=Counter(result)
         if 3 in count.keys() and 0 in count.keys():
             if count[3]>count[0]:
                 lst_offset.append(name)
             else:
                 lst_syn.append(name)
         elif 3 in count.keys() and 0 not in count.keys():
             lst_offset.append(name)
         elif 3 not in count.keys() and 0 in count.keys():
             lst_syn.append(name)
    return lst_syn,lst_offset  
############################################################################        

result=map(syn,name_lst)        
syn_channel=[i[0][0] for i in result if i[0]!=[]]
offset_channel=[i[1][0] for i in result if i[1]!=[]]


def utc_to_eastern(x):
    utc = pytz.timezone('UTC')
    easternTime = pytz.timezone('US/Eastern')
    date = datetime.datetime.strptime(x, time_format)
    date_utc = utc.localize(date)
    date_eastern = date_utc.astimezone(easternTime)
    new_dt=time.strftime(time_format,date_eastern.timetuple())
    return new_dt
  

def utc_to_pacific(x):
    utc = pytz.timezone('UTC')
    
    pacificTime=pytz.timezone('US/Pacific')
    
    date = datetime.datetime.strptime(x, time_format)
    dateutc = utc.localize(date)
    datepacific = dateutc.astimezone(pacificTime)
    new_dt=time.strftime(time_format,datepacific.timetuple())
    return new_dt






df.loc[df['clean channel'].isin(syn_channel),'airing time local']=df['airing time utc'].apply(lambda x: utc_to_eastern(x)) 

df.loc[(df['clean channel'].isin(offset_channel)) & (df['tv channel name'].str.contains('PACIFIC')),'airing time local']\
        =df['airing time utc'].apply(lambda x: utc_to_pacific(x))
   
df.loc[(df['clean channel'].isin(offset_channel)) & (df['tv channel name'].str.contains('EAST')),'airing time local']\
        =df['airing time utc'].apply(lambda x: utc_to_eastern(x))










 
      
df.to_csv('/Users/masai/Downloads/clean.csv',index=False)














