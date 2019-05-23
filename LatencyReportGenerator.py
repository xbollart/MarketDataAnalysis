

#%%

import pandas as pd
import numpy as np
from datetime import datetime,timedelta

def addLatency (row,columnName):
    return row[columnName] + timedelta(milliseconds=np.random.randint(1,maxLatency)) 


df = pd.DataFrame()
recordSize = 1000
minUnixTime =  1514782800000 # 2018-01-01
maxUnixTime = 1546318799000 # 2018-12-31
maxLatency = 1000 # 1000 miliseconds

#Generate latencies
df['MsgType'] = ["Submit" if x == 0 else "Cancel" for x in np.random.randint(0, 2, recordSize) ] 
df['MsgRecvTime'] = [datetime.fromtimestamp(x/1000) for x in np.random.randint(minUnixTime,maxUnixTime, recordSize)]
df['MsgSendTime'] = df.apply (lambda row: addLatency(row,'MsgRecvTime'), axis=1)
df['AckRecvTime'] = df.apply (lambda row: addLatency(row,'MsgSendTime'), axis=1)
df['AckSendTime'] = df.apply (lambda row: addLatency(row,'AckRecvTime'), axis=1)

#format columns
df['MsgRecvTime'] = df['MsgRecvTime'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
df['MsgSendTime'] = df['MsgSendTime'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
df['AckRecvTime'] = df['AckRecvTime'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
df['AckSendTime'] = df['AckSendTime'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')

df['MsgId'] = df.index
df[['MsgId','MsgType','MsgRecvTime','MsgSendTime','AckRecvTime','AckSendTime']].to_json('BrokerLatencyReport.json', orient='records')



