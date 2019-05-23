
#%%

import pandas as pd
import numpy as np
from LatencyMetrics import LatencyMetrics
from datetime import datetime,timedelta



df = pd.read_json('BrokerLatencyReport.json',orient='records', convert_dates=['MsgRecvTime','MsgSendTime','AckRecvTime','AckSendTime'])
df.set_index('MsgId',inplace=True)
df['BrokInLatency'] = (df['MsgSendTime'] - df['MsgRecvTime']) / np.timedelta64(1,'ms')
df['BrokOutLatency'] = (df['AckSendTime'] - df['AckRecvTime']) / np.timedelta64(1,'ms')
df['ExchangeLatency'] = (df['AckRecvTime'] - df['MsgSendTime']) / np.timedelta64(1,'ms')

SubmitOrders_df = df[df['MsgType'] == 'Submit']
CancelOrders_df = df[df['MsgType'] == 'Cancel']

# 1) Broker latency
brokerInSubmit = LatencyMetrics('BrokerInSubmit')

brokerInSubmit.max = SubmitOrders_df['BrokInLatency'].max() 
brokerInSubmit.min = SubmitOrders_df['BrokInLatency'].min()
brokerInSubmit.mean = SubmitOrders_df['BrokInLatency'].mean() 
brokerInSubmit.median = SubmitOrders_df['BrokInLatency'].median() 

brokerInCancel = LatencyMetrics('BrokerInCancel')

brokerInCancel.max = CancelOrders_df['BrokInLatency'].max() 
brokerInCancel.min = CancelOrders_df['BrokInLatency'].min() 
brokerInCancel.mean = CancelOrders_df['BrokInLatency'].mean() 
brokerInCancel.median = CancelOrders_df['BrokInLatency'].median() 

brokerOutSubmit = LatencyMetrics('BrokerOutSubmit')

brokerOutSubmit.max = SubmitOrders_df['BrokOutLatency'].max() 
brokerOutSubmit.min = SubmitOrders_df['BrokOutLatency'].min() 
brokerOutSubmit.mean = SubmitOrders_df['BrokOutLatency'].mean() 
brokerOutSubmit.median = SubmitOrders_df['BrokOutLatency'].median()

brokerOutCancel = LatencyMetrics('BrokerOutCancel')

brokerOutCancel.max = CancelOrders_df['BrokOutLatency'].max() 
brokerOutCancel.min = CancelOrders_df['BrokOutLatency'].min() 
brokerOutCancel.mean = CancelOrders_df['BrokOutLatency'].mean() 
brokerOutCancel.median = CancelOrders_df['BrokOutLatency'].median() 

# 2) Exchange latency
exchangeSubmit = LatencyMetrics('ExchangeSubmit')

exchangeSubmit.max = SubmitOrders_df['ExchangeLatency'].max() 
exchangeSubmit.min = SubmitOrders_df['ExchangeLatency'].min() 
exchangeSubmit.mean = SubmitOrders_df['ExchangeLatency'].mean() 
exchangeSubmit.median = SubmitOrders_df['ExchangeLatency'].median() 

exchangeCancel = LatencyMetrics('ExchangeCancel')

exchangeCancel.max = CancelOrders_df['ExchangeLatency'].max() 
exchangeCancel.min = CancelOrders_df['ExchangeLatency'].min() 
exchangeCancel.mean = CancelOrders_df['ExchangeLatency'].mean() 
exchangeCancel.median = CancelOrders_df['ExchangeLatency'].median() 

print("Broker Report")
brokerInSubmit.printResult()
brokerInCancel.printResult()
brokerOutSubmit.printResult()
brokerOutCancel.printResult()

print("Exchange Report")
exchangeSubmit.printResult()
exchangeCancel.printResult()