#!/usr/local/bin/python3
#%% [markdown]

import sys
import datetime
import pandas as pd
import numpy as np

if(len(sys.argv)< 3):
    towerFile = 'tower_file.csv'
    exchangeFile = 'exchange_file.csv'
else:
    towerFile = sys.argv[1]
    exchangeFile = sys.argv[2]

#%% [markdown]
# ## Load Tower dataframe
dateparse1 = lambda x: pd.datetime.strptime(x, "%Y%m%d-%H:%M:%S")
df1 = pd.read_csv(towerFile,names=['date', 'Instrument', 'Side', 'Quantity', 'Price'],parse_dates=['date'],date_parser=dateparse1)

#%% [markdown]
# ## Load Exchange dataframe
dateparse2 = lambda x: pd.datetime.strptime(x, "%Y%m%d%H%M%S")
df2 = pd.read_csv(exchangeFile,names=['date', 'Instrument', 'Side', 'Quantity', 'Price'],parse_dates=['date'],date_parser=dateparse2)

merged = df1.merge(df2, indicator=True, how='outer')
diff_merged =  merged[merged['_merge'] != 'both']
diff_merged['missing_in'] =  ['not in tower' if x =='right_only' else 'not in exchange' for x in diff_merged['_merge']] 

#%% [markdown]
# ## Print report
print(diff_merged) 
header = ["date", "Instrument", "Side", "Quantity","Price","missing_in"]
diff_merged.to_csv('diff_report.csv',columns = header, index=False,header=False,date_format="%Y%m%d-%H:%M:%S")


