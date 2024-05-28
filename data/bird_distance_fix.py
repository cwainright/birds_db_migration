"""
Fix the broken referential integrity in `tbl_Field_Data.Distance_id`
The distance classes changed on the paper datasheet in 2019.

The lookup table added new values (from ["<=50M","50-100M"] to ["<=25M","26-50M","51-100M"]) but didn't enforce the change on the front-end
and the QC process seems not to have updated data on the back-end, so the deprecated codes continue to be present in the data through 2023.
E.g., Code 1 and 2 were allegedly deprecated in 2019 and replaced by codes 3, 4, and 5. Code 1 was entered as recently as 2022-07-08 and code 2 was entered as recently as 2023-07-21.
"""

import pandas as pd
import numpy as np
import datetime as dt
import data.getdf as getdf

df = getdf._get_df()

df.Distance_id.unique() # there are 5 unique distance IDs

mask = (df['Distance_id']==1)
max(df[mask].Date.unique())
min(df[mask].Date.unique())
mask = (df['Distance_id']==2)
max(df[mask].Date.unique())
min(df[mask].Date.unique())
mask = (df['Distance_id']==3)
max(df[mask].Date.unique())
min(df[mask].Date.unique())
mask = (df['Distance_id']==4)
max(df[mask].Date.unique())
min(df[mask].Date.unique())
mask = (df['Distance_id']==5)
max(df[mask].Date.unique())
min(df[mask].Date.unique())

df.Distance_id.unique()

mycols = ['event_id','Date','GRTS_Order','Plot_Name', 'filename']
# before 2019-01-01, we only ever used (1,2), which should be: {1:"<=50 Meters", 2:"50-100 Meters"}
mask = (df['Date']<dt.datetime(2019,1,1))
df[mask].Distance_id.unique()
# from 2019-01-01 to 2020-01-01, we used (2,5,4) which should be: {2:"51-100 Meters", 4:"<= 25 Meters", 5:"26-50 Meters"}
mask = (df['Date']>dt.datetime(2019,1,1)) & (df['Date']<dt.datetime(2020,1,1))
df[mask].Distance_id.unique()
# no sampling in 2020
mask = (df['Date']>dt.datetime(2020,1,1)) & (df['Date']<dt.datetime(2021,1,1))
df[mask].Distance_id.unique()
# from 2021-01-01 to 2022-01-01, we used (2,3,5,4) which should be: {2:"51-100 Meters", 4:"<= 25 Meters", 5:"26-50 Meters", with unknown (3) because it was deprecated in 2019}
mask = (df['Date']>dt.datetime(2021,1,1)) & (df['Date']<dt.datetime(2022,1,1))
df[mask].Distance_id.unique()
mask = (df['Date']>dt.datetime(2021,1,1)) & (df['Date']<dt.datetime(2022,1,1)) & (df['Distance_id']==3)
df[mask][mycols].drop_duplicates('event_id')
# from 2022-01-01 to 2023-01-01, we used (1,2,3,5,4) which should be: {2:"51-100 Meters", 4:"<= 25 Meters", 5:"26-50 Meters", with unknown (1,3) because those are deprecated in 2019}
mask = (df['Date']>dt.datetime(2022,1,1)) & (df['Date']<dt.datetime(2023,1,1))
df[mask].Distance_id.unique()
mask = (df['Date']>dt.datetime(2022,1,1)) & (df['Date']<dt.datetime(2023,1,1)) & (df['Distance_id']==1)
df[mask][mycols].drop_duplicates('event_id')
mask = (df['Date']>dt.datetime(2022,1,1)) & (df['Date']<dt.datetime(2023,1,1)) & (df['Distance_id']==3)
df[mask][mycols].drop_duplicates('event_id')
# from 2023-01-01 to 2024-01-01, we used (2,3,5,4) which should be: {2:"51-100 Meters", 4:"<= 25 Meters", 5:"26-50 Meters", with unknown (3) because it was deprecated in 2019}
mask = (df['Date']>dt.datetime(2023,1,1))
df[mask].Distance_id.unique()
mask = (df['Date']>dt.datetime(2023,1,1)) & (df['Distance_id']==3)
df[mask][mycols].drop_duplicates('event_id')
