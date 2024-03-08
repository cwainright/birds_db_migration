
import pandas as pd
import numpy as np
import src.make_templates as mt
import src.tbl_xwalks as tx
import openpyxl
birds = mt.make_birds()

###### TO DO MONDAY 2024-03-11 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# 1. Sort out all dummy values present in outcomes['review']
outcomes = tx._find_dupe_site_visits(birds)
findme = []
for k in outcomes['review'].keys():
    for v in outcomes['review'][k]['DetectionEventID']:
        findme.append(v)

df = birds['ncrn']['DetectionEvent']['source'][birds['ncrn']['DetectionEvent']['source']['event_id'].isin(findme)].merge(birds['ncrn']['Location']['source'], left_on='location_id', right_on='Location_ID', how='left')
df = df.merge(birds['ncrn']['Protocol']['source'][['Protocol_ID','Protocol_Name']], left_on='protocol_id', right_on='Protocol_ID', how='left')
df = df[['event_id','Date','activity_start_datetime','Protocol_Name','GRTS_Order','Plot_Name']].sort_values(['Date','GRTS_Order'])
df['likely_resolution'] = None
df['dummy'] = df['Date'].astype(str) +'.' + df['GRTS_Order'].astype(str) + '.' + df['Plot_Name']
df['resolution'] = None
for dummy in df['dummy'].unique():
    mysub = df[df['dummy'] == dummy]
    if len(mysub.activity_start_datetime.unique()) == 1:
        df['likely_resolution'] = np.where(df['event_id'].isin(mysub.event_id.unique()), 'duplicate event; field crew was unable to delete', df['likely_resolution'])
df

unknowns = df[df['likely_resolution'].isna()]
tmp = birds['ncrn']['BirdDetection']['source'][birds['ncrn']['BirdDetection']['source']['Event_ID'].isin(unknowns.event_id.unique())].groupby(['Event_ID','AOU_Code']).size().reset_index(name='count').sort_values(['count'], ascending=True)
tmp = tmp.sort_values('AOU_Code')
tmp = tmp.merge(unknowns[['event_id', 'activity_start_datetime', 'GRTS_Order','dummy']], left_on='Event_ID', right_on='event_id', how='left')
for dummy in tmp.dummy.unique():
    pd.pivot_table(tmp[tmp['dummy']==dummy],values='count',index='AOU_Code', columns=['dummy','activity_start_datetime'])

unknowns = df
alldf = birds['ncrn']['BirdDetection']['source'][birds['ncrn']['BirdDetection']['source']['Event_ID'].isin(unknowns.event_id.unique())].groupby(['Event_ID','AOU_Code']).size().reset_index(name='count').sort_values(['count'], ascending=True)
alldf = alldf.sort_values('AOU_Code')
alldf = alldf.merge(unknowns[['event_id', 'activity_start_datetime', 'GRTS_Order','dummy']], left_on='Event_ID', right_on='event_id', how='left')
for dummy in alldf.dummy.unique():
    pd.pivot_table(alldf[alldf['dummy']==dummy],values='count',index='AOU_Code', columns=['dummy','activity_start_datetime'])

output = {}
output['research'] = df
# whole dataset:
tmpdict = {}
for dummy in alldf.dummy.unique():
    tmpdict[dummy] = pd.pivot_table(alldf[alldf['dummy']==dummy],values='count',index='AOU_Code', columns=['dummy','activity_start_datetime','Event_ID'])
# mysteries-only:
# for dummy in tmp.dummy.unique():
#     tmpdict[dummy] = pd.pivot_table(tmp[tmp['dummy']==dummy],values='count',index='AOU_Code', columns=['dummy','activity_start_datetime','Event_ID'])
output.update(dict(sorted(tmpdict.items())))

with pd.ExcelWriter("data/birds_research.xlsx") as writer:
    for k in output.keys():
        try:
            output[k].to_excel(writer,sheet_name=k, index=False)
        except:
            output[k].to_excel(writer,sheet_name=k)
###### TO DO MONDAY 2024-03-11 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++