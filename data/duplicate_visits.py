
import pandas as pd
import numpy as np
import src.make_templates as mt
import src.tbl_xwalks as tx
import openpyxl
import datetime
birds = mt.make_birds()

###### TO DO MONDAY 2024-03-11 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# 1. Sort out all group values present in outcomes['review']
outcomes = tx._find_dupe_site_visits(birds)
findme = []
for k in outcomes['review'].keys():
    for v in outcomes['review'][k]['DetectionEventID']:
        findme.append(v)

df = birds['ncrn']['DetectionEvent']['source'][birds['ncrn']['DetectionEvent']['source']['event_id'].isin(findme)].merge(birds['ncrn']['Location']['source'], left_on='location_id', right_on='Location_ID', how='left')
df = df.merge(birds['ncrn']['Protocol']['source'][['Protocol_ID','Protocol_Name']], left_on='protocol_id', right_on='Protocol_ID', how='left')
df = df[['event_id','Date','activity_start_datetime','Protocol_Name','GRTS_Order','Plot_Name']].sort_values(['Date','GRTS_Order'])
df['likely_resolution'] = None
df['group'] = df['Date'].astype(str) +'.' + df['GRTS_Order'].astype(str) + '.' + df['Plot_Name']
df['resolution'] = None
for group in df['group'].unique():
    mysub = df[df['group'] == group]
    if len(mysub.activity_start_datetime.unique()) == 1:
        df['likely_resolution'] = np.where(df['event_id'].isin(mysub.event_id.unique()), 'duplicate event; field crew was unable to delete', df['likely_resolution'])
df

unknowns = df[df['likely_resolution'].isna()]
tmp = birds['ncrn']['BirdDetection']['source'][birds['ncrn']['BirdDetection']['source']['Event_ID'].isin(unknowns.event_id.unique())].groupby(['Event_ID','AOU_Code']).size().reset_index(name='count').sort_values(['count'], ascending=True)
tmp = tmp.sort_values('AOU_Code')
tmp = tmp.merge(unknowns[['event_id', 'activity_start_datetime', 'GRTS_Order','group']], left_on='Event_ID', right_on='event_id', how='left')
for group in tmp.group.unique():
    pd.pivot_table(tmp[tmp['group']==group],values='count',index='AOU_Code', columns=['group','activity_start_datetime'])

unknowns = df
alldf = birds['ncrn']['BirdDetection']['source'][birds['ncrn']['BirdDetection']['source']['Event_ID'].isin(unknowns.event_id.unique())].groupby(['Event_ID','AOU_Code']).size().reset_index(name='count').sort_values(['count'], ascending=True)
alldf = alldf.sort_values('AOU_Code')
alldf = alldf.merge(unknowns[['event_id', 'activity_start_datetime', 'GRTS_Order','group']], left_on='Event_ID', right_on='event_id', how='left')
for group in alldf.group.unique():
    pd.pivot_table(alldf[alldf['group']==group],values='count',index='AOU_Code', columns=['group','activity_start_datetime'])

output = {}
output['research'] = df
# whole dataset:
tmpdict = {}
for group in alldf.group.unique():
    tmpdict[group] = pd.pivot_table(alldf[alldf['group']==group],values='count',index='AOU_Code', columns=['group','activity_start_datetime','Event_ID'])
# mysteries-only:
# for group in tmp.group.unique():
#     tmpdict[group] = pd.pivot_table(tmp[tmp['group']==group],values='count',index='AOU_Code', columns=['group','activity_start_datetime','Event_ID'])
output.update(dict(sorted(tmpdict.items())))

with pd.ExcelWriter("data/birds_research.xlsx") as writer:
    for k in output.keys():
        try:
            output[k].to_excel(writer,sheet_name=k, index=False)
        except:
            output[k].to_excel(writer,sheet_name=k)
###### TO DO MONDAY 2024-03-11 +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# update the output excel to show only unresolved rows (for UD)
outcomedf = pd.read_excel(r'data\birds_research.xlsx', sheet_name='research')
outcomedf = outcomedf[outcomedf['resolution'].isna()]
outcomedf.rename(columns={'dummy':'group'},inplace=True)

alldf = alldf[alldf['group'].isin(outcomedf.group.unique())]

output = {}
output['research'] = outcomedf
output['research']['Date'] = output['research']['Date'].dt.date
# whole dataset:
tmpdict = {}
for group in alldf.group.unique():
    tmpdict[group] = pd.pivot_table(alldf[alldf['group']==group],values='count',index='AOU_Code', columns=['group','activity_start_datetime','Event_ID'])
output.update(dict(sorted(tmpdict.items())))

with pd.ExcelWriter("data/birds_dupes_no_datasheet.xlsx") as writer:
    for k in output.keys():
        try:
            output[k].to_excel(writer,sheet_name=k, index=False)
        except:
            output[k].to_excel(writer,sheet_name=k)

smalldf = outcomedf[['Date','GRTS_Order','Plot_Name','group']].drop_duplicates('group')
with pd.ExcelWriter("data/birdsmall.xlsx") as writer:
    smalldf.to_excel(writer,sheet_name='small', index=False)


# same as above but add the observer and recorder
# +---
outcomes = tx._find_dupe_site_visits(birds)
findme = []
for k in outcomes['review'].keys():
    for v in outcomes['review'][k]['DetectionEventID']:
        findme.append(v)

df = birds['ncrn']['DetectionEvent']['source'][birds['ncrn']['DetectionEvent']['source']['event_id'].isin(findme)].merge(birds['ncrn']['Location']['source'], left_on='location_id', right_on='Location_ID', how='left')
df = df.merge(birds['ncrn']['Protocol']['source'][['Protocol_ID','Protocol_Name']], left_on='protocol_id', right_on='Protocol_ID', how='left')
before_cols = list(df.columns)
df = df.merge(birds['ncrn']['Contact']['source'][['Contact_ID','Last_Name','First_Name']], left_on='observer', right_on='Contact_ID', how='left')
df['observer'] = df['First_Name'] + ' ' + df['Last_Name']
df = df[before_cols]
df = df.merge(birds['ncrn']['Contact']['source'][['Contact_ID','Last_Name','First_Name']], left_on='recorder', right_on='Contact_ID', how='left')
df['recorder'] = df['First_Name'] + ' ' + df['Last_Name']
df = df[before_cols]
# df = df.merge(birds['ncrn']['Contact']['source'][['Contact_ID','Last_Name','First_Name']], left_on='entered_by', right_on='Contact_ID', how='left')
# df['entered_by'] = df['First_Name'] + ' ' + df['Last_Name']
# df = df[before_cols]
df = df[['event_id','Date','activity_start_datetime','Protocol_Name','GRTS_Order','Plot_Name','observer', 'recorder', 'entered_by']].sort_values(['Date','GRTS_Order'])
df['likely_resolution'] = None
df['group'] = df['Date'].astype(str) +'.' + df['GRTS_Order'].astype(str) + '.' + df['Plot_Name']
df['resolution'] = None
for group in df['group'].unique():
    mysub = df[df['group'] == group]
    if len(mysub.activity_start_datetime.unique()) == 1:
        df['likely_resolution'] = np.where(df['event_id'].isin(mysub.event_id.unique()), 'duplicate event; field crew was unable to delete', df['likely_resolution'])
df

unknowns = df[df['likely_resolution'].isna()]
tmp = birds['ncrn']['BirdDetection']['source'][birds['ncrn']['BirdDetection']['source']['Event_ID'].isin(unknowns.event_id.unique())].groupby(['Event_ID','AOU_Code']).size().reset_index(name='count').sort_values(['count'], ascending=True)
tmp = tmp.sort_values('AOU_Code')
tmp = tmp.merge(unknowns[['event_id', 'activity_start_datetime', 'GRTS_Order','group', 'observer', 'recorder', 'entered_by']], left_on='Event_ID', right_on='event_id', how='left')
for group in tmp.group.unique():
    pd.pivot_table(tmp[tmp['group']==group],values='count',index='AOU_Code', columns=['group','activity_start_datetime'])

unknowns = df
alldf = birds['ncrn']['BirdDetection']['source'][birds['ncrn']['BirdDetection']['source']['Event_ID'].isin(unknowns.event_id.unique())].groupby(['Event_ID','AOU_Code']).size().reset_index(name='count').sort_values(['count'], ascending=True)
alldf = alldf.sort_values('AOU_Code')
alldf = alldf.merge(unknowns[['event_id', 'activity_start_datetime', 'GRTS_Order','group', 'observer', 'recorder', 'entered_by']], left_on='Event_ID', right_on='event_id', how='left')
for group in alldf.group.unique():
    pd.pivot_table(alldf[alldf['group']==group],values='count',index='AOU_Code', columns=['group', 'Event_ID', 'activity_start_datetime', 'observer', 'recorder', 'entered_by'])

output = {}
output['research'] = df
# whole dataset:
tmpdict = {}
for group in alldf.group.unique():
    tmpdict[group] = pd.pivot_table(alldf[alldf['group']==group],values='count',index='AOU_Code', columns=['group', 'Event_ID', 'activity_start_datetime', 'observer', 'recorder', 'entered_by'])
# mysteries-only:
# for group in tmp.group.unique():
#     tmpdict[group] = pd.pivot_table(tmp[tmp['group']==group],values='count',index='AOU_Code', columns=['group','activity_start_datetime','Event_ID'])
output.update(dict(sorted(tmpdict.items())))

with pd.ExcelWriter("data/birds_research_20230314.xlsx") as writer:
    for k in output.keys():
        try:
            output[k].to_excel(writer,sheet_name=k, index=False)
        except:
            output[k].to_excel(writer,sheet_name=k)



outcomedf = pd.read_excel(r'data\birds_research_20230314.xlsx', sheet_name='research')
outcomedf = outcomedf[outcomedf['resolution'].isna()]
outcomedf.rename(columns={'dummy':'group'},inplace=True)
smalldf = outcomedf[['Date','GRTS_Order','Plot_Name', 'group']].drop_duplicates('group')
smalldf['Date'] = smalldf['Date'].dt.date
with pd.ExcelWriter("data/birdsmall_20240314.xlsx") as writer:
    smalldf.to_excel(writer,sheet_name='small', index=False)




















# outcomes = tx._find_dupe_site_visits(birds)
# findme = []
# for k in outcomes['review'].keys():
#     for v in outcomes['review'][k]['DetectionEventID']:
#         findme.append(v)

# df = birds['ncrn']['DetectionEvent']['source'][birds['ncrn']['DetectionEvent']['source']['event_id'].isin(findme)].merge(birds['ncrn']['Location']['source'], left_on='location_id', right_on='Location_ID', how='left')
# df = df.merge(birds['ncrn']['Protocol']['source'][['Protocol_ID','Protocol_Name']], left_on='protocol_id', right_on='Protocol_ID', how='left')
# df = df.merge(birds['ncrn']['Contact']['source'][['Contact_ID','Last_Name','First_Name']], left_on='observer', right_on='Contact_ID', how='left')
# df['observer'] = df['First_Name'] + ' ' + df['Last_Name']
# df = df[['event_id','Date','activity_start_datetime','Protocol_Name','GRTS_Order','Plot_Name','observer']].sort_values(['Date','GRTS_Order'])
# df['likely_resolution'] = None
# df['group'] = df['Date'].astype(str) +'.' + df['GRTS_Order'].astype(str) + '.' + df['Plot_Name']
# df['resolution'] = None
# for group in df['group'].unique():
#     mysub = df[df['group'] == group]
#     if len(mysub.activity_start_datetime.unique()) == 1:
#         df['likely_resolution'] = np.where(df['event_id'].isin(mysub.event_id.unique()), 'duplicate event; field crew was unable to delete', df['likely_resolution'])
# df

# unknowns = df[df['likely_resolution'].isna()]
# tmp = birds['ncrn']['BirdDetection']['source'][birds['ncrn']['BirdDetection']['source']['Event_ID'].isin(unknowns.event_id.unique())].groupby(['Event_ID','AOU_Code']).size().reset_index(name='count').sort_values(['count'], ascending=True)
# tmp = tmp.sort_values('AOU_Code')
# tmp = tmp.merge(unknowns[['event_id', 'activity_start_datetime', 'GRTS_Order','group', 'observer']], left_on='Event_ID', right_on='event_id', how='left')
# for group in tmp.group.unique():
#     pd.pivot_table(tmp[tmp['group']==group],values='count',index='AOU_Code', columns=['group','activity_start_datetime'])

# unknowns = df
# alldf = birds['ncrn']['BirdDetection']['source'][birds['ncrn']['BirdDetection']['source']['Event_ID'].isin(unknowns.event_id.unique())].groupby(['Event_ID','AOU_Code']).size().reset_index(name='count').sort_values(['count'], ascending=True)
# alldf = alldf.sort_values('AOU_Code')
# alldf = alldf.merge(unknowns[['event_id', 'activity_start_datetime', 'GRTS_Order','group', 'observer']], left_on='Event_ID', right_on='event_id', how='left')
# for group in alldf.group.unique():
#     pd.pivot_table(alldf[alldf['group']==group],values='count',index='AOU_Code', columns=['group', 'Event_ID', 'activity_start_datetime', 'observer'])

# output = {}
# output['research'] = df
# # whole dataset:
# tmpdict = {}
# for group in alldf.group.unique():
#     tmpdict[group] = pd.pivot_table(alldf[alldf['group']==group],values='count',index='AOU_Code', columns=['group', 'Event_ID', 'activity_start_datetime', 'observer'])
# # mysteries-only:
# # for group in tmp.group.unique():
# #     tmpdict[group] = pd.pivot_table(tmp[tmp['group']==group],values='count',index='AOU_Code', columns=['group','activity_start_datetime','Event_ID'])
# output.update(dict(sorted(tmpdict.items())))

# with pd.ExcelWriter("data/birds_research_shortlist.xlsx") as writer:
#     for k in output.keys():
#         try:
#             output[k].to_excel(writer,sheet_name=k, index=False)
#         except:
#             output[k].to_excel(writer,sheet_name=k)