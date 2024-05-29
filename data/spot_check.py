"""
spot check db records against pdfs

Find scanned datasheets:
~\OneDrive - DOI\Documents - NCRN Birds\Field operations\scanned_datasheets
"""
import src.check as c
import pandas as pd
import numpy as np
import src.make_templates as mt


birds = mt.make_birds()
c.check_birds(birds)

# flatten
source = pd.merge(birds['ncrn']['DetectionEvent']['source'], birds['ncrn']['BirdDetection']['source'], left_on='event_id', right_on='Event_ID')
source = pd.merge(source, birds['ncrn']['Location']['source'], left_on='location_id', right_on='Location_ID')
source = pd.merge(source, birds['lu']['DistanceClass']['source'], on='Distance_id')
source = pd.merge(source, birds['lu']['Sex']['source'], left_on='Sex_ID', right_on='Sex_Code_Value')
source = pd.merge(source, birds['lu']['TimeInterval']['source'], on='Interval')
det_lu = birds['lu']['DetectionType']['source'].copy()
det_lu['ID_Method_Code'] = det_lu.index+1
source = pd.merge(source, det_lu, on='ID_Method_Code')
source['AOU_Code'] = source['AOU_Code'].str.split('_',1).str[0]

k_load = pd.merge(birds['ncrn']['DetectionEvent']['k_load'][['ID','LocationID','StartDateTime']], birds['ncrn']['BirdDetection']['k_load'][['ID','BirdSpeciesParkID','DetectionEventID','ProtocolDistanceClassID','SexID','ProtocolTimeIntervalID','ProtocolDetectionTypeID']].rename(columns={'ID':'DetectionID'}), left_on='ID', right_on='DetectionEventID')
k_load = pd.merge(k_load, birds['ncrn']['Location']['k_load'][['ID','Code']].rename(columns={'ID':'loc_key','Code':'GRTS_Order'}), left_on='LocationID', right_on='loc_key')
k_load = pd.merge(k_load, birds['lu']['DistanceClass']['k_load'][['ID','Label']].rename(columns={'ID':'dist_key','Label':'Distance_Text'}), left_on='ProtocolDistanceClassID', right_on='dist_key')
k_load = pd.merge(k_load, birds['lu']['Sex']['k_load'][['ID','Code','Label']].rename(columns={'ID':'Sex_ID','Code':'Sex_Code_Value','Label':'Sex_Code_Description'}), left_on='SexID', right_on='Sex_ID')
k_load = pd.merge(k_load, birds['lu']['TimeInterval']['k_load'][['ID','Label']].rename(columns={'ID':'time_key','Label':'Interval_Length'}), left_on='ProtocolTimeIntervalID', right_on='time_key')
k_load = pd.merge(k_load, birds['lu']['DetectionType']['k_load'][['ID','Code']].rename(columns={'ID':'det_key','Code':'ID_Code'}), left_on='ProtocolDetectionTypeID', right_on='det_key')
k_load = pd.merge(k_load, birds['ncrn']['BirdSpeciesPark']['k_load'][['ID','BirdSpeciesID']].rename(columns={'ID':'bsp_key'}), left_on='BirdSpeciesParkID', right_on='bsp_key')
k_load = pd.merge(k_load, birds['ncrn']['BirdSpecies']['k_load'][['ID','Code']].rename(columns={'ID':'bs_key','Code':'AOU_Code'}), left_on='BirdSpeciesID', right_on='bs_key')


# spot check birds_anti_grts346_20130520
mask = (source['Date'].astype(str)=='2013-05-20') & (source['GRTS_Order']=='346')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (k_load['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2013-05-20') & (k_load['GRTS_Order']=='346')
k_load[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts346_20220702
mask = (source['Date'].astype(str)=='2022-07-02') & (source['GRTS_Order']=='346')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (k_load['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2022-07-02') & (k_load['GRTS_Order']=='346')
k_load[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts397_20203618
mask = (source['Date'].astype(str)=='2023-06-18') & (source['GRTS_Order']=='397')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (k_load['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2023-06-18') & (k_load['GRTS_Order']=='397')
k_load[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts408_20170626
mask = (source['Date'].astype(str)=='2017-06-26') & (source['GRTS_Order']=='408')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (k_load['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2017-06-26') & (k_load['GRTS_Order']=='408')
k_load[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts408_20170708
mask = (source['Date'].astype(str)=='2017-07-08') & (source['GRTS_Order']=='408')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (k_load['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2017-07-08') & (k_load['GRTS_Order']=='408')
k_load[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts414_20130520
mask = (source['Date'].astype(str)=='2013-05-20') & (source['GRTS_Order']=='414')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (k_load['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2013-05-20') & (k_load['GRTS_Order']=='414')
k_load[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts1189_20230619
mask = (source['Date'].astype(str)=='2023-06-19') & (source['GRTS_Order']=='1189')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (k_load['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2023-06-19') & (k_load['GRTS_Order']=='1189')
k_load[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts2517_20190719
mask = (source['Date'].astype(str)=='2013-05-20') & (source['GRTS_Order']=='414')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (k_load['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2013-05-20') & (k_load['GRTS_Order']=='414')
k_load[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts2517_20230524
mask = (k_load['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2023-05-24') & (k_load['GRTS_Order']=='2517')
k_load[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts2592_20220524 # data entry error: distance text EUST x 2
mask = (k_load['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2022-05-24') & (k_load['GRTS_Order']=='2592')
k_load[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts2592_20220701 # not sure about this one; data entry errors?
mask = (source['Date'].astype(str)=='2022-07-01') & (source['GRTS_Order']=='2592')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (k_load['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2022-07-01') & (k_load['GRTS_Order']=='2592')
k_load[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts2622_20180716
mask = (k_load['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2018-07-16') & (k_load['GRTS_Order']=='2622')
k_load[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts2627_20170626
mask = (k_load['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2017-06-26') & (k_load['GRTS_Order']=='2627')
k_load[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_anti_grts4505_20230619
mask = (k_load['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2023-06-19') & (k_load['GRTS_Order']=='4505')
k_load[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_camp9005_20180712
mask = (source['Date'].astype(str)=='2018-07-12') & (source['GRTS_Order']=='')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (k_load['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2018-07-12') & (k_load['GRTS_Order']=='CAMP-9005')
k_load[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_camp9006_20180712
mask = (source['Date'].astype(str)=='2018-07-12') & (source['GRTS_Order']=='')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (k_load['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2018-07-12') & (k_load['GRTS_Order']=='CAMP-9006')
k_load[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])

