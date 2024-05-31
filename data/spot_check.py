"""
spot check db records against pdfs

Find scanned datasheets:
~\OneDrive - DOI\Documents - NCRN Birds\Field operations\scanned_datasheets
"""
import src.check as c
import pandas as pd
import numpy as np
import src.make_templates as mt
import glob

# make the dataset from code
birds = mt.make_birds()
c.check_birds(birds)
birds = c.validate_db(birds, 10, True)


"""
Spot-check detection-level attributes

1. AOU Code
2. bird sex
3. bird time interval
4. bird detection type
5. bird distance class
"""

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

db = pd.merge(birds['ncrn']['DetectionEvent']['db'][['ID','LocationID','StartDateTime', 'RelativeHumidity']], birds['ncrn']['BirdDetection']['db'][['ID','BirdSpeciesParkID','DetectionEventID','ProtocolDistanceClassID','SexID','ProtocolTimeIntervalID','ProtocolDetectionTypeID']].rename(columns={'ID':'DetectionID'}), left_on='ID', right_on='DetectionEventID')
db = pd.merge(db, birds['ncrn']['Location']['db'][['ID','Code']].rename(columns={'ID':'loc_key','Code':'GRTS_Order'}), left_on='LocationID', right_on='loc_key')
db = pd.merge(db, birds['lu']['DistanceClass']['db'][['ID','Label']].rename(columns={'ID':'dist_key','Label':'Distance_Text'}), left_on='ProtocolDistanceClassID', right_on='dist_key')
db = pd.merge(db, birds['lu']['Sex']['db'][['ID','Code','Label']].rename(columns={'ID':'Sex_ID','Code':'Sex_Code_Value','Label':'Sex_Code_Description'}), left_on='SexID', right_on='Sex_ID')
db = pd.merge(db, birds['lu']['TimeInterval']['db'][['ID','Label']].rename(columns={'ID':'time_key','Label':'Interval_Length'}), left_on='ProtocolTimeIntervalID', right_on='time_key')
db = pd.merge(db, birds['lu']['DetectionType']['db'][['ID','Code']].rename(columns={'ID':'det_key','Code':'ID_Code'}), left_on='ProtocolDetectionTypeID', right_on='det_key')
db = pd.merge(db, birds['ncrn']['BirdSpeciesPark']['db'][['ID','BirdSpeciesID']].rename(columns={'ID':'bsp_key'}), left_on='BirdSpeciesParkID', right_on='bsp_key')
db = pd.merge(db, birds['ncrn']['BirdSpecies']['db'][['ID','Code']].rename(columns={'ID':'bs_key','Code':'AOU_Code'}), left_on='BirdSpeciesID', right_on='bs_key')


# spot check birds_anti_grts346_20130520
mask = (source['Date'].astype(str)=='2013-05-20') & (source['GRTS_Order']=='346')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2013-05-20') & (db['GRTS_Order']=='346')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts346_20220702
mask = (source['Date'].astype(str)=='2022-07-02') & (source['GRTS_Order']=='346')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2022-07-02') & (db['GRTS_Order']=='346')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts397_20230618
mask = (source['Date'].astype(str)=='2023-06-18') & (source['GRTS_Order']=='397')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2023-06-18') & (db['GRTS_Order']=='397')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts408_20170626
mask = (source['Date'].astype(str)=='2017-06-26') & (source['GRTS_Order']=='408')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2017-06-26') & (db['GRTS_Order']=='408')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts408_20170708
mask = (source['Date'].astype(str)=='2017-07-08') & (source['GRTS_Order']=='408')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2017-07-08') & (db['GRTS_Order']=='408')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts414_20130520
mask = (source['Date'].astype(str)=='2013-05-20') & (source['GRTS_Order']=='414')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2013-05-20') & (db['GRTS_Order']=='414')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts1189_20230619
mask = (source['Date'].astype(str)=='2023-06-19') & (source['GRTS_Order']=='1189')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2023-06-19') & (db['GRTS_Order']=='1189')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts2517_20190719
mask = (source['Date'].astype(str)=='2013-05-20') & (source['GRTS_Order']=='414')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2013-05-20') & (db['GRTS_Order']=='414')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts2517_20230524
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2023-05-24') & (db['GRTS_Order']=='2517')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts2592_20220524 # data entry error: distance text EUST x 2
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2022-05-24') & (db['GRTS_Order']=='2592')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts2592_20220701 # not sure about this one; data entry errors?
mask = (source['Date'].astype(str)=='2022-07-01') & (source['GRTS_Order']=='2592')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2022-07-01') & (db['GRTS_Order']=='2592')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts2622_20180716
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2018-07-16') & (db['GRTS_Order']=='2622')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# spot check birds_anti_grts2627_20170626
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2017-06-26') & (db['GRTS_Order']=='2627')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_anti_grts4505_20230619
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2023-06-19') & (db['GRTS_Order']=='4505')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_camp9005_20180712
mask = (source['Date'].astype(str)=='2018-07-12') & (source['GRTS_Order']=='')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2018-07-12') & (db['GRTS_Order']=='CAMP-9005')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_camp9006_20180712
mask = (source['Date'].astype(str)=='2018-07-12') & (source['GRTS_Order']=='')
source[mask][['Date', 'AOU_Code', 'GRTS_Order', 'Distance_Text', 'Sex_ID', 'Sex_Code_Description', 'Interval_Length', 'ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2018-07-12') & (db['GRTS_Order']=='CAMP-9006')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_choh_grts60_20160610
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2016-06-10') & (db['GRTS_Order']=='60')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_choh_grts314_20160629
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2016-06-29') & (db['GRTS_Order']=='314')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_choh_grts586_20160629
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2016-06-29') & (db['GRTS_Order']=='586')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_choh_grts671_20160609
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2016-06-09') & (db['GRTS_Order']=='671')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_gwmp_grts113_20130522
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2013-05-22') & (db['GRTS_Order']=='113')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_gwmp_grts444_20130616
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2013-06-16') & (db['GRTS_Order']=='444')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_gwmp_grts668_20130522
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2013-05-22') & (db['GRTS_Order']=='668')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_hafe_grts3_20160622
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2016-06-22') & (db['GRTS_Order']=='3')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_hafe_grts19_20160622
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2016-06-22') & (db['GRTS_Order']=='19')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_hafe_grts197_20160621
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2016-06-21') & (db['GRTS_Order']=='197')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_hafe_grts552_20160528
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2016-05-28') & (db['GRTS_Order']=='552')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_hafe_grts673_20160621
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2016-06-21') & (db['GRTS_Order']=='673')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_hafe_grts947_20160621
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2016-06-21') & (db['GRTS_Order']=='947')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])
# birds_hafe_grts2994_20170709
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]=='2017-07-09') & (db['GRTS_Order']=='2994')
db[mask][['StartDateTime','AOU_Code', 'GRTS_Order','Distance_Text','Sex_ID','Sex_Code_Value','Sex_Code_Description','Interval_Length','ID_Code']].sort_values(['Interval_Length','Distance_Text','AOU_Code'])

"""
Spot-check event-level attributes

note: attributes are 0-index in `source` and 1-index in `db`, so 0==1.

1. weather
2. humidity
3. wind
4. noise
"""
db = pd.merge(birds['ncrn']['DetectionEvent']['db'][['ID','LocationID','StartDateTime', 'ProtocolNoiseLevelID', 'ProtocolWindCodeID', 'ProtocolPrecipitationTypeID', 'RelativeHumidity','AirTemperature']], birds['ncrn']['Location']['db'][['ID','Code']].rename(columns={'ID':'loc_key','Code':'GRTS_Order'}), left_on='LocationID', right_on='loc_key')
db = pd.merge(db, birds['lu']['NoiseLevel']['db'][['ID','Label']].rename(columns={'ID':'noise_key','Label':'noise_label'}), left_on='ProtocolNoiseLevelID', right_on='noise_key')
db = pd.merge(db, birds['lu']['WindCode']['db'][['ID','Label']].rename(columns={'ID':'wind_key','Label':'wind_label'}), left_on='ProtocolWindCodeID', right_on='wind_key')
db = pd.merge(db, birds['lu']['PrecipitationType']['db'][['ID','Label']].rename(columns={'ID':'precip_key','Label':'precip_label'}), left_on='ProtocolPrecipitationTypeID', right_on='precip_key')

source = pd.merge(birds['ncrn']['DetectionEvent']['source'], birds['ncrn']['Location']['source'], left_on='location_id', right_on='Location_ID')
source = pd.merge(source, birds['lu']['NoiseLevel']['source'], left_on='disturbance_level', right_on='Disturbance_Code')
source = pd.merge(source, birds['lu']['WindCode']['source'], left_on='wind_speed', right_on='Wind_Code')
precip_lu = birds['lu']['PrecipitationType']['source'].copy()
precip_lu['k_field'] = precip_lu.index
source = pd.merge(source, precip_lu[['k_field','Label']].rename(columns={'Label':'precip_label'}), left_on='sky_condition', right_on='k_field')

# birds_anti_grts346_20130520
mygrts = '346'
mydate = '2013-05-20'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts346_20220702
mygrts = '346'
mydate = '2022-07-02'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts397_20230618
mygrts = '397'
mydate = '2023-06-18'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key', 'AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts408_20170626
mygrts = '408'
mydate = '2017-06-26'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts408_20170708
mygrts = '408'
mydate = '2017-07-08'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts414_20130520
mygrts = '414'
mydate = '2013-05-20'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts980_20170708
mygrts = '980'
mydate = '2017-07-08'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts1189_20230619
mygrts = '1189'
mydate = '2023-06-19'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts1555_20170528
mygrts = '1555'
mydate = '2017-05-28'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts1623_20170717
mygrts = '1623'
mydate = '2017-07-14'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts2468_20170714
mygrts = '2468'
mydate = '2017-07-14'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts2517_20190719 PASS
mygrts = '2517'
mydate = '2019-07-19'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts2517_20230524 PASS
mygrts = '2517'
mydate = '2023-05-24'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts2592_20220524 PASS
mygrts = '2592'
mydate = '2022-05-24'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts2554_20170708
mygrts = '2592'
mydate = '2022-05-24'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts2627_20170707
mygrts = '2627'
mydate = '2017-07-07'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts4079_20170707
mygrts = '4079'
mydate = '2017-07-07'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts4173_20170714
mygrts = '4079'
mydate = '2017-07-07'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts4398_20170726
mygrts = '4398'
mydate = '2017-06-28'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_cato_grts254_20130529
mygrts = '254'
mydate = '2013-05-29'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_cato_grts262_20130626
mygrts = '262'
mydate = '2013-06-26'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_choh_grts5_20160608
mygrts = '262'
mydate = '2013-06-26'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_choh_grts60_20160610
mygrts = '60'
mydate = '2016-06-10'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_choh_grts314_20160629
mygrts = '314'
mydate = '2016-06-29'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_choh_grts586_20160629
mygrts = '586'
mydate = '2016-06-29'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_choh_grts671_20160609
mygrts = '671'
mydate = '2016-06-09'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_gwmp_grts113_20130522
mygrts = '113'
mydate = '2013-05-22'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_gwmp_grts444_20130616
mygrts = '444'
mydate = '2013-06-16'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_gwmp_grts668_20130522
mygrts = '668'
mydate = '2013-05-22'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_hafe_grts3_20160622
mygrts = '3'
mydate = '2016-06-22'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_hafe_grts19_20160622
mygrts = '19'
mydate = '2016-06-22'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_hafe_grts197_20160621
mygrts = '197'
mydate = '2016-06-21'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_hafe_grts552_20160528
mygrts = '552'
mydate = '2016-05-28'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_hafe_grts673_20160621
mygrts = '673'
mydate = '2016-06-21'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_hafe_grts947_20160621
mygrts = '947'
mydate = '2016-06-21'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_hafe_grts2994_20170709
mygrts = '2994'
mydate = '2017-07-09'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts2592_20220701
mygrts = '2592'
mydate = '2022-07-01'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts2622_20180716
mygrts = '2622'
mydate = '2018-07-16'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_anti_grts2627_20170626
mygrts = '2627'
mydate = '2017-06-26'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]
# birds_anti_grts4505_20230619
mygrts = '4505'
mydate = '2023-06-19'
mask = (source['Date'].astype(str)==mydate) & (source['GRTS_Order']==mygrts)
source[mask][['Date', 'GRTS_Order', 'humidity', 'disturbance_level','wind_speed','sky_condition', 'temperature']]
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_camp9005_20180712
mygrts = 'CAMP-9005'
mydate = '2018-07-12'
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]

# birds_camp9006_20180712
mygrts = 'CAMP-9006'
mydate = '2018-07-12'
mask = (db['StartDateTime'].astype(str).str.split(' ',1).str[0]==mydate) & (db['GRTS_Order']==mygrts)
db[mask][['StartDateTime', 'GRTS_Order', 'RelativeHumidity','noise_key', 'wind_key', 'precip_key','AirTemperature', 'noise_label','wind_label','precip_label']]


# summarize:
txtfiles = []
for file in glob.glob(r"data\scanned_datasheets\*.pdf"):
    txtfiles.append(file)
datasheets = pd.DataFrame()
datasheets['filepath'] = txtfiles
datasheets['filename'] = datasheets['filepath'].str.rsplit(pat='\\',n=1).str[1]
# 'https://doimspp.sharepoint.com/:b:/r/sites/NCRNBirds/Shared%20Documents/Field%20operations/scanned_datasheets/birds_anti_grts346_20130520.pdf'
beginning = r'https://doimspp.sharepoint.com/:b:/r/sites/NCRNBirds/Shared%20Documents/Field%20operations/scanned_datasheets/'
datasheets['url'] = beginning + datasheets['filename']
datasheets['date_raw'] = datasheets['filepath'].str.replace('_fledg','').str.rsplit(pat='.pdf',n=1).str[0].str.rsplit(pat='\\',n=1).str[1].str.rsplit(pat='_',n=1).str[1]
datasheets['year'] = datasheets['date_raw'].str[:4]
datasheets['month'] = datasheets['date_raw'].str[4:6]
datasheets['day'] = datasheets['date_raw'].str[6:]
datasheets['grts'] = datasheets['filepath'].str.replace('_fledg','').str.rsplit(pat='.pdf',n=1).str[0].str.rsplit(pat='\\',n=1).str[1].str.rsplit(pat='_',n=2).str[1].str.replace('grts','')
datasheets['park'] = datasheets['filepath'].str.replace('_fledg','').str.rsplit(pat='.pdf',n=1).str[0].str.rsplit(pat='\\',n=1).str[1].str.rsplit(pat='_',n=3).str[1].str.upper()


# how many parks did we cover?
len([x for x in datasheets.park.unique() if 'camp' not in x.lower()]) # this many plus camp
len(datasheets.year.unique()) # how many years?
min(datasheets.year.unique()) # min year
max(datasheets.year.unique()) # max year
len(datasheets.grts.unique()) # how many grts?

for f in datasheets.sort_values('date_raw').filename.values:
    print(f)

# flatten the whole dataset
db = pd.merge(birds['ncrn']['DetectionEvent']['db'][['ID','LocationID','StartDateTime', 'ProtocolNoiseLevelID', 'ProtocolWindCodeID', 'ProtocolPrecipitationTypeID', 'RelativeHumidity','AirTemperature']], birds['ncrn']['BirdDetection']['db'][['ID','BirdSpeciesParkID','DetectionEventID','ProtocolDistanceClassID','SexID','ProtocolTimeIntervalID','ProtocolDetectionTypeID']].rename(columns={'ID':'DetectionID'}), left_on='ID', right_on='DetectionEventID')
db = pd.merge(db, birds['ncrn']['Location']['db'][['ID','Code']].rename(columns={'ID':'loc_key','Code':'GRTS_Order'}), left_on='LocationID', right_on='loc_key')
db = pd.merge(db, birds['lu']['DistanceClass']['db'][['ID','Label']].rename(columns={'ID':'dist_key','Label':'Distance_Text'}), left_on='ProtocolDistanceClassID', right_on='dist_key')
db = pd.merge(db, birds['lu']['Sex']['db'][['ID','Code','Label']].rename(columns={'ID':'Sex_ID','Code':'Sex_Code_Value','Label':'Sex_Code_Description'}), left_on='SexID', right_on='Sex_ID')
db = pd.merge(db, birds['lu']['TimeInterval']['db'][['ID','Label']].rename(columns={'ID':'time_key','Label':'Interval_Length'}), left_on='ProtocolTimeIntervalID', right_on='time_key')
db = pd.merge(db, birds['lu']['DetectionType']['db'][['ID','Code']].rename(columns={'ID':'det_key','Code':'ID_Code'}), left_on='ProtocolDetectionTypeID', right_on='det_key')
db = pd.merge(db, birds['ncrn']['BirdSpeciesPark']['db'][['ID','BirdSpeciesID']].rename(columns={'ID':'bsp_key'}), left_on='BirdSpeciesParkID', right_on='bsp_key')
db = pd.merge(db, birds['ncrn']['BirdSpecies']['db'][['ID','Code']].rename(columns={'ID':'bs_key','Code':'AOU_Code'}), left_on='BirdSpeciesID', right_on='bs_key')
db = pd.merge(db, birds['lu']['NoiseLevel']['db'][['ID','Label']].rename(columns={'ID':'noise_key','Label':'noise_label'}), left_on='ProtocolNoiseLevelID', right_on='noise_key')
db = pd.merge(db, birds['lu']['WindCode']['db'][['ID','Label']].rename(columns={'ID':'wind_key','Label':'wind_label'}), left_on='ProtocolWindCodeID', right_on='wind_key')
db = pd.merge(db, birds['lu']['PrecipitationType']['db'][['ID','Label']].rename(columns={'ID':'precip_key','Label':'precip_label'}), left_on='ProtocolPrecipitationTypeID', right_on='precip_key')

db.to_csv(r'data/db_load_20240531.csv')
