"""Data exploration to resove mismatching AOU Codes"""

import pandas as pd
import numpy as np
import src.make_templates as mt

testdict = mt.make_birds()
realvals = pd.read_csv(r'assets\db\bird_species.csv')
realvals.rename(columns={'Code':'AOU_Code', 'CommonName':'Common_Name', 'ScientificName':'Scientific_Name'}, inplace=True)
realvals2 = pd.read_csv(r'assets\db\official_BirdSpecies.csv')
realvals = realvals[[x for x in realvals.columns if x == 'AOU_Code' or x not in realvals2.columns]]
realvals = realvals[[x for x in realvals.columns if x != 'ID' and x != 'Comments']]

species = testdict['ncrn']['BirdSpecies']['source'].copy()
locs = testdict['ncrn']['Location']['source'].copy()
questions = species[species['AOU_Code'].isin(realvals.AOU_Code.unique())==False].AOU_Code.unique()
fielddata = testdict['ncrn']['BirdDetection']['source'].copy()
events = testdict['ncrn']['DetectionEvent']['source'].copy()

df = events.rename(columns={'event_id':'Event_ID'}).merge(fielddata, on='Event_ID', how='left')
df = df.merge(species, on='AOU_Code', how='left')
df = df.rename(columns={'location_id':'Location_ID'}).merge(locs, on='Location_ID', how='left')
df = df[['Event_ID', 'Location_ID', 'Unit_Code', 'Plot_Name', 'Date', 'AOU_Code']]
df = df[df['AOU_Code'].isin(questions)]
df.AOU_Code.unique()
len(df.AOU_Code.unique())
df.sort_values('Date')
df.sort_values('Date', ascending=False)
len(df.AOU_Code.unique())
len(df.Event_ID.unique())
len(df.Location_ID.unique())
len(df.Unit_Code.unique())
species[species['AOU_Code']=='UNCR']

# There are 20 4-character AOU species codes that exist in our dataset
# but do not have any taxonomic information in `tlu_Species`
# nor do they exist in the current NPS Landbirds species code list

# The species codes are:
# ['UNCR', 'ETTI', 'UNCH', 'UNWA', 'UNDU', 'RODO', 'UNFL', 'UNSP',
#        'WPWI', 'UNTH', 'UNWR', 'UNSW', 'UNHA', 'UNOW', 'SOVI', 'GRBH',
#        'SASP', 'UNAH', 'CARC', 'BTHH']

# These species codes were recorded for 10623 bird detections tbl_Field_Data
# These detections are from 6630 unique Event_IDs at 646 unique Location IDs in 12 parks.
# These detections occurred from 2007-05-04 through 2023-07-15.

summarydf = df.groupby(['AOU_Code']).size().reset_index(name='count').sort_values(['count'], ascending=False)
summarydf['maxdate'] = ''
summarydf['mindate'] = ''
# summarydf['most_common_park'] = ''
summarydf['most_common_plot'] = ''
summarydf['scientific_name'] = np.NaN
summarydf['common_name'] = np.NaN

for c in summarydf.AOU_Code.values:
    mindate = min(df[df['AOU_Code']==c].Date)
    maxdate = max(df[df['AOU_Code']==c].Date)
    # com_park = df[df['AOU_Code']==c].groupby(['Unit_Code']).size().reset_index(name='count').sort_values(['count'], ascending=False).max(level=0).Unit_Code.unique()[0]
    com_loc = df[df['AOU_Code']==c].groupby(['Plot_Name']).size().reset_index(name='count').sort_values(['count'], ascending=False).max(level=0).Plot_Name.unique()[0]
    summarydf['mindate'] = np.where(summarydf['AOU_Code']==c, mindate, summarydf['mindate'])
    summarydf['maxdate'] = np.where(summarydf['AOU_Code']==c, maxdate, summarydf['maxdate'])
    # summarydf['most_common_park'] = np.where(summarydf['AOU_Code']==c, com_park, summarydf['most_common_park'])
    summarydf['most_common_plot'] = np.where(summarydf['AOU_Code']==c, com_loc, summarydf['most_common_plot'])

save_to = r'data/species_questions_20240215.csv'
summarydf.to_csv(r'data/species_questions_20240215.csv', index=False)

testdf = pd.read_csv(save_to)
