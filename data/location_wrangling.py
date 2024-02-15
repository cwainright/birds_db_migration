"""Data exploration to resove mismatching location codes"""

import pandas as pd
import numpy as np
import src.make_templates as mt

testdict = mt.make_birds()

locs = testdict['ncrn']['Location']['source'].copy()
fielddata = testdict['ncrn']['BirdDetection']['source'].copy()
events = testdict['ncrn']['DetectionEvent']['source'].copy()
contacts = testdict['ncrn']['Contact']['source']

df = events.rename(columns={'location_id':'Location_ID', 'event_id':'Event_ID', 'event_notes':'Event_Notes', 'entered_date':'Entered_Date', 'updated_by':'Updated_By', 'entered_by':'Entered_By', 'is_excluded':'Is_Excluded', 'updated_date':'Updated_Date'}).merge(fielddata, on='Event_ID', how='left')
df = df.merge(locs, on='Location_ID', how='left')
df = df.merge(contacts, left_on='Entered_By', right_on='Contact_ID', how='left')
df['Entered_By'] = df['First_Name'].astype(str) + ' ' + df['Last_Name'].astype(str)
known_good = ['Event_ID', 'Location_ID', 'Date', 'Event_Notes', 'Unit_Code', 'Plot_Name', 'Active', 'Entered_Date', 'Entered_By', 'Updated_Date', 'Updated_By']
df = df[[x for x in df.columns if x in known_good or 'utm' in x.lower()]]

# events that are missing important attributes
# locations that don't have UTM
questions = list(df[df['UTM_X_Coord'].isna()].Event_ID.unique())
len(questions)
for e in df[df['UTM_Y_Coord'].isna()].Event_ID.unique():
    if e not in questions:
        questions.append(e)
        print(f'appended {e}')

# locations that don't have Plot_Name
plots = list(df[df['Plot_Name'].isna()].Event_ID.unique())
for e in plots:
    if e not in questions:
        questions.append(e)
        print(f'appended {e}')
# locations that don't have Unit_Code
units = list(df[df['Unit_Code'].isna()].Event_ID.unique())
for e in units:
    if e not in questions:
        questions.append(e)
        print(f'appended {e}')
# locations that don't have Active
active = list(df[df['Active'].isna()].Event_ID.unique())
for e in active:
    if e not in questions:
        questions.append(e)
        print(f'appended {e}')


df = df[df['Event_ID'].isin(questions)].drop_duplicates('Event_ID')

save_to = r'data/loc_questions_20240215.csv'
df.to_csv(save_to, index=False)

testdf = pd.read_csv(save_to)

