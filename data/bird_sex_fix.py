"""
Fix the broken referential integrity in `tbl_Field_Data.Sex_ID`

Background:
From 2007 through 2016, NCRN consistently used integers to indicate bird sex: {1:"U",2:"M",3:"F"}.
In 2017 and 2018, NCRN used a confusing combination of two sets integers to indicate bird sex:
    {1:"U",2:"M",3:"F"}
    {0:"U",1:"M",2:"F"}
From 2019 to present, NCRN consistently used integers to indicate bird sex: {0:"U",1:"M",2:"F"}.

Narrative:
I don't know why NCRN used a combination of integers IDs in 2017 and 2018.
My guess is that NCRN intended to switch from 1-index to 0-index IDs in 2017.
However, since NCRN historically maintained >1 NCRN Landbirds MS Access database, a data manager may simply have deployed
a copy of the database with the old 1-index, instead of the new 0-index IDs.
If that happened, some records would continue logging 1-index IDs while others would use 0-index IDs.
At the end of the season, the data manager then merged the database backends in to one main dataset.
By merging those databases, the data manager inadvertently added IDs (0,1,2) and IDs (1,2,3) into the same dataset.
That mistake was not corrected until 2019, when all NCRN Landbirds database frontends switched to IDs (0,1,2).
The bird sex ID mistake was originally identified in 2021 but, since NCRN Landbirds has been chronically deprioritized, nobody corrected this problem until 2024.

Problem statement:
All years should the same set of integer IDs to indicate the same bird sex.

Approach:
The algo coded below sorts site visits based on the criteria detailed below.
Once all site visits are sorted, the birds data pipeline `src.tbl_xwalks._exception_lu_Sex()` will conditionally change the values of `ncrn.BirdDetection.Sex_Code_Value` from these unique IDs (0,1,2,3) to match this bird sex ID lookup {1:"U",2:"M",3:"F"}.
We update NCRN values to 1-index bird sex IDs (instead of 0-index) because this is the convention adopted by NETNMIDN.

Sorting criteria:
The algorithm queries all bird detections, and then sorts them into one of four categories:
1. 'uses_0_1_2'
    - This is a base case.
    - Any site visit from 2019 and later is automatically assigned to this case because (0,1,2) are the only unique IDs present during this time period.
    - If a site visit occurred in 2017 or 2018, and that site visit's bird detections used ID `0` but not use ID `3`, the algo sorts that `Event_ID` into the category of site visits that used only IDs (0,1,2).
2. 'uses_1_2_3'
    - This is a base case.
    - Any site visit from 2016 and earlier is automatically assigned to this case because (1,2,3) are the only unique IDs present during this time period.
    - If a site visit occurred in 2017 or 2018, and that site visit's bird detections used ID `1` but not use ID `0`, the algo sorts that `Event_ID` into the category of site visits that used only IDs (1,2,3).
3. 'ambiguous'
    - This is an edge case.
    - If any site visit occurred in 2017 or 2018, and didn't use 0, and used only one ID, we have no way to know whether it belongs in 'uses_0_1_2' or 'uses_1_2_3'.
    - e.g., if a site visit used only one ID, (1), we can't know whether the field crew recorded all "U"s or all "M"s, without looking at the paper datasheet.
    - This category requires manual intervention and resolution.
4. 'uses_4_codes'
    - This is an edge case that accounts for the fact that NCRN has four values in its MS Access db bird sex lookup table: {1:"U",2:"M",3:"F",4:"J"}.
    - "J" indicates fledgling birds (i.e., juveniles).
    - In 2024, "J" was not a valid bird sex choice on the field datasheet.
    - To my knowledge, "J" has never been a valid bird sex choice on NCRN Landbirds field datasheets or in the protocol.
    - Despite "J" being an invalid entry, the db included it in the lookup table so, in theory, someone could have used it.
    - If any site visit ever uses all four codes (0,1,2,3), it'd indicate that someone used code "J", which would require manual intervention and resolution.
    - Pre-2017 and post-2018 only ever used three unique codes, and, without reviewing all 2017 and 2018 datasheets, there's no way to be completely sure no birds were marked "J".
    - We know that ID `4` was never used. And we know that ID `3` was never used in the same site visit as ID `0`, so we can safely say that "J" was never used at all.

The sum of keys in each category must equal the sum of unique `Event_ID`s in the dataset.
If the sum is not equal, the sort algo has failed and must be debugged.

Once all site visits are sorted, any site visits in categories ('ambiguous', 'uses_4_codes'), must be manually sorted into ('uses_0_1_2', 'uses_1_2_3') before proceeding.
To proceed to the next step, no site visits may remain in ('ambiguous', 'uses_4_codes').

Once all site visits are in either ('uses_0_1_2', 'uses_1_2_3'), write category 'uses_0_1_2' to file, and incorporate that file into the pipeline `src.tbl_xwalks._exception_lu_Sex()`.
All site visits in 'uses_0_1_2' are to be updated given this lookup table: {2:3,1:2,0:1}.
The update must occur in order: the order shown in the lookup table to avoid overwriting values. (e.g., if you changed all `0`s to `1`s, and then changed all `1`s to `2`s, you'd also accidently change all `0`s to `2`s)
"""
import pandas as pd
import numpy as np
import datetime as dt
import data.getdf as getdf

df = getdf._get_df()

mask = (df['Date']>dt.datetime(2017,1,1)) & (df['Date']<dt.datetime(2019,1,1))
# df[mask].Sex_ID.unique()
# len(df[mask].filename.unique())
len(df[mask].Event_ID.unique()) # use `Event_ID` as key because all CAMP sites have a GRTS of 0 in source data.

# sort the site visits
mask = (df['Date']>dt.datetime(2017,1,1)) & (df['Date']<dt.datetime(2019,1,1)) # algo iteratively sorts only 2017 and 2018. All other years are base-cases discussed above.
for_review = {
    'uses_0_1_2':{}
    ,'uses_1_2_3':{}
    ,'ambiguous':{}
    ,'uses_4_codes':{}
}
counter = 0
for visit in df[mask].Event_ID.unique():
    outcome = None
    mask2 = (df['Event_ID']==visit)
    sexes = df[mask2].Sex_ID.unique()
    if len(sexes)>3:
        outcome = 'uses_4_codes'
    elif 0 in sexes and 3 not in sexes:
        outcome = 'uses_0_1_2'
    elif 1 in sexes and 0 not in sexes and len(sexes)>1:
        outcome = 'uses_1_2_3'
    else:
        outcome = 'ambiguous'
    for_review[outcome][visit] = sexes
    counter +=1
    if counter % 100 == 0:
        print(f"{counter=} of {len(df[mask].Event_ID.unique())}")

assert len(for_review['uses_0_1_2'].keys())+len(for_review['uses_1_2_3'].keys())+len(for_review['ambiguous'].keys())+len(for_review['uses_4_codes'].keys()) == len(df[mask].Event_ID.unique()), print("FATAL ERROR IN SORTING ALGORITHM") # sanity check

mask = (df['Date']>=dt.datetime(2019,1,1)) # anything 2019 and later is a base-case and should be updated from (0,1,2) to (1,2,3)
# len(df[mask].Event_ID.unique())
to_be_updated = [x for x in df[mask].Event_ID.unique()]
# len(to_be_updated)
mask = (df['Date']<=dt.datetime(2017,1,1))
assert len(df[mask].Event_ID.unique()) + len(for_review['uses_1_2_3']) + len(for_review['ambiguous']) + len(to_be_updated) == len(df.Event_ID.unique()) # sanity check, make sure that all events are accounted-for

# save a copy of the events to look up
need_to_fix = pd.DataFrame()
need_to_fix = df[df['Event_ID'].isin(for_review['ambiguous'].keys())][['event_id','Date','filename','Plot_Name','GRTS_Order']]
need_to_fix['ids'] = None
for k,v in for_review['ambiguous'].items():
    newstr = ';'.join([str(x) for x in for_review['ambiguous'][k]])
    mask = (need_to_fix['event_id']==k)
    need_to_fix['ids'] = np.where(mask, newstr, need_to_fix['ids'])
# need_to_fix.drop_duplicates('event_id').sort_values('Date')
# need_to_fix.drop_duplicates('event_id').sort_values('Date').to_csv(r'data/find_these.csv', index=False)

# go find the paper datasheets in 'data/find_these.csv' and make a decision about how to proceed

fixes = pd.read_csv(r'data/find_these.csv')
update_from_012_to_123 = to_be_updated + [x for x in for_review['uses_0_1_2'].keys()] + [x for x in fixes[fixes['result']=='used_0_1_2'].event_id.unique()]
change_2_to_1 = [x for x in fixes[fixes['result']=='change_2_to_1'].event_id.unique()]

for event in fixes.event_id.unique():
    try:
        for_review['ambiguous'].pop(event)
    except:
        print(event)

assert len(for_review['ambiguous'])==0
assert len(for_review['uses_4_codes'])==0
updates = pd.DataFrame({
    'Event_ID': update_from_012_to_123
    ,'operation': 'update_from_012_to_123'
})
updates2 = pd.DataFrame({
    'Event_ID': change_2_to_1
    ,'operation': 'change_2_to_1'
})
updates = pd.concat([updates, updates2]).reset_index(drop=True)
assert len(updates) == len(updates['Event_ID'].unique())

# only save out if there are no ambiguous codes
# if (len(for_review['ambiguous'])==0) and len(for_review['uses_4_codes'])==0:
#     updates.to_csv(r'assets\db\update_sexes.csv')
