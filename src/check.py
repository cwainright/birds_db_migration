import pandas as pd
import numpy as np
import assets.assets as assets
import re
import src.tbl_xwalks as tx
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import time
import datetime as dt

EXCLUSIONS = ['unique_vals', 'original'] # 'unique_vals` is empty when the table does not enforce unique values in any field; this can happen in reality so we ignore here
KNOWN_EMPTY = {
    'ncrn':[
        'ScannedFile'
    ]
    ,'dbo':[
        'ParkUser'
    ],'lu':[

    ]
}

def check_birds(xwalk_dict:dict) -> None:
    """Validate a dictionary of birds data

    Args:
        xwalk_dict (dict): The dictionary output by src.make_templates.make_birds().

    Returns:
        None: This function returns None.
    """
    start_time = time.time()
    print('')
    print(f'Validating dictionary against db schema...')
    _check_schema(xwalk_dict=xwalk_dict)
    print('')
    print('Checking each table for required attributes...')
    # _validate_xwalks(xwalk_dict=xwalk_dict)
    _check_attrs(xwalk_dict=xwalk_dict)
    print('')
    print('Checking the dimensions of each table...')
    _validate_loads(xwalk_dict=xwalk_dict)
    print('')
    print('Checking each table for unique values...')
    _validate_unique_vals(xwalk_dict=xwalk_dict)
    print('')
    print('Checking each table for nulls in non-nullable fields...')
    _validate_nulls(xwalk_dict=xwalk_dict)
    print('')
    print('Checking referential integrity...')
    _validate_referential_integrity(xwalk_dict=xwalk_dict)
    print('')
    print('Checking that logical keys were replaced by INTs...')
    _validate_foreign_keys(xwalk_dict=xwalk_dict)
    _validate_primary_keys(xwalk_dict=xwalk_dict)
    print('')
    end_time = time.time()
    elapsed_time = end_time - start_time
    elapsed_time = str(dt.timedelta(seconds=elapsed_time))
    elapsed_time = elapsed_time.split('.')[0]
    print(f'`check()` succeeded in: {elapsed_time}')

    return None

def _validate_foreign_keys(xwalk_dict:dict) -> None:
    # loads_to_check:list = ['tbl_load','k_load']
    loads_to_check:list = ['k_load']
    missing = {
    'counter':0
    ,'mylist':[]
    }
    # TODO: update this to check whether all values present in birds[schema][tbl][load][fk] are present in birds[lookup[0]][lookup[1]]['pk_fk_lookup'][pk]
    for schema in xwalk_dict.keys():
        for tbl in xwalk_dict[schema].keys():
            mask = (xwalk_dict[schema][tbl]['xwalk']['fk']==True) & (xwalk_dict[schema][tbl]['xwalk']['calculation']!='blank_field')
            fks = xwalk_dict[schema][tbl]['xwalk'][mask].destination.unique()
            if len(fks) >0:
                for fk in fks:
                    if fk.lower().endswith('code') or fk=='SynonymID' and schema=='ncrn' and tbl=='BirdSpecies':
                        pass
                    else:
                        for load in loads_to_check:
                            try:
                                xwalk_dict[schema][tbl][load][fk].astype(int)
                            except:
                                missing['counter'] +=1
                                missing['mylist'].append(f"birds['{schema}']['{tbl}']['{load}']['{fk}'] could not be coerced to int")

    # summarize output by table
    if missing['counter'] >0:
        print(f"WARNING: non-int foreign-keys present in `{load}`! (n): {missing['counter']}")
        for v in missing['mylist']:
            print(f'    {v}')
    else:
        for load in loads_to_check:
            print(f'SUCCESS: All primary-key/foreign-key relationships are in int format in `{load}`!')
    
    return None

def _validate_primary_keys(xwalk_dict:dict) -> None:
    # loads_to_check:list = ['tbl_load','k_load']
    loads_to_check:list = ['k_load']
    missing = {
    'counter':0
    ,'mylist':[]
    }
    for schema in xwalk_dict.keys():
        for tbl in xwalk_dict[schema].keys():
            mask = (xwalk_dict[schema][tbl]['xwalk']['pk']==True)
            pks = xwalk_dict[schema][tbl]['xwalk'][mask].destination.unique()
            if len(pks) ==1:
                for pk in pks:
                    if pk == 'Code': # when the primary key is called 'Code', we keep a str pk...
                        pass
                    else:
                        for load in loads_to_check:
                            try:
                                xwalk_dict[schema][tbl][load][pk].astype(int)
                            except:
                                missing['counter'] +=1
                                missing['mylist'].append(f"birds['{schema}']['{tbl}']['{load}']['{pk}'] could not be coerced to int")
            else:
                print(f"FAIL: multiple primary-key fields found in birds['{schema}']['{tbl}']['xwalk']")

    # summarize output by table
    if missing['counter'] >0:
        print(f"WARNING: non-int primary-keys present in `{load}`! (n): {missing['counter']}")
        for v in missing['mylist']:
            print(f'    {v}')
    else:
        for load in loads_to_check:
            print(f'SUCCESS: All primary keys are in int format in `{load}`!')
    
    return None

def _check_schema(xwalk_dict:dict) -> None:
    """Check the dictionary's table schema against the db's schema"""
    mydf = pd.read_csv(r'assets\db\db_schema.csv')
    # 'assets\db\db_schema.csv' is the result of running the below query against NCRN_Landbirds
    # USE [db_name_here]
    # GO 
    # SELECT *
    # FROM sys.Tables
    # GO
    mydf = mydf[['name', 'object_id']]
    mydf['exists'] = False
    tbls = []
    for schema in xwalk_dict.keys():
        for tbl in xwalk_dict[schema].keys():
            tbls.append(tbl)

    present_xwalk_dict_absent_db = [x for x in tbls if x not in mydf.name.unique()]
    present_db_absent_xwalk_dict = [x for x in mydf.name.unique() if x not in tbls]

    counter = 0
    for schema in xwalk_dict.keys():
        # print(f'        "{schema}": {len(xwalk_dict[schema].keys())}')
        counter += len(xwalk_dict[schema].keys())
    if len(mydf) == counter:
        print(f'SUCCESS: The dictionary contains the same count of tables as the db schema (n): {counter}')
    else:
        print('')
        print('WARNING: The dictionary and schema have different counts of tables!')
        print(f'Schema tables (n): {len(mydf.name.unique())}')
        print(f'Dictionary tables (n): {len(tbls)}')
        print(f'Tables present in dictionary but absent from db schema (n): {len(present_xwalk_dict_absent_db)}')
        for tbl in present_xwalk_dict_absent_db:
            print(f'    {tbl}')
        print(f'Tables present in db but absent from dictionary (n): {len(present_db_absent_xwalk_dict)}')
        for tbl in present_db_absent_xwalk_dict:
            print(f'    {tbl}')
    if len(present_xwalk_dict_absent_db) == 0 and len(present_db_absent_xwalk_dict) == 0:
        print(f'SUCCESS: The dictionary contains the same table names as the db schema')

    return None

def _validate_payload(xwalk_dict:dict) -> dict:
    # TODO: this function should validate `payload`
    # TODO: All `payload`s should have >0 columns; if there are no columns, payload-generation failed.
    return xwalk_dict

def _validate_loads(xwalk_dict:dict) -> None:
    """Check that the load attrs produced from each `source` and `xwalk` is valid"""
    mykeys = _traverse(xwalk_dict, EXCLUSIONS)

    _validate_dims(xwalk_dict, mykeys, 'tbl_load')
    # _validate_dims(xwalk_dict, mykeys, 'k_load')

    return None

def _validate_dims(xwalk_dict:dict, mykeys:list, target:str) -> None:
    """Check the dimensions of each `tbl_load`"""

    _validate_rows(xwalk_dict, mykeys, target)
    _validate_cols(xwalk_dict, mykeys, target)

    return None

def _validate_rows(xwalk_dict:dict, mykeys:list, target:str) -> None:
    """Check that the `tbl_load` has the correct number of rows"""
    missing = {}
    for k in mykeys:
        missing[k] = {
            'counter':0
            ,'mylist':[]
        }
    missing_vals = {}
    for schema in xwalk_dict.keys():
        missing_vals[schema] = {}
        for tbl in xwalk_dict[schema].keys():
            missing_vals[schema][tbl] = []
            for k in xwalk_dict[schema][tbl].keys():
                if k not in EXCLUSIONS:
                    if k == 'destination':
                        if len(xwalk_dict[schema][tbl]['source']) != len(xwalk_dict[schema][tbl][target]):
                            missing[k]['counter'] +=1
                            missing[k]['mylist'].append(f"birds['{schema}']['{tbl}']['{k}']")
                            missing_vals[schema][tbl].append(f"birds['{schema}']['{tbl}']['{k}']")
    tbls_missing = {
        'tbl_missing':[]
        ,'tables':{}
    }
    for schema in missing_vals.keys():
        for tbl in missing_vals[schema].keys():
            counter = 0
            if len(missing_vals[schema][tbl]) >0:
                counter += 1
                for attr in missing_vals[schema][tbl]:
                    tbls_missing['tbl_missing'].append(attr)
    successes = []
    for k in missing:
        if missing[k]['counter'] >0:
            print(f"WARNING: The count of rows is wrong in {missing[k]['counter']} tables!")
            for v in missing[k]['mylist']:
                print(f'    {v}')
        else:
            successes.append(k)
    if len(successes)==len(mykeys):
        print(f'SUCCESS: The count of rows for each `{target}` matches its `source`!')

    return None

def _validate_cols(xwalk_dict:dict, mykeys:list, target:str) -> None:
    """Check that the `tbl_load` has the correct columns"""
    # check that each column in `tbl_load` exists in `destination`
    missing = {}
    for k in mykeys:
        missing[k] = {
            'counter':0
            ,'mylist':[]
        }
    missing_vals = {}
    for schema in xwalk_dict.keys():
        missing_vals[schema] = {}
        for tbl in xwalk_dict[schema].keys():
            missing_vals[schema][tbl] = []
            for k in xwalk_dict[schema][tbl].keys():
                if k not in EXCLUSIONS:
                    if k == 'destination':
                        for col in xwalk_dict[schema][tbl][target].columns:
                            if col != 'rowid' and col not in xwalk_dict[schema][tbl]['destination'].columns:
                                missing[k]['counter'] +=1
                                missing[k]['mylist'].append(f"birds['{schema}']['{tbl}']['{k}']")
                                missing_vals[schema][tbl].append(f"birds['{schema}']['{tbl}']['{k}']")
    tbls_missing = {
        'tbl_missing':[]
        ,'tables':{}
    }
    for schema in missing_vals.keys():
        for tbl in missing_vals[schema].keys():
            counter = 0
            if len(missing_vals[schema][tbl]) >0:
                counter += 1
                for attr in missing_vals[schema][tbl]:
                    tbls_missing['tbl_missing'].append(attr)
    # summarize output by attribute
    successes = []
    for k in missing:
        if missing[k]['counter'] >0:
            print(f"WARNING: The count of columns is wrong in {missing[k]['counter']} tables!")
            for v in missing[k]['mylist']:
                print(f'    {v}')
        else:
            successes.append(k)
    if len(successes)==len(mykeys):
        print(f'SUCCESS: The count of columns for each `{target}` matches its `destination`!')
    
    # check that the column order in `tbl_load` matches that of `destination`
    missing = {}
    for k in mykeys:
        missing[k] = {
            'counter':0
            ,'mylist':[]
        }
    missing_vals = {}
    for schema in xwalk_dict.keys():
        missing_vals[schema] = {}
        for tbl in xwalk_dict[schema].keys():
            missing_vals[schema][tbl] = []
            for k in xwalk_dict[schema][tbl].keys():
                if k not in EXCLUSIONS:
                    if k == 'destination':
                        dest_cols = list(xwalk_dict[schema][tbl]['destination'].columns)
                        tbl_load_cols = xwalk_dict[schema][tbl]['tbl_load'].columns
                        tbl_load_cols = [x for x in tbl_load_cols if x != 'rowid']
                        checks = []
                        for i in range(len(dest_cols)):
                            checks.append(dest_cols[i] == tbl_load_cols[i])
                        if all(checks)==False:
                            missing[k]['counter'] +=1
                            missing[k]['mylist'].append(f"birds['{schema}']['{tbl}']['{k}']")
                            missing_vals[schema][tbl].append(f"birds['{schema}']['{tbl}']['{k}']")
    tbls_missing = {
        'tbl_missing':[]
        ,'tables':{}
    }
    for schema in missing_vals.keys():
        for tbl in missing_vals[schema].keys():
            counter = 0
            if len(missing_vals[schema][tbl]) >0:
                counter += 1
                for attr in missing_vals[schema][tbl]:
                    tbls_missing['tbl_missing'].append(attr)
    # summarize output by attribute
    successes = []
    for k in missing:
        if missing[k]['counter'] >0:
            print(f"WARNING: The column-order in `{target}` does not match that of `destination` in {missing[k]['counter']} tables!")
            for v in missing[k]['mylist']:
                print(f'    {v}')
        else:
            successes.append(k)
    if len(successes)==len(mykeys):
        print(f'SUCCESS: The column-order in `{target}` matches that of `destination`!')

    return None

def _validate_referential_integrity(xwalk_dict:dict) -> None:
    """Check for null values in non-nullable fields"""
    loads_to_check:list = ['k_load']
    # loads_to_check:list = ['tbl_load']
    missing = {
    'counter':0
    ,'mylist':[]
    }
    for schema in xwalk_dict.keys():
        for tbl in xwalk_dict[schema].keys():
            mask = (xwalk_dict[schema][tbl]['xwalk']['fk']==True) & (xwalk_dict[schema][tbl]['xwalk']['calculation']!='blank_field')
            fks = xwalk_dict[schema][tbl]['xwalk'][mask].destination.unique()
            if len(fks) >0:
                for fk in fks:
                    for load in loads_to_check:
                        try:
                            constrained_by = xwalk_dict[schema][tbl]['xwalk'][xwalk_dict[schema][tbl]['xwalk']['destination']==fk].references.values[0]
                            lookup = constrained_by.split('.')
                            if len(lookup) != 3:
                                print(f"FAIL: check referential integrity, lookup error: birds['{schema}']['{tbl}']['xwalk'].destination=='{fk}'; ['references'] is broken")
                            else:
                                ref = xwalk_dict[lookup[0]][lookup[1]][load][lookup[2]]
                                present_load_absent_lookup = [x for x in xwalk_dict[schema][tbl][load][fk].unique() if x not in ref.unique()]
                                if len(present_load_absent_lookup) >0:
                                    if all(i != i for i in present_load_absent_lookup) and all(xwalk_dict[schema][tbl]['xwalk'][xwalk_dict[schema][tbl]['xwalk']['destination']==fk].can_be_null.unique()): # if NaN is the only value present in the column but absent from the lookup and the field is nullable
                                        pass
                                    else:
                                        missing['counter'] +=1
                                        missing['mylist'].append(f"birds['{schema}']['{tbl}']['{load}']['{fk}']: {len(present_load_absent_lookup)} not in {constrained_by}")
                                        missing['mylist'].append(f"    [x for x in birds['{schema}']['{tbl}']['{load}']['{fk}'].unique() if x not in birds['{lookup[0]}']['{lookup[1]}']['{load}']['{lookup[2]}'].unique()]")
                                else:
                                    continue
                        except:
                            print(f"FAIL: check referential integrity: birds['{schema}']['{tbl}']['{load}']['{fk}']")

    # summarize output by table
    if missing['counter'] >0:
        print(f"WARNING: broken primary-key/foreign-key references present! (n): {missing['counter']}")
        for v in missing['mylist']:
            print(f'    {v}')
    else:
        for load in loads_to_check:
            print(f'SUCCESS: All foreign keys in `{load}` exist as primary keys in their related table!')
    
    return None

def _validate_unique_vals(xwalk_dict:dict) -> None:
    """Check for duplicate values in required-unique fields"""
    loads_to_check:list = ['k_load']
    # loads_to_check:list = ['tbl_load']
    missing = {
    'counter':0
    ,'mylist':[]
    }
    for schema in xwalk_dict.keys():
        for tbl in xwalk_dict[schema].keys():
            if len(xwalk_dict[schema][tbl]['unique_vals']) > 0:
                for load in loads_to_check:
                    tmp = xwalk_dict[schema][tbl][load].copy()
                    for val in xwalk_dict[schema][tbl]['unique_vals']:
                        unique_vals =val.split(',')
                        unique_vals = [f"tmp['{x}'].astype(str).str.lower()" for x in unique_vals]
                        unique_vals = '+"_"+'.join(unique_vals)
                        mycode = "tmp['dummy'] = " + unique_vals
                        try:
                            exec(mycode)
                        except:
                            print(f'FAIL: {mycode}')
                            print(f"FAIL: birds['{schema}']['{tbl}']['{load}']")
                        if len(tmp.dummy.unique()) != len(tmp):
                            missing['counter'] +=1
                            missing['mylist'].append(f"birds['{schema}']['{tbl}']['{load}']: {mycode}")
    # summarize output by table
    if missing['counter'] >0:
        print(f"WARNING: required-unique fields contain duplicate values! (n): {missing['counter']}")
        for v in missing['mylist']:
            print(f'    {v}')
            if v == "birds['ncrn']['DetectionEvent']['tbl_load']: tmp['dummy'] = tmp['LocationID'].astype(str).str.lower()+tmp['StartDateTime'].astype(str).str.lower()+tmp['ProtocolID'].astype(str).str.lower()":
                outcomes = tx._find_dupe_site_visits(xwalk_dict)
                for k in outcomes['review'].keys():
                    print(f"        DUPLICATE EVENT: {outcomes['review'][k]}")
    else:
        for load in loads_to_check:
            print(f'SUCCESS: All required-unique fields have only unique values in {load}!')

    return None

def _validate_xwalks(xwalk_dict:dict) -> None:
    """Check that the `xwalk` attr produced for each `tbl_load` is valid"""
    mykeys = ['xwalk']
    missing = {}
    for k in mykeys:
        missing[k] = {
            'counter':0
            ,'mylist':[]
        }
    missing_vals = {}
    for schema in xwalk_dict.keys():
        missing_vals[schema] = {}
        for tbl in xwalk_dict[schema].keys():
            missing_vals[schema][tbl] = {}
            for k in mykeys:
                missing_vals[schema][tbl][k] = {}
                mask = (xwalk_dict[schema][tbl]['xwalk'].source.isna()) | (xwalk_dict[schema][tbl]['xwalk'].source == 'tbd') | (xwalk_dict[schema][tbl]['xwalk'].source == 'placeholder')
                mysub = xwalk_dict[schema][tbl]['xwalk'][mask]
                if len(mysub) >0 or len(xwalk_dict[schema][tbl]['xwalk'])==0:
                    if tbl not in KNOWN_EMPTY[schema].keys():
                        missing[k]['counter'] +=1
                        missing[k]['mylist'].append(f"birds['{schema}']['{tbl}']['{k}']")
    tbls_missing = {
        'tbl_missing':[]
        ,'tables':{}
    }
    for schema in missing_vals.keys():
        for tbl in missing_vals[schema].keys():
            counter = 0
            if len(missing_vals[schema][tbl]) >0:
                counter += 1
                for attr in missing_vals[schema][tbl]:
                    tbls_missing['tbl_missing'].append(attr)
    # summarize output by attribute
    successes = []
    for k in missing:
        if missing[k]['counter'] >0:
            print(f"WARNING: Attribute `{k}` has empty slots in {missing[k]['counter']} tables!")
            for v in missing[k]['mylist']:
                print(f'    {v}')
        else:
            successes.append(k)
    if len(successes)==len(mykeys):
        print('SUCCESS: All `xwalks` are complete!')

    return None

def _traverse(myd:dict, EXCLUSIONS:list) -> list:
    """Recursively check levels of input dictionary until we get the right depth; allows for schema addition"""
    mykeys = []
    def __traverse(myd):
        if isinstance(myd, dict):
            if 'source' in myd.keys():
                for k in list(myd.keys()):
                    if k not in mykeys:
                        mykeys.append(k)
            else:
                for k in myd:
                    __traverse(myd[k])
        else:
            pass
    __traverse(myd)
    mykeys = [x for x in mykeys if x not in EXCLUSIONS]

    return mykeys

def _check_attrs(xwalk_dict:dict) -> None:

    mykeys = _traverse(xwalk_dict, EXCLUSIONS)

    missing = {}
    for k in mykeys:
        missing[k] = {
            'counter':0
            ,'mylist':[]
        }
    missing_vals = {}
    for schema in xwalk_dict.keys():
        missing_vals[schema] = {}
        for tbl in xwalk_dict[schema].keys():
            missing_vals[schema][tbl] = []
            for k in xwalk_dict[schema][tbl].keys():
                if k not in EXCLUSIONS:
                    if k == 'destination':
                        if len(xwalk_dict[schema][tbl][k].columns) == 0:
                            if tbl not in KNOWN_EMPTY[schema]:
                                missing[k]['counter'] +=1
                                missing[k]['mylist'].append(f"birds['{schema}']['{tbl}']['{k}']")
                                missing_vals[schema][tbl].append(f"birds['{schema}']['{tbl}']['{k}']")
                    else:
                        if len(xwalk_dict[schema][tbl][k]) == 0:
                            if tbl not in KNOWN_EMPTY[schema]:
                                missing[k]['counter'] +=1
                                missing[k]['mylist'].append(f"birds['{schema}']['{tbl}']['{k}']")
                                missing_vals[schema][tbl].append(f"birds['{schema}']['{tbl}']['{k}']")

    tbls_missing = {
        'tbl_missing':[]
        ,'tables':{}
    }
    for schema in missing_vals.keys():
        for tbl in missing_vals[schema].keys():
            counter = 0
            if len(missing_vals[schema][tbl]) >0:
                counter += 1
                for attr in missing_vals[schema][tbl]:
                    tbls_missing['tbl_missing'].append(attr)
    # summarize output by attribute
    successes = []
    for k in missing:
        if missing[k]['counter'] >0:
            print(f"WARNING: Attribute `{k}` is empty in {missing[k]['counter']} tables!")
            for v in missing[k]['mylist']:
                print(f'    {v}')
        else:
            successes.append(k)
    if len(successes)==len(mykeys):
        # print('SUCCESS: All required attributes exist in all tables!')
        for k in mykeys:
            print(f'SUCCESS: Required attribute `{k}` is complete in all tables!')
    # summarize output by table
    # if len(tbls_missing['tbl_missing']) >0:
    #     print(f"WARNING: {len(tbls_missing['tbl_missing'])} tables have at least one empty attribute!")
    #     for schema in missing_vals.keys():
    #         for tbl in missing_vals[schema]:
    #             if len(missing_vals[schema][tbl])>0:
    #                 # print(f"    dict['{schema}']['{tbl}'] is missing {len(missing_vals[schema][tbl])} attribute(s):")
    #                 print(f"    dict['{schema}']['{tbl}']")
    #                 for attr in missing_vals[schema][tbl]:
    #                     # print(f'        {attr}')
    #                     attr = attr.replace(f"birds['{schema}']['{tbl}']", '')
    #                     attr = attr.replace("]", '')
    #                     attr = attr.replace("[", '')
    #                     print(f'        {attr}')
    # else:
    #     print('SUCCESS: All tables have all required attributes!')

    return None

def _validate_nulls(xwalk_dict:dict) -> None:
    """Check for null values in non-nullable fields"""
    loads_to_check:list = ['tbl_load','k_load']
    # loads_to_check:list = ['tbl_load']
    missing = {
    'counter':0
    ,'mylist':[]
    }
    for schema in xwalk_dict.keys():
        for tbl in xwalk_dict[schema].keys():
            mask = (xwalk_dict[schema][tbl]['xwalk']['can_be_null']==False) & (xwalk_dict[schema][tbl]['xwalk']['calculation']!='blank_field')
            non_nullables = xwalk_dict[schema][tbl]['xwalk'][mask].destination.unique()
            for col in non_nullables:
                for load in loads_to_check:
                    if len(xwalk_dict[schema][tbl][load]) >0:
                        tmp = xwalk_dict[schema][tbl][load][xwalk_dict[schema][tbl][load][col].isna()]
                        if len(tmp) >0:
                            missing['counter'] +=1
                            missing['mylist'].append(f"birds['{schema}']['{tbl}']['{load}']['{col}']: {len(tmp)} NULLs")
                    else:
                        pass
    # summarize output by table
    if missing['counter'] >0:
        print(f"WARNING: null values present in non-nullable fields! (n): {missing['counter']}")
        for v in missing['mylist']:
            print(f'    {v}')
    else:
        for load in loads_to_check:
            print(f'SUCCESS: All non-nullable fields have values in {load}!')

    return None
def unit_test(xwalk_dict:dict, n:int=None, verbose:bool=False) -> None:

    """
    Unit test to confirm that pk-fk relationships return the exact records in `k_load` that were present in `source`

    **kwargs include:
        `n` (int): the count of pivot tables you'd like to compare
        `verbose` (bool): True tells the program to print up to 5 pivot tables to console for visual inspection

    Flatten each iteration of the dataset into the format needed for contractor-loading
    Make a flattened dataset out of the `source`, `tbl_load`, and `k_load`
    # in theory, all of the datasets from `source` forward should have the same number of rows

    Compare the datasets
    - counts of records per date
    - counts of records per site
    - counts of records per bird species
    - counts of records per observer, recorder
    - pivot, compare counts of records (like in summary sent to collaborator)
    """
    start_time = time.time()
    outcomes = []
    print('')
    print('Unit testing `ncrn` schema...')

    print('')
    print(f'Unit testing `ncrn.DetectionEvent.k_load`...')
    outcomes.extend(_unit_test_ncrn_DetectionEvent(xwalk_dict))
    print('')
    print(f'Unit testing `ncrn.BirdDetection.k_load`...')
    outcomes.extend(_unit_test_ncrn_BirdDetection(xwalk_dict))
    if n:
        print('')
        print(f'Comparing a random sample of {n} site visits for accuracy...')
        outcomes.extend(_compare_pivot_tables(xwalk_dict, n, verbose))
    
    if all(outcomes):
        print('')
        print(f'SUCCESS: `ncrn` passed all unit tests!')
    # 3. compare the dates
    end_time = time.time()
    elapsed_time = end_time - start_time
    elapsed_time = str(dt.timedelta(seconds=elapsed_time))
    elapsed_time = elapsed_time.split('.')[0]
    print('')
    print(f'`unit_test()` succeeded in: {elapsed_time}')
    return None

def _unit_test_ncrn_DetectionEvent(xwalk_dict:dict) -> list:
    outcomes = []

    df_s, df_k = _make_flat_DetectionEvent_k_load(xwalk_dict)
    outcomes.extend(_unit_test_ncrn_DetectionEvent_LocationID(df_s, df_k))
    outcomes.append(_unit_test_ncrn_DetectionEvent_ProtocolID(df_s, df_k))
    outcomes.append(_unit_test_ncrn_DetectionEvent_EnteredBy(df_s, df_k))
    outcomes.append(_unit_test_ncrn_DetectionEvent_Observer_ContactID(df_s, df_k))
    outcomes.append(_unit_test_ncrn_DetectionEvent_Recorder_ContactID(df_s, df_k))
    outcomes.append(_unit_test_ncrn_DetectionEvent_ProtocolNoiseLevelID(df_s, df_k))
    outcomes.append(_unit_test_ncrn_DetectionEvent_ProtocolWindCodeID(df_s, df_k))
    outcomes.append(_unit_test_ncrn_DetectionEvent_ProtocolPrecipitationTypeID(df_s, df_k))
    outcomes.append(_unit_test_ncrn_DetectionEvent_AirTemperature(df_s, df_k))
    outcomes.append(_unit_test_ncrn_DetectionEvent_StartDateTime(df_s, df_k))
    outcomes.append(_unit_test_ncrn_DetectionEvent_EnteredDateTime(df_s, df_k))
    outcomes.append(_unit_test_ncrn_DetectionEvent_DataProcessingLevelID(df_s, df_k))
    outcomes.append(_unit_test_ncrn_DetectionEvent_DataProcessingLevelDate(df_s, df_k))
    outcomes.append(_unit_test_ncrn_DetectionEvent_RelativeHumidity(df_s, df_k))

    return outcomes

def _unit_test_ncrn_BirdDetection(xwalk_dict:dict) -> list:
    outcomes = []

    df_s, df_k = _make_flat_BirdDetection_k_load(xwalk_dict)
    outcomes.append(_unit_test_BirdDetection_Code(df_s, df_k))
    outcomes.append(_unit_test_BirdDetection_CommonName(df_s, df_k))
    outcomes.append(_unit_test_BirdDetection_ScientificName(df_s, df_k))
    outcomes.append(_unit_test_BirdDetection_groupby(df_s, df_k))

    return outcomes

def _make_flat_DetectionEvent_k_load(xwalk_dict:dict) -> tuple[pd.DataFrame, pd.DataFrame]:

    # flatten the `source` dataset; left-join attributes to fact table
    contact_s = xwalk_dict['ncrn']['Contact']['source'].copy()
    contact_s = contact_s[['Contact_ID','Last_Name','First_Name']]
    location_s = xwalk_dict['ncrn']['Location']['source'].copy()
    location_s = location_s[['Location_ID','Site_ID','Unit_Code', 'Plot_Name']]
    site_s = xwalk_dict['ncrn']['Site']['source'].copy()
    site_s = site_s[['Site_ID', 'Site_Name']]
    location_s = location_s.merge(site_s, on='Site_ID', how='left')
    events_s = xwalk_dict['ncrn']['DetectionEvent']['source'].copy()
    df_s = events_s.merge(location_s, left_on='location_id', right_on='Location_ID', how='left')
    df_s = df_s.merge(contact_s, left_on='observer', right_on='Contact_ID', how='left')
    df_s.rename(columns={'Last_Name':'observer_Last_Name','First_Name':'observer_First_Name'},inplace=True)
    df_s = df_s.merge(contact_s, left_on='recorder', right_on='Contact_ID', how='left')
    df_s.rename(columns={'Last_Name':'recorder_Last_Name','First_Name':'recorder_First_Name'},inplace=True)

    # flatten the `k_load` dataset; left-join attributes to fact table
    contact_k = xwalk_dict['ncrn']['Contact']['k_load'].copy()
    contact_k = contact_k[['ID','LastName','FirstName']]
    location_k = xwalk_dict['ncrn']['Location']['k_load'].copy()
    location_k = location_k[['ID','SiteID','Label']]
    location_k.rename(columns={'Label':'location_Label'}, inplace=True)
    site_k = xwalk_dict['ncrn']['Site']['k_load'].copy()
    site_k = site_k[['ID', 'Label']]
    site_k.rename(columns={'Label':'site_Label'}, inplace=True)
    location_k = location_k.merge(site_k, left_on='SiteID', right_on='ID', how='left')
    events_k = xwalk_dict['ncrn']['DetectionEvent']['k_load'].copy()
    df_k = events_k.merge(location_k, left_on='LocationID', right_on='ID_x', how='left')
    df_k = df_k.merge(contact_k, left_on='Observer_ContactID', right_on='ID', how='left')
    df_k.rename(columns={'LastName':'observer_LastName','FirstName':'observer_FirstName'},inplace=True)
    df_k = df_k.merge(contact_k, left_on='Recorder_ContactID', right_on='ID', how='left')
    df_k.rename(columns={'LastName':'recorder_LastName','FirstName':'recorder_FirstName'},inplace=True)

    return df_s, df_k

def _unit_test_ncrn_DetectionEvent_Observer_ContactID(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:

    outcome = all(df_s['observer_Last_Name']==df_k['observer_LastName'])
    if all(df_s['observer_Last_Name']==df_k['observer_LastName']):
        print(f'    SUCCESS: The observer first and last name in `source` matched that of `k_load` for `birds.ncrn.DetectionEvent`!')
    else:
        print(f'    FAIL: The observer first and last name in `source` DID NOT match that of `k_load` for `birds.ncrn.DetectionEvent`!')

    return outcome

def _unit_test_ncrn_DetectionEvent_ProtocolWindCodeID(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:

    outcome = all(df_s['wind_speed']==df_k['ProtocolWindCodeID'])
    if all(df_s['wind_speed']==df_k['ProtocolWindCodeID']):
        print(f'    SUCCESS: The wind speed in `source` matched that of `k_load` for `birds.ncrn.DetectionEvent`!')
    else:
        print(f'    FAIL: The wind speed in `source` DID NOT match that of `k_load` for `birds.ncrn.DetectionEvent`!')

    return outcome

def _unit_test_ncrn_DetectionEvent_ProtocolNoiseLevelID(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:

    outcome = all(df_s['disturbance_level']==df_k['ProtocolNoiseLevelID'])
    if all(df_s['disturbance_level']==df_k['ProtocolNoiseLevelID']):
        print(f'    SUCCESS: The ProtocolNoiseLevelID in `source` matched that of `k_load` for `birds.ncrn.DetectionEvent`!')
    else:
        print(f'    FAIL: The ProtocolNoiseLevelID in `source` DID NOT match that of `k_load` for `birds.ncrn.DetectionEvent`!')

    return outcome

def _unit_test_ncrn_DetectionEvent_AirTemperature(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:

    mask_s = (df_s['temperature'].isna())
    mask_k = (df_k['AirTemperature'].isna())
    na_s = df_s[mask_s].copy().reset_index(drop=True)
    na_k = df_k[mask_k].copy().reset_index(drop=True)
    non_na_s = df_s[~mask_s].copy().reset_index(drop=True)
    non_na_k = df_k[~mask_k].copy().reset_index(drop=True)
    
    outcome = all(non_na_s['temperature']==non_na_k['AirTemperature']) and len(na_s) == len(na_k)

    if outcome==True:
        print(f'    SUCCESS: The AirTemperature in `source` matched that of `k_load` for `birds.ncrn.DetectionEvent`!')
    else:
        print(f'    FAIL: The AirTemperature in `source` DID NOT match that of `k_load` for `birds.ncrn.DetectionEvent`!')

    return outcome

def _unit_test_ncrn_DetectionEvent_EnteredBy(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:

    outcome = all(df_s['entered_by']==df_k['EnteredBy'])
    if all(df_s['entered_by']==df_k['EnteredBy']):
        print(f'    SUCCESS: The EnteredBy first and last name in `source` matched that of `k_load` for `birds.ncrn.DetectionEvent`!')
    else:
        print(f'    FAIL: The EnteredBy first and last name in `source` DID NOT match that of `k_load` for `birds.ncrn.DetectionEvent`!')

    return outcome

def _unit_test_ncrn_DetectionEvent_ProtocolID(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:

    outcome = all(df_s['protocol_id']==df_k['ProtocolID'])
    if all(df_s['protocol_id']==df_k['ProtocolID']):
        print(f'    SUCCESS: The ProtocolID in `source` matched that of `k_load` for `birds.ncrn.DetectionEvent`!')
    else:
        print(f'    FAIL: The ProtocolID in `source` DID NOT match that of `k_load` for `birds.ncrn.DetectionEvent`!')

    return outcome

def _unit_test_ncrn_DetectionEvent_ProtocolPrecipitationTypeID(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:

    outcome = all(df_s['sky_condition']==df_k['ProtocolPrecipitationTypeID'])
    if all(df_s['sky_condition']==df_k['ProtocolPrecipitationTypeID']):
        print(f'    SUCCESS: The ProtocolPrecipitationTypeID in `source` matched that of `k_load` for `birds.ncrn.DetectionEvent`!')
    else:
        print(f'    FAIL: The ProtocolPrecipitationTypeID in `source` DID NOT match that of `k_load` for `birds.ncrn.DetectionEvent`!')

    return outcome

def _unit_test_ncrn_DetectionEvent_EnteredDateTime(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:

    outcome = all(df_s['entered_date']==df_k['EnteredDateTime'])
    if all(df_s['entered_date']==df_k['EnteredDateTime']):
        print(f'    SUCCESS: The EnteredDateTime in `source` matched that of `k_load` for `birds.ncrn.DetectionEvent`!')
    else:
        print(f'    FAIL: The EnteredDateTime in `source` DID NOT match that of `k_load` for `birds.ncrn.DetectionEvent`!')

    return outcome

def _unit_test_ncrn_DetectionEvent_RelativeHumidity(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:
    mask_s = (df_s['humidity'].isna())
    mask_k = (df_k['RelativeHumidity'].isna())
    na_s = df_s[mask_s].copy().reset_index(drop=True)
    na_k = df_k[mask_k].copy().reset_index(drop=True)
    non_na_s = df_s[~mask_s].copy().reset_index(drop=True)
    non_na_k = df_k[~mask_k].copy().reset_index(drop=True)
    
    outcome = all(non_na_s['humidity']==non_na_k['RelativeHumidity']) and len(na_s) == len(na_k)
    if outcome==True:
        print(f'    SUCCESS: The RelativeHumidity in `source` matched that of `k_load` for `birds.ncrn.DetectionEvent`!')
    else:
        print(f'    FAIL: The RelativeHumidity in `source` DID NOT match that of `k_load` for `birds.ncrn.DetectionEvent`!')

    return outcome

def _unit_test_ncrn_DetectionEvent_DataProcessingLevelID(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:

    outcome = all(df_s['dataprocessinglevelid']==df_k['DataProcessingLevelID'])
    if all(df_s['dataprocessinglevelid']==df_k['DataProcessingLevelID']):
        print(f'    SUCCESS: The DataProcessingLevelID in `source` matched that of `k_load` for `birds.ncrn.DetectionEvent`!')
    else:
        print(f'    FAIL: The DataProcessingLevelID in `source` DID NOT match that of `k_load` for `birds.ncrn.DetectionEvent`!')

    return outcome

def _unit_test_ncrn_DetectionEvent_StartDateTime(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:

    outcome = all(df_s['activity_start_datetime']==df_k['StartDateTime'])
    if all(df_s['activity_start_datetime']==df_k['StartDateTime']):
        print(f'    SUCCESS: The StartDateTime in `source` matched that of `k_load` for `birds.ncrn.DetectionEvent`!')
    else:
        print(f'    FAIL: The StartDateTime in `source` DID NOT match that of `k_load` for `birds.ncrn.DetectionEvent`!')

    return outcome

def _unit_test_ncrn_DetectionEvent_DataProcessingLevelDate(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:

    mask_s = (df_s['dataprocessingleveldate'].isna())
    mask_k = (df_k['DataProcessingLevelDate'].isna())
    na_s = df_s[mask_s].copy().reset_index(drop=True)
    na_k = df_k[mask_k].copy().reset_index(drop=True)
    non_na_s = df_s[~mask_s].copy().reset_index(drop=True)
    non_na_k = df_k[~mask_k].copy().reset_index(drop=True)
    
    outcome = all(non_na_s['dataprocessingleveldate']==non_na_k['DataProcessingLevelDate']) and len(na_s) == len(na_k)

    if outcome==True:
        print(f'    SUCCESS: The DataProcessingLevelDate in `source` matched that of `k_load` for `birds.ncrn.DetectionEvent`!')
    else:
        print(f'    FAIL: The DataProcessingLevelDate in `source` DID NOT match that of `k_load` for `birds.ncrn.DetectionEvent`!')

    return outcome

def _unit_test_ncrn_DetectionEvent_LocationID(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:
    # TODO: compare the ID and the attributes (name, lat/lon) need to join those in `_make_flat_DetectionEvent_k_load()`

    outcomes = []
    outcomes.append(all(df_s['Plot_Name']==df_k['location_Label']))
    outcomes.append(all(df_s['Site_Name']==df_k['site_Label']))

    if all(df_s['Plot_Name']==df_k['location_Label']):
        print(f'    SUCCESS: The Plot_Name in `source` matched that of `k_load` for `birds.ncrn.DetectionEvent`!')
    else:
        print(f'    FAIL: The Plot_Name in `source` DID NOT match that of `k_load` for `birds.ncrn.DetectionEvent`!')
    if all(df_s['Site_Name']==df_k['site_Label']):
        print(f'    SUCCESS: The Site_Name in `source` matched that of `k_load` for `birds.ncrn.DetectionEvent`!')
    else:
        print(f'    FAIL: The Site_Name in `source` DID NOT match that of `k_load` for `birds.ncrn.DetectionEvent`!')

    return outcomes

def _unit_test_ncrn_DetectionEvent_Recorder_ContactID(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:

    outcome = all(df_s['recorder_Last_Name']==df_k['recorder_LastName'])
    if all(df_s['recorder_Last_Name']==df_k['recorder_LastName']):
        print(f'    SUCCESS: The recorder first and last name in `source` matched that of `k_load` for `birds.ncrn.DetectionEvent`!')
    else:
        print(f'    FAIL: The recorder first and last name in `source` DID NOT match that of `k_load` for `birds.ncrn.DetectionEvent`!')

    return outcome

def _unit_test_BirdDetection_Code(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:

    outcome = all(df_s['Code']==df_k['Code'])

    # groupby `DetectionEventID` and count individuals (i.e., len(subset))
    if all(df_s['Code']==df_k['Code']):
        print(f'    SUCCESS: The array of species codes in `source` matched that of `k_load` for `birds.ncrn.BirdDetection`!')
    else:
        print(f'    FAIL: The array of species codes in `source` DID NOT match that of `k_load` for `birds.ncrn.BirdDetection`!')
    return outcome

def _unit_test_BirdDetection_groupby(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:

    outcome = all(df_s.groupby(['DetectionEventID']).size().reset_index(name='count').sort_values(['count'], ascending=False)['count']==df_k.groupby(['DetectionEventID']).size().reset_index(name='count').sort_values(['count'], ascending=False)['count'])

    if all(df_s.groupby(['DetectionEventID']).size().reset_index(name='count').sort_values(['count'], ascending=False)['count']==df_k.groupby(['DetectionEventID']).size().reset_index(name='count').sort_values(['count'], ascending=False)['count']):
        print(f'    SUCCESS: All species observations in `source` matched `k_load` for `birds.ncrn.BirdDetection`!')
    else:
        print(f'    FAIL: All species observations in `source` DID NOT match `k_load` for `birds.ncrn.BirdDetection`!')

    return outcome

def _make_flat_BirdDetection_k_load(xwalk_dict:dict) -> tuple[pd.DataFrame, pd.DataFrame]:

    # 2. compare the species names
    species_k = xwalk_dict['ncrn']['BirdSpecies']['k_load'].copy()
    species_k = species_k[['ID','Code','CommonName','ScientificName']]
    speciespark_k = xwalk_dict['ncrn']['BirdSpeciesPark']['k_load'].copy()
    speciespark_k = speciespark_k[['ID','BirdSpeciesID','ParkID']]
    park_k = xwalk_dict['ncrn']['Park']['k_load'].copy()
    park_k = park_k[['ID','ParkCode']]
    detection_k = xwalk_dict['ncrn']['BirdDetection']['k_load'].copy()
    detection_k = detection_k[['ID','DetectionEventID','BirdSpeciesParkID']]
    detection_s = xwalk_dict['ncrn']['BirdDetection']['source'].copy()
    detection_s = detection_k[['ID','DetectionEventID','BirdSpeciesParkID']]
    tmp = speciespark_k.merge(species_k, left_on='BirdSpeciesID', right_on='ID', how='left')
    tmp = tmp.merge(park_k, left_on='ParkID', right_on='ID', how='left')
    df_s = detection_s.merge(tmp, left_on='BirdSpeciesParkID', right_on='ID_x')

    species_k = xwalk_dict['ncrn']['BirdSpecies']['k_load'].copy()
    species_k = species_k[['ID','Code','CommonName','ScientificName']]
    speciespark_k = xwalk_dict['ncrn']['BirdSpeciesPark']['k_load'].copy()
    speciespark_k = speciespark_k[['ID','BirdSpeciesID','ParkID']]
    park_k = xwalk_dict['ncrn']['Park']['k_load'].copy()
    park_k = park_k[['ID','ParkCode']]
    detection_k = xwalk_dict['ncrn']['BirdDetection']['k_load'].copy()
    detection_k = detection_k[['ID','DetectionEventID','BirdSpeciesParkID']]
    tmp = speciespark_k.merge(species_k, left_on='BirdSpeciesID', right_on='ID', how='left')
    tmp = tmp.merge(park_k, left_on='ParkID', right_on='ID', how='left')
    df_k = detection_k.merge(tmp, left_on='BirdSpeciesParkID', right_on='ID_x')

    return df_s, df_k

def _unit_test_BirdDetection_CommonName(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:

    outcome = all(df_s['CommonName']==df_k['CommonName'])

    if all(df_s['CommonName']==df_k['CommonName']):
        print(f'    SUCCESS: The array of common names recorded in `source` matched `k_load` for `birds.ncrn.BirdDetection`!')
    else:
        print(f'    FAIL: The array of common names recorded in `source` DID NOT match that of `k_load` for `birds.ncrn.BirdDetection`!')

    return outcome

def _unit_test_BirdDetection_ScientificName(df_s:pd.DataFrame, df_k:pd.DataFrame) -> bool:

    outcome = all(df_s['ScientificName']==df_k['ScientificName'])

    if all(df_s['ScientificName']==df_k['ScientificName']):
        print(f'    SUCCESS: The array of scientific names recorded in `source` matched `k_load` for `birds.ncrn.BirdDetection`!')
    else:
        print(f'    FAIL: The array of common names recorded in `source` DID NOT match `k_load` for `birds.ncrn.BirdDetection`!')

    return outcome

def _compare_pivot_tables(xwalk_dict:dict, n:int, verbose:bool=False) -> list:
    """Compare the results reported in `n` randomly-chosen site visits against each other in `source` and `k_load`

    Args:
        xwalk_dict (dict): a `birds` dictionary
        n (int): the number of site visits to compare

    Returns:
        list: a list of `bool` types, True indicates that the site visit was identical for `source` and `k_load`
    """
    outcomes = []

    findme = xwalk_dict['ncrn']['DetectionEvent']['source'].sample(n).event_id.unique()
    
    comparisons = {}
    comparisons = _pivot_source(xwalk_dict, comparisons, findme)
    comparisons = _pivot_k_load(xwalk_dict, comparisons, findme)
    
    for k,v in comparisons.items():
        source = comparisons[k]['source'].droplevel(['Event_ID'],axis=1)
        k_load = comparisons[k]['k_load'].droplevel(['Event_ID'],axis=1)
        outcomes.append(all(source==k_load))
    
    if all(outcomes):
        print(f'SUCCESS: Compared findings in {n} site visits `source` matched `k_load` for and found all records to be identical!')
    else:
        print(f'FAIL: The array of common names recorded in `source` DID NOT match that of `k_load` for `birds.ncrn.BirdDetection`!')

    if verbose==True:
        MAX = 2
        subset = {k: comparisons[k] for k in list(comparisons)[:MAX]}
        print('')
        print(f'`verbose` is True. Displaying {MAX} site visits...')
        print('')
        for k,v in subset.items():
            print(k)
            df1 = subset[k]['source'].droplevel(['group'],axis=1)
            df2 = subset[k]['k_load'].droplevel(['group'],axis=1)
            print(pd.concat([df1, df2], axis=1))
            print('')

    return outcomes

def _pivot_source(xwalk_dict:dict, comparisons:dict, findme:list) -> dict:

    df = xwalk_dict['ncrn']['DetectionEvent']['source'][xwalk_dict['ncrn']['DetectionEvent']['source']['event_id'].isin(findme)].merge(xwalk_dict['ncrn']['Location']['source'], left_on='location_id', right_on='Location_ID', how='left')
    df = df.merge(xwalk_dict['ncrn']['Protocol']['source'][['Protocol_ID','Protocol_Name']], left_on='protocol_id', right_on='Protocol_ID', how='left')
    before_cols = list(df.columns)
    df = df.merge(xwalk_dict['ncrn']['Contact']['source'][['Contact_ID','Last_Name','First_Name']], left_on='observer', right_on='Contact_ID', how='left')
    df['observer'] = df['First_Name'] + ' ' + df['Last_Name']
    df = df[before_cols]
    df = df.merge(xwalk_dict['ncrn']['Contact']['source'][['Contact_ID','Last_Name','First_Name']], left_on='recorder', right_on='Contact_ID', how='left')
    df['recorder'] = df['First_Name'] + ' ' + df['Last_Name']
    df = df[before_cols]
    df = df[['event_id','Date','activity_start_datetime','Protocol_Name','GRTS_Order','Plot_Name','observer', 'recorder', 'entered_by']].sort_values(['Date','GRTS_Order'])
    df['group'] = df['Date'].astype(str) +'.' + df['GRTS_Order'].astype(str) + '.' + df['Plot_Name']
    for x in df.group.unique():
        comparisons[x] = {
            'source':pd.DataFrame()
            ,'k_load':pd.DataFrame()
        }

    alldf = xwalk_dict['ncrn']['BirdDetection']['source'][xwalk_dict['ncrn']['BirdDetection']['source']['Event_ID'].isin(df.event_id.unique())].groupby(['Event_ID','AOU_Code']).size().reset_index(name='count').sort_values(['count'], ascending=True)
    alldf['AOU_Code'] = alldf['AOU_Code'].str.split('_').str[0]
    alldf = alldf.sort_values('AOU_Code')
    alldf = alldf.merge(df[['event_id', 'activity_start_datetime', 'GRTS_Order','group', 'observer', 'recorder', 'entered_by']], left_on='Event_ID', right_on='event_id', how='left')
    for group in alldf.group.unique():
        pd.pivot_table(alldf[alldf['group']==group],values='count',index='AOU_Code', columns=['group', 'Event_ID', 'activity_start_datetime', 'observer', 'recorder', 'entered_by'])
        comparisons[group]['source'] = pd.pivot_table(alldf[alldf['group']==group],values='count',index='AOU_Code', columns=['group', 'Event_ID', 'activity_start_datetime', 'observer', 'recorder', 'entered_by'])

    return comparisons

def _pivot_k_load(xwalk_dict:dict, comparisons:dict, findme:list) -> dict:
    
    findme2 = xwalk_dict['ncrn']['DetectionEvent']['pk_fk_lookup'][xwalk_dict['ncrn']['DetectionEvent']['pk_fk_lookup']['ID'].isin(findme)].rowid.unique()
    locations=xwalk_dict['ncrn']['Location']['k_load'].copy().drop(['EnteredBy'],axis=1).rename(columns={'ID':'location_ID'})
    df = xwalk_dict['ncrn']['DetectionEvent']['k_load'][xwalk_dict['ncrn']['DetectionEvent']['k_load']['ID'].isin(findme2)].merge(locations, left_on='LocationID', right_on='location_ID', how='left')
    protocols = xwalk_dict['ncrn']['Protocol']['k_load'].copy()[['ID','Title']].rename(columns={'ID':'protocol_ID'})
    df = df.merge(protocols, left_on='ProtocolID', right_on='protocol_ID', how='left')
    before_cols = list(df.columns)
    contacts=xwalk_dict['ncrn']['Contact']['k_load'][['ID','LastName','FirstName']].copy().rename(columns={'ID':'observer_ID'})
    df = df.merge(contacts, left_on='Observer_ContactID', right_on='observer_ID', how='left')
    df['observer'] = df['FirstName'] + ' ' + df['LastName']
    before_cols.append('observer')
    df = df[before_cols]
    contacts=xwalk_dict['ncrn']['Contact']['k_load'][['ID','LastName','FirstName']].copy().rename(columns={'ID':'recorder_ID'})
    df = df.merge(contacts, left_on='Recorder_ContactID', right_on='recorder_ID', how='left')
    df['recorder'] = df['FirstName'] + ' ' + df['LastName']
    before_cols.append('recorder')
    df = df[before_cols]
    df['Date'] = df['StartDateTime'].dt.date
    df.rename(columns={
        'ID':'event_id'
        ,'StartDateTime':'activity_start_datetime'
        ,'Title':'Protocol_Name'
        ,'Label':'Plot_Name'
        ,'OldCode':'GRTS_Order'
        ,'EnteredBy':'entered_by'
    },inplace=True)
    df = df[['event_id','Date','activity_start_datetime','Protocol_Name','GRTS_Order','Plot_Name','observer', 'recorder', 'entered_by']].sort_values(['Date','GRTS_Order'])
    df['group'] = df['Date'].astype(str) +'.' + df['GRTS_Order'].astype(str) + '.' + df['Plot_Name']

    speciespark = xwalk_dict['ncrn']['BirdSpeciesPark']['k_load'].copy()[['ID','BirdSpeciesID']].rename(columns={'ID':'birdspeciespark_ID'})
    species = xwalk_dict['ncrn']['BirdSpecies']['k_load'].copy()[['ID','Code']].rename(columns={'ID':'birdspecies_ID', 'Code':'AOU_Code'})
    speciespark = speciespark.merge(species, left_on='BirdSpeciesID',right_on='birdspecies_ID', how='left')[['birdspeciespark_ID','AOU_Code']]
    detections = xwalk_dict['ncrn']['BirdDetection']['k_load'].copy().merge(speciespark, left_on='BirdSpeciesParkID', right_on='birdspeciespark_ID', how='left')
    alldf = detections[detections['DetectionEventID'].isin(df.event_id.unique())].groupby(['DetectionEventID','AOU_Code']).size().reset_index(name='count').sort_values(['count'], ascending=True)
    alldf = alldf.sort_values('AOU_Code')
    alldf = alldf.merge(df[['event_id', 'activity_start_datetime', 'GRTS_Order','group', 'observer', 'recorder', 'entered_by']], left_on='DetectionEventID', right_on='event_id', how='left')
    alldf.rename(columns={'DetectionEventID':'Event_ID'},inplace=True)
    for group in alldf.group.unique():
        # pd.pivot_table(alldf[alldf['group']==group],values='count',index='AOU_Code', columns=['group', 'Event_ID', 'activity_start_datetime', 'observer', 'recorder', 'entered_by'])
        comparisons[group]['k_load'] = pd.pivot_table(alldf[alldf['group']==group],values='count',index='AOU_Code', columns=['group', 'Event_ID', 'activity_start_datetime', 'observer', 'recorder', 'entered_by'])

    return comparisons

def make_views(xwalk_dict:dict) -> dict:
    data_template = pd.read_excel(assets.DATA_TEMPLATE['fname'], sheet_name=assets.DATA_TEMPLATE['sheetname'])
    location_template = pd.read_csv(assets.LOCATION_TEMPLATE)
    views = {
        'data': pd.DataFrame(columns=data_template.columns)
        ,'locations': pd.DataFrame(columns=location_template.columns)
    }

    return views
