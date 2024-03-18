import pandas as pd
import numpy as np
import assets.assets as assets
import re
import src.tbl_xwalks as tx
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import time
import datetime as dt

EXCLUSIONS = ['unique_vals'] # 'unique_vals` is empty when the table does not enforce unique values in any field; this can happen in reality so we ignore here
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
    print('Checking that logical keys were replaced by ints...')
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
    # loads_to_check:list = ['tbl_load','k_load']
    loads_to_check:list = ['tbl_load']
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
                        unique_vals = '+'.join(unique_vals)
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

def unit_test(xwalk_dict:dict) -> None:
    """
    Automated unit test to confirm that pk-fk relationships return the exact records in `k_load` that were present in `source`

    Make a flattened dataset out of the `source`, `tbl_load`, and `k_load`

    Compare the datasets

    - counts of records per date
    - counts of records per site
    - counts of records per bird species
    - counts of records per observer, recorder
    - pivot, compare counts of records (like in summary sent to collaborator)
    """

    return None

