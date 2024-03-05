import pandas as pd
import assets.assets as assets
import re

# EXCLUSIONS = ['tsql', 'payload'] # remove each element once we add modules to populate
EXCLUSIONS = ['unique_vals'] # remove each element once we add modules to populate

def check_birds(xwalk_dict:dict) -> None:
    """Validate a dictionary of birds data

    Args:
        xwalk_dict (dict): The dictionary output by src.make_templates.make_birds().

    Returns:
        None: This function returns None.
    """
    print('')
    print(f'Validating dictionary against db schema...')
    _check_schema(xwalk_dict=xwalk_dict)
    print('')
    print('Checking the dimensions of each table...')
    _validate_tbl_loads(xwalk_dict=xwalk_dict)
    print('')
    print('Checking each table for required attributes...')
    _validate_xwalks(xwalk_dict=xwalk_dict)
    _check_blanks(xwalk_dict=xwalk_dict)
    print('')
    print('Checking each table for unique values...')
    _validate_unique_vals(xwalk_dict=xwalk_dict)
    print('')

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

def _validate_tbl_loads(xwalk_dict:dict) -> None:
    """Check that the `tbl_load` attr produced from each `source` and `xwalk` is valid"""
    mykeys = _traverse(xwalk_dict, EXCLUSIONS)

    _validate_dims(xwalk_dict, mykeys)
    # _validate_referential_integrity(xwalk_dict)

    return None

def _validate_dims(xwalk_dict:dict, mykeys:list) -> None:
    """Check the dimensions of each `tbl_load`"""

    _validate_rows(xwalk_dict, mykeys)
    _validate_cols(xwalk_dict, mykeys)

    return None

def _validate_rows(xwalk_dict:dict, mykeys:list) -> None:
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
                        if len(xwalk_dict[schema][tbl]['source']) != len(xwalk_dict[schema][tbl]['tbl_load']):
                            missing[k]['counter'] +=1
                            missing[k]['mylist'].append(f"dict['{schema}']['{tbl}']['{k}']")
                            missing_vals[schema][tbl].append(f"dict['{schema}']['{tbl}']['{k}']")
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
        print('SUCCESS: The count of rows for each `tbl_load` matches its `source`!')

    return None

def _validate_cols(xwalk_dict:dict, mykeys:list) -> None:
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
                        for col in xwalk_dict[schema][tbl]['tbl_load'].columns:
                            if col != 'rowid' and col not in xwalk_dict[schema][tbl]['destination'].columns:
                                missing[k]['counter'] +=1
                                missing[k]['mylist'].append(f"dict['{schema}']['{tbl}']['{k}']")
                                missing_vals[schema][tbl].append(f"dict['{schema}']['{tbl}']['{k}']")
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
        print('SUCCESS: The count of columns for each `tbl_load` matches its `destination`!')
    
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
                            missing[k]['mylist'].append(f"dict['{schema}']['{tbl}']['{k}']")
                            missing_vals[schema][tbl].append(f"dict['{schema}']['{tbl}']['{k}']")
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
            print(f"WARNING: The column-order in `tbl_load` does not match that of `destination` in {missing[k]['counter']} tables!")
            for v in missing[k]['mylist']:
                print(f'    {v}')
        else:
            successes.append(k)
    if len(successes)==len(mykeys):
        print('SUCCESS: The column-order in `tbl_load` matches that of `destination`!')

    return None

def _validate_referential_integrity(xwalk_dict:dict) -> None:
    # TODO: check that the INT id for each GUID lines up among related tables
    
    return None

def _validate_unique_vals(xwalk_dict:dict) -> None:
    loads_to_check:list = ['tbl_load','k_load']
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
                        unique_vals = [f"tmp['{x}'].astype(str)" for x in unique_vals]
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
        print(f"WARNING: required-unique fields contain duplicate values in {missing['counter']} tables!")
        for v in missing['mylist']:
            print(f'    {v}')
    else:
        print('SUCCESS: All required-unique fields contain only unique values in {loads_to_check}!')

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
                    missing[k]['counter'] +=1
                    missing[k]['mylist'].append(f"dict['{schema}']['{tbl}']['{k}']")
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

def _check_blanks(xwalk_dict:dict) -> None:

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
                            missing[k]['counter'] +=1
                            missing[k]['mylist'].append(f"dict['{schema}']['{tbl}']['{k}']")
                            missing_vals[schema][tbl].append(f"dict['{schema}']['{tbl}']['{k}']")
                    else:
                        if len(xwalk_dict[schema][tbl][k]) == 0:
                            missing[k]['counter'] +=1
                            missing[k]['mylist'].append(f"dict['{schema}']['{tbl}']['{k}']")
                            missing_vals[schema][tbl].append(f"dict['{schema}']['{tbl}']['{k}']")

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
        print('SUCCESS: All required attributes exist in all tables!')

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
    #                     attr = attr.replace(f"dict['{schema}']['{tbl}']", '')
    #                     attr = attr.replace("]", '')
    #                     attr = attr.replace("[", '')
    #                     print(f'        {attr}')
    # else:
    #     print('SUCCESS: All tables have all required attributes!')

    return None
