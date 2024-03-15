import pandas as pd
import numpy as np
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

def _update_foreign_keys(xwalk_dict:dict) -> dict:
    """Update the source-file foreign keys to destination-file foreign keys
    
    In general, the source file used guids or logical-key concatenations and the destination needs integers

    """

    loads_to_check:list = ['k_load']
    for schema in xwalk_dict.keys():
        for tbl in xwalk_dict[schema].keys():
            mask = (xwalk_dict[schema][tbl]['xwalk']['fk']==True) & (xwalk_dict[schema][tbl]['xwalk']['calculation']!='blank_field')
            fks = xwalk_dict[schema][tbl]['xwalk'][mask].destination.unique()
            if len(fks) >0:
                for fk in fks:
                    if fk=='SynonymID' and schema=='ncrn' and tbl=='BirdSpecies':
                        pass
                    else:
                        constrained_by = xwalk_dict[schema][tbl]['xwalk'][xwalk_dict[schema][tbl]['xwalk']['destination']==fk].references.values[0]
                        lookup = constrained_by.split('.')
                        if len(lookup) ==3:
                            ref = xwalk_dict[lookup[0]][lookup[1]]['pk_fk_lookup'].copy()
                            pk_orig = lookup[2]
                            pk_new = pk_orig + '_pk'
                            ref.rename(columns={pk_orig:pk_new}, inplace=True)
                            for load in loads_to_check:
                                if all([x for x in xwalk_dict[schema][tbl][load][fk].unique() if x in ref[pk_new].unique()]):
                                    before_columns = xwalk_dict[schema][tbl][load].columns
                                    before_len = len(xwalk_dict[schema][tbl][load])
                                    try:
                                        xwalk_dict[schema][tbl][load][fk].astype(int) # if the key is already an int, leave it
                                    except:
                                        try:
                                            beforedf = xwalk_dict[schema][tbl][load].copy()
                                            xwalk_dict[schema][tbl][load] = xwalk_dict[schema][tbl][load].merge(ref, left_on=fk, right_on=pk_new, how='left')
                                            if len(xwalk_dict[schema][tbl][load])==before_len:
                                                xwalk_dict[schema][tbl][load][fk] = xwalk_dict[schema][tbl][load]['rowid']
                                                xwalk_dict[schema][tbl][load] = xwalk_dict[schema][tbl][load][before_columns]
                                                print(f"Updated: `birds['{schema}']['{tbl}']['{load}']['{fk}']` now congruent with `birds['{lookup[0]}']['{lookup[1]}']['{load}']['{lookup[2]}']`")
                                            else:
                                                print(f"FAILED TO UPDATE FOREIGN KEY: merge added rows, change rolled back... `birds['{schema}']['{tbl}']['{load}']['{fk}']` to `birds['{lookup[0]}']['{lookup[1]}']['{load}']['{lookup[2]}']`")
                                                xwalk_dict[schema][tbl][load] = beforedf.copy()
                                        except:
                                            print(f"FAILED TO UPDATE FOREIGN KEY: merge step: `birds['{schema}']['{tbl}']['{load}']['{fk}']` to `birds['{lookup[0]}']['{lookup[1]}']['{load}']['{lookup[2]}']`")
                                else:
                                    print(f"FAILED TO UPDATE FOREIGN KEY: lookup-table step: `birds['{schema}']['{tbl}']['{load}']['{fk}']` to `birds['{lookup[0]}']['{lookup[1]}']['{load}']['{lookup[2]}']`")
                        else:
                            print(f"FAIL: check referential integrity, lookup error: birds['{schema}']['{tbl}']['xwalk'].destination=='{fk}'; ['references'] is broken")

    return xwalk_dict

def _update_primary_keys(xwalk_dict:dict) -> dict:
    """Update the source-file primary keys to destination-file primary keys
    
    In general, the source file contains guids or logical-key concatenations and the destination needs integers

    """
    loads_to_check:list = ['k_load']
    for schema in xwalk_dict.keys():
        for tbl in xwalk_dict[schema].keys():
            mask = (xwalk_dict[schema][tbl]['xwalk']['pk']==True)
            pks = xwalk_dict[schema][tbl]['xwalk'][mask].destination.unique()
            lookup = xwalk_dict[schema][tbl]['pk_fk_lookup']
            if len(pks) == 1:
                for pk in pks:
                    if pk == 'Code': # when the primary key is called 'Code', we keep a str pk...
                        pass
                    else:
                        for load in loads_to_check:
                            # only proceed if the lookup is a proven-positive match to the data table
                            if len(lookup) == len(xwalk_dict[schema][tbl][load][pk].unique()):
                                if all(lookup[pk].values == xwalk_dict[schema][tbl][load][pk].values):
                                    try:
                                        xwalk_dict[schema][tbl][load][pk] = lookup['rowid']
                                    except:
                                        print(f"FAIL: lookup contains different columns than expected: birds['{schema}']['{tbl}']['pk_fk_lookup']['rowid']")
                                else:
                                    print(f"FAIL: lookup contains incongruent values:birds['{schema}']['{tbl}']['{load}'] is different than birds['{schema}']['{tbl}']['pk_fk_lookup']")
                            else: 
                                print(f"FAIL: length of birds['{schema}']['{tbl}']['{load}']] is different than birds['{schema}']['{tbl}']['pk_fk_lookup']: {len(lookup)=} vs. {len(xwalk_dict[schema][tbl][load][pk].unique())=}")
            else:
                print(f"FAIL: multiple primary-key fields found in birds['{schema}']['{tbl}']['xwalk']")

    return xwalk_dict
