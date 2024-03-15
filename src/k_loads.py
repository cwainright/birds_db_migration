import pandas as pd
import numpy as np

def _update_foreign_keys(xwalk_dict:dict) -> dict:
    """Update the source-file foreign keys to destination-file foreign keys
    
    In general, the source file used guids or logical-key concatenations and the destination needs integers

    """

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
            if len(pks) ==1:
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
                # print(f"FAILc")
                print(f"FAIL: multiple primary-key fields found in birds['{schema}']['{tbl}']['xwalk']")

    return xwalk_dict
