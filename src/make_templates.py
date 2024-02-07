"""make the templates to which we crosswalk access tables"""
import assets.assets as assets
import pandas as pd
import pickle
import src.build_tbls as bt
import src.tbl_xwalks as tw
import numpy as np

# template_list = assets.DEST_LIST.copy()
template_list = list(assets.TBL_XWALK.keys())

def make_xwalks(dest:str='') -> dict:
    """Create a dictionary of crosswalks for each table in the source (Access) and destination (SQL Server) databases

    Args:
        dest (str, optional): Relative or absolute filepath to which a pickle of the output should be saved. Must end in '.pkl'. Defaults to ''.

    Returns:
        dict: a containing destination dataframes and the source componenets from which they were generated 

    Examples:
        import src.make_templates as mt
        testdict = mt.make_xwalks('saved_dictionary.pkl')
        with open('saved_dictionary.pkl', 'rb') as f:
            loaded_dict = pickle.load(f) 
    """
    if dest !='':
        assert dest.endswith('.pkl'), print(f'You entered `{dest}`. If you want to save the output of `make_xwalks()`, `dest` must end in ".pkl"')

    source_dict = bt._get_src_tbls() # query the source data (i.e., the Access table(s) containing fields)
    dest_dict = bt._get_dest_tbls() # query the destination data (i.e., the SQL Server table; usually an empty dataframe with the correct columns)

    # main object to hold data
    xwalk_dict = {}
    for tbl in template_list:
        xwalk_dict[tbl] = {
            'xwalk': pd.DataFrame(columns=['destination', 'source', 'calculation', 'note']) # document the crosswalk for each table
            ,'source_name': assets.TBL_XWALK[tbl] # store the name of the source table
            ,'source': pd.DataFrame() # placeholder to store the source data
            ,'destination': dest_dict[tbl] # store the destination data (mostly just for its column names and order)
            ,'tbl_load': pd.DataFrame() # placeholder for the source data crosswalked to the destination schema
        }
        xwalk_dict[tbl]['source'] = source_dict[xwalk_dict[tbl]['source_name']] # route the source data to its placeholder
        xwalk_dict[tbl]['tbl_load'] = pd.DataFrame(columns=xwalk_dict[tbl]['destination'].columns)
        xwalk_dict[tbl]['xwalk']['destination'] = xwalk_dict[tbl]['destination'].columns # route the destination columns to their placeholder in the crosswalk

    # create xwalk for each destination table
    xwalk_dict = _create_xwalks(xwalk_dict)

    # validate
    xwalk_dict = _validate_xwalks(xwalk_dict)

    # execute xwalk to generate load
    xwalk_dict = _execute_xwalks(xwalk_dict)

    # validate
    xwalk_dict = _validate_tbl_loads(xwalk_dict)

    # save output
    if dest !='':
        with open(dest, 'wb') as f:
            pickle.dump(xwalk_dict, f)
        print(f'Output saved to `{dest}`')
    
    return xwalk_dict

def _create_xwalks(xwalk_dict:dict) -> dict:

    xwalk_dict = tw._detection_event_xwalk(xwalk_dict)
    xwalk_dict = tw._bird_detection_xwalk(xwalk_dict)

    return xwalk_dict

def _execute_xwalks(xwalk_dict:dict) -> dict:
    # TODO: this function should execute the instructions stored in each table's `xwalk` to produce a `tbl_load`

    for tbl in template_list:
        print(tbl)
        xwalk = xwalk_dict[tbl]['xwalk']
        # if destination has a one-to-one source field, execute assignments
        one_to_ones = list(xwalk[xwalk['calculation']=='map_source_to_destination_1_to_1'].destination.values)
        for dest_col in one_to_ones:
            print(dest_col)
            src_col = xwalk[xwalk['destination']==dest_col].source.values[0]
            print(src_col)
            xwalk_dict[tbl]['tbl_load'][dest_col] = xwalk_dict[tbl]['source'][src_col]

        # if it's a calculate field, calculate
        # calculates = list(xwalk[xwalk['calculation']=='calculate_dest_field_from_source_field'].destination.values)
        # if it's a blank field, leave blank
        # blanks = list(xwalk[xwalk['calculation']=='blank_field'].destination.values)

    return xwalk_dict

def _validate_tbl_loads(xwalk_dict:dict) -> dict:
    # TODO: this function should check that the `tbl_load` attr produced from each `source` and `xwalk` is valid

    # check that dims of `tbl_load` == dims of `source` (same number of rows and columns)
    xwalk_dict = _validate_dims(xwalk_dict)
    # check that each column in `tbl_load` exists in `destination`
    # check that the column order in `tbl_load` matches that of `destination`
    xwalk_dict = _validate_cols(xwalk_dict)
    # check constraints? may be more work than simply letting sqlserver do the checks
    return xwalk_dict


def _validate_xwalks(xwalk_dict:dict) -> dict:
    # TODO: this function should check that the `xwalk` attr produced for each `tbl_load` is valid

    # find and report missing values
    missing = xwalk_dict['ncrn.DetectionEvent']['xwalk'][xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'].isna()].destination.unique()
    if len(missing) >0:
        for m in missing:
            print(f'[\'ncrn.DetectionEvent\'][\'xwalk\'] is missing a `source` value where `destination`==\'{m}\'.')

    # how else can the xwalk go sideways?

    return xwalk_dict

def _validate_dims(xwalk_dict:dict) -> dict:
    # TODO: check that dims of `tbl_load` == dims of `source` (same number of rows and columns)
    return xwalk_dict

def _validate_cols(xwalk_dict:dict) -> dict:
    # TODO: check that each column in `tbl_load` exists in `destination`
    # TODO: check that the column order in `tbl_load` matches that of `destination`
    return xwalk_dict


