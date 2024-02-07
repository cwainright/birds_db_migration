"""make the templates to which we crosswalk access tables"""
import assets.assets as assets
import pandas as pd
import pickle
import src.build_tbls as bt
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

    # object to hold data
    xwalk_dict = {}
    for tbl in template_list:
        xwalk_dict[tbl] = {
            'xwalk': pd.DataFrame(columns=['source', 'destination', 'calculation']) # document the crosswalk for each table
            ,'source_name': assets.TBL_XWALK[tbl] # store the name of the source table
            ,'source': pd.DataFrame() # placeholder to store the source data
            ,'destination': dest_dict[tbl] # store the destination data (mostly just for its column names and order)
            ,'tbl_load': pd.DataFrame() # placeholder for the source data crosswalked to the destination schema
        }
        xwalk_dict[tbl]['source'] = source_dict[xwalk_dict[tbl]['source_name']] # route the source data to its placeholder
        xwalk_dict[tbl]['xwalk']['destination'] = xwalk_dict[tbl]['destination'].columns # route the destination columns to their placeholder in the crosswalk

    # create xwalk for each destination table
    xwalk_dict = _create_xwalks(xwalk_dict)

    # execute xwalk to generate load
    xwalk_dict = _execute_xwalks(xwalk_dict)

    # validate
    # TODO: write logic to check that each column we hard-coded into ['xwalk']['source'] and ['xwalk']['destination'] actually exists in the dataframe so we can't go sideways on column reassignment
    xwalk_dict = _validate_xwalks(xwalk_dict)

    # save output
    if dest !='':
        with open(dest, 'wb') as f:
            pickle.dump(xwalk_dict, f)
        print(f'Output saved to `{dest}`')
    
    return xwalk_dict

def _create_xwalks(xwalk_dict:dict) -> dict:

    xwalk_dict = _detection_event_xwalk(xwalk_dict)
    xwalk_dict = _bird_detection_xwalk(xwalk_dict)

    return xwalk_dict

def _execute_xwalks(xwalk_dict:dict) -> dict:
    # TODO: this function should execute the instructions stored in each table's `xwalk` to produce a `tbl_load`
    return xwalk_dict

def _validate_xwalks(xwalk_dict:dict) -> dict:
    # check that dims of `tbl_load` == dims of `source` (same number of rows and columns)
    xwalk_dict = _validate_dims(xwalk_dict)
    # check that each column in `tbl_load` exists in `destination`
    # check that the column order in `tbl_load` matches that of `destination`
    xwalk_dict = _validate_cols(xwalk_dict)
    # check constraints? may be more work than simply letting sqlserver do the checks
    return xwalk_dict

def _validate_dims(xwalk_dict:dict) -> dict:
    # TODO: check that dims of `tbl_load` == dims of `source` (same number of rows and columns)
    return xwalk_dict

def _validate_cols(xwalk_dict:dict) -> dict:
    # TODO: check that each column in `tbl_load` exists in `destination`
    # TODO: check that the column order in `tbl_load` matches that of `destination`
    return xwalk_dict

def _detection_event_xwalk(xwalk_dict:dict) -> dict:
    """Make the crosswalk for source.tbl_Events to destination.ncrn.DetectionEvent

    Crosswalk collects three types of information:
    1. 1:1 fields. When a destination field has a matching source field, `source` captures the source field name and `calculation` is '1_to_1_fieldmap'.
    2. Calculated fields. When one or more source fields need to be combined to match the destination field, `source` captures the code to manipulate and `calculation` is 'calculate'.
    3. Blanks. When a destination field is not required and has no matching source field, `source` is np.NaN and `calculation` is 'blank'.

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    
    # 1:1 fields
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'event_id', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'LocationID')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'location_id', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'ProtocolID')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'protocol_id', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'EnteredBy')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'entered_by', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'AirTemperature')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'temperature', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'Notes')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'event_notes', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'EnteredDateTime')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'entered_date', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'DataProcessingLevelID')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'dataprocessinglevelid', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'DataProcessingLevelDate')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'dataprocessingleveldate', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'RelativeHumidity')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'humidity', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'].isna()==False)
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['calculation'] =  np.where(mask, '1_to_1_fieldmap', xwalk_dict['ncrn.DetectionEvent']['xwalk']['calculation'])

    # Calculated fields
    # `AirTemperatureRecorded` BIT type (bool as 0 or 1); not a NCRN field. Should be 1 when the row has a value in `AirTemperature`
    # `StartDateTime` need to make datetime from src_tbl.Date and src_tbl.Start_Time
    # `IsSampled` BIT type (bool as 0 or 1) I think this is the inverse of src_tbl.is_excluded, which is a boolean in Access with one unique value: [0]
    # -- lookup-constrained calculations
    # `SamplingMethodID`: [lu.SamplingMethod]([ID])
    # `Observer_ContactID`: [ncrn.Contact]([ID])
    # `Recorder_ContactID`: [ncrn.Contact]([ID])
    # `Observer_ExperienceLevelID`: [lu.ExperienceLevel]([ID])
    # `ProtocolNoiseLevelID`: [lu.NoiseLevel]([ID])
    # `ProtocolWindCodeID`: [lu.WindCode]([ID])
    # `ProtocolPrecipitationTypeID`: [lu.PrecipitationType]([ID])
    # `TemperatureUnitCode`: [lu.TemperatureUnit]([ID])

    # Blanks
    # `DataProcessingLevelNote`
    # `Rowversion`
    # `UserCode`
    blank_fields = ['DataProcessingLevelNote', 'Rowversion','UserCode']
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'].isin(blank_fields))
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['calculation'] =  np.where(mask, 'leave_blank', xwalk_dict['ncrn.DetectionEvent']['xwalk']['calculation'])

    # validate
    missing = xwalk_dict['ncrn.DetectionEvent']['xwalk'][xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'].isna()].destination.unique()
    if len(missing) >0:
        for m in missing:
            print(f'[\'ncrn.DetectionEvent\'][\'xwalk\'] is missing a `source` value where `destination`==\'{m}\'.')

    return xwalk_dict

def _bird_detection_xwalk(xwalk_dict:dict) -> dict:
    """Crosswalk source.tbl_Event to destination.ncrn.BirdDetection

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    # xwalk_dict['ncrn.BirdDetection']['source_name'] = 'tbl_Field_Data'
    # xwalk_dict['ncrn.DetectionEvent']['xwalk']

    return xwalk_dict
