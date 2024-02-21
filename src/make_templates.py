"""make the templates to which we crosswalk access tables"""
import assets.assets as assets
import pandas as pd
import pickle
import src.build_tbls as bt
import src.tbl_xwalks as tx
import src.check as c
import numpy as np
import datetime as dt
import time

TBL_XWALK = assets.TBL_XWALK
TBL_ADDITIONS = assets.TBL_ADDITIONS

def make_birds(dest:str='') -> dict:
    """Create a dictionary of crosswalks for each table in the source (Access) and destination (SQL Server) databases

    Args:
        dest (str, optional): Relative or absolute filepath to which a pickle of the output should be saved. Must end in '.pkl'. Defaults to ''.

    Returns:
        dict: a containing destination dataframes and the source componenets from which they were generated 

    Examples:
        import src.make_templates as mt
        testdict = mt.make_birds('saved_dictionary.pkl')
        with open('saved_dictionary.pkl', 'rb') as f:
            loaded_dict = pickle.load(f) 
    """
    print('')
    print('Building birds...')
    print('')
    print('')
    start_time = time.time()
    if dest !='':
        assert dest.endswith('.pkl'), print(f'You entered `{dest}`. If you want to save the output of `make_xwalks()`, `dest` must end in ".pkl"')

    source_dict = bt._get_src_tbls() # query the source data (i.e., the Access table(s))
    dest_dict = bt._get_dest_tbls() # query the destination data (i.e., the SQL Server table; usually an empty dataframe with the correct columns)

    # main object to hold data
    xwalk_dict = {}
    # add the tables for which we have a source and assign their attributes
    for schema in TBL_XWALK.keys():
        xwalk_dict[schema] = {}
        for tbl in TBL_XWALK[schema].keys():
            xwalk_dict[schema][tbl] = {
                'xwalk': pd.DataFrame(columns=['destination', 'source', 'calculation', 'note']) # the crosswalk to translate from `source` to `tbl_load`
                ,'source_name': assets.TBL_XWALK[schema][tbl] # name of source table
                ,'source': pd.DataFrame() # source data
                ,'destination': dest_dict[tbl] # destination data (mostly just for its column names and order)
                ,'tbl_load': pd.DataFrame() # `source` data crosswalked to the destination schema
                ,'payload_cols': [] # the columns to extract from `tbl_load` and load into `payload`
                ,'payload': pd.DataFrame() # `tbl_load` transformed for loading to destination database
                ,'tsql': '' # the t-sql to load the `payload` to the destination table
            }
            xwalk_dict[schema][tbl]['source'] = source_dict[xwalk_dict[schema][tbl]['source_name']] # route the source data to its placeholder

    # add the tables for which we have no source
    for schema in TBL_ADDITIONS.keys():
        if schema not in xwalk_dict.keys():
            xwalk_dict[schema] = {}
        for tbl in TBL_ADDITIONS[schema]:
            xwalk_dict[schema][tbl] = {
                'xwalk': pd.DataFrame(columns=['destination', 'source', 'calculation', 'note']) # the crosswalk to translate from `source` to `tbl_load`
                ,'source_name': 'NCRN_Landbirds.'+schema+'.'+tbl # name of source table
                ,'source': pd.DataFrame(columns=dest_dict[tbl].columns) # source data
                ,'destination': dest_dict[tbl] # destination data (mostly just for its column names and order)
                ,'tbl_load': pd.DataFrame() # `source` data crosswalked to the destination schema
                ,'payload_cols': [] # the columns to extract from `tbl_load` and load into `payload`
                ,'payload': pd.DataFrame() # `tbl_load` transformed for loading to destination database
                ,'tsql': '' # the t-sql to load the `payload` to the destination table
            }
    
    # distribute and assign attributes from query results (`bt.get_src_tbls()` and `bt._get_dest_tbls()`)
    for schema in xwalk_dict.keys():
        for tbl in xwalk_dict[schema].keys():
            xwalk_dict[schema][tbl]['tbl_load'] = pd.DataFrame(columns=xwalk_dict[schema][tbl]['destination'].columns)
            xwalk_dict[schema][tbl]['xwalk']['destination'] = xwalk_dict[schema][tbl]['destination'].columns # route the destination columns to their placeholder in the crosswalk
            exclude_cols = ['ID', 'Rowversion', 'UserCode'] # list of columns that SQL Server should calculate upon data loading; these cols should not be part of the payload
            xwalk_dict[schema][tbl]['payload_cols'] = [x for x in xwalk_dict[schema][tbl]['destination'].columns if x not in exclude_cols] # the columns to extract from `tbl_load` and load into `payload`

    # create xwalk for each destination table
    xwalk_dict = _create_xwalks(xwalk_dict)

    # execute exception-handling
    xwalk_dict = _execute_xwalk_exceptions(xwalk_dict)

    # validate
    xwalk_dict = c._validate_xwalks(xwalk_dict)

    # execute xwalk to generate load
    xwalk_dict = _execute_xwalks(xwalk_dict)

    # validate
    xwalk_dict = c._validate_tbl_loads(xwalk_dict)

    # generate payload
    xwalk_dict = _generate_payload(xwalk_dict)

    # validate payload
    xwalk_dict = c._validate_payload(xwalk_dict)

    # generate t-sql
    xwalk_dict = _generate_tsql(xwalk_dict)

    # save output
    if dest !='':
        with open(dest, 'wb') as f:
            pickle.dump(xwalk_dict, f)
        print(f'Output saved to `{dest}`')
    end_time = time.time()
    elapsed_time = end_time - start_time
    elapsed_time = str(dt.timedelta(seconds=elapsed_time))
    elapsed_time = elapsed_time.split('.')[0]
    print('')
    print(f'`make_birds()` succeeded in: {elapsed_time}')

    return xwalk_dict

def _create_xwalks(xwalk_dict:dict) -> dict:

    xwalk_dict = tx._ncrn_DetectionEvent(xwalk_dict)
    xwalk_dict = tx._ncrn_BirdDetection(xwalk_dict)
    xwalk_dict = tx._ncrn_Protocol(xwalk_dict)
    xwalk_dict = tx._ncrn_Location(xwalk_dict)
    xwalk_dict = tx._ncrn_Park(xwalk_dict)
    xwalk_dict = tx._lu_TimeInterval(xwalk_dict)
    xwalk_dict = tx._lu_WindCode(xwalk_dict)
    xwalk_dict = tx._lu_DataProcessingLevel(xwalk_dict)
    xwalk_dict = tx._lu_DetectionType(xwalk_dict)
    xwalk_dict = tx._lu_DistanceClass(xwalk_dict)
    xwalk_dict = tx._lu_GeodeticDatum(xwalk_dict)
    xwalk_dict = tx._lu_Sex(xwalk_dict)
    xwalk_dict = tx._ncrn_Contact(xwalk_dict)
    xwalk_dict = tx._lu_PrecipitationType(xwalk_dict)
    xwalk_dict = tx._ncrn_BirdSpeciesPark(xwalk_dict)
    xwalk_dict = tx._ncrn_BirdGroups(xwalk_dict)
    xwalk_dict = tx._lu_NoiseLevel(xwalk_dict)
    xwalk_dict = tx._ncrn_AuditLog(xwalk_dict)
    xwalk_dict = tx._ncrn_AuditLogDetail(xwalk_dict)
    xwalk_dict = tx._lu_SamplingMethod(xwalk_dict)
    xwalk_dict = tx._lu_Habitat(xwalk_dict)
    xwalk_dict = tx._ncrn_BirdSpecies(xwalk_dict)
    xwalk_dict = tx._ncrn_Site(xwalk_dict)
    xwalk_dict = tx._ncrn_ScannedFile(xwalk_dict)
    xwalk_dict = tx._lu_TemperatureUnit(xwalk_dict)
    xwalk_dict = tx._lu_ExperienceLevel(xwalk_dict)
    xwalk_dict = tx._lu_ProtectedStatus(xwalk_dict)
    xwalk_dict = tx._dbo_Role(xwalk_dict)
    xwalk_dict = tx._dbo_ParkUser(xwalk_dict)
    xwalk_dict = tx._ncrn_ProtocolWindCode(xwalk_dict)
    xwalk_dict = tx._ncrn_ProtocolPrecipitationType(xwalk_dict)

    return xwalk_dict

def _execute_xwalk_exceptions(xwalk_dict:dict) -> dict:
    # tables that require the creation of one-or-more temp tables (e.g., CTE, execution of additional queries, or generation of lookups)
    xwalk_dict = tx._exception_ncrn_DetectionEvent(xwalk_dict)
    xwalk_dict = tx._exception_ncrn_BirdDetection(xwalk_dict)
    xwalk_dict = tx._exception_ncrn_BirdSpecies(xwalk_dict)
    xwalk_dict = tx._exception_ncrn_AuditLogDetail(xwalk_dict)
    xwalk_dict = tx._exception_lu_Habitat(xwalk_dict)
    xwalk_dict = tx._exception_ncrn_Contact(xwalk_dict)
    xwalk_dict = tx._exception_ncrn_Location(xwalk_dict)
    xwalk_dict = tx._exception_ncrn_Site(xwalk_dict)

    # tables that have no equivalent in NCRN's db and require creation
    xwalk_dict = tx._exception_ncrn_BirdSpeciesGroups(xwalk_dict)
    xwalk_dict = tx._exception_ncrn_BirdSpeciesPark(xwalk_dict)
    xwalk_dict = tx._exception_lu_ExperienceLevel(xwalk_dict)
    # xwalk_dict = tx._exception_ncrn_ProtocolDetectionType(xwalk_dict)
    xwalk_dict = tx._exception_ncrn_ScannedFile(xwalk_dict)
    xwalk_dict = tx._exception_lu_TemperatureUnit(xwalk_dict)
    xwalk_dict = tx._exception_lu_ProtectedStatus(xwalk_dict)
    xwalk_dict = tx._exception_dbo_Role(xwalk_dict)
    xwalk_dict = tx._exception_dbo_ParkUser(xwalk_dict)
    xwalk_dict = tx._exception_ncrn_ProtocolWindCode(xwalk_dict)
    xwalk_dict = tx._exception_ncrn_ProtocolPrecipitationType(xwalk_dict)

    return xwalk_dict

def _execute_xwalks(xwalk_dict:dict) -> dict:

    for schema in xwalk_dict.keys():
        for tbl in xwalk_dict[schema].keys():
            xwalk = xwalk_dict[schema][tbl]['xwalk']
            
            # if destination column has a one-to-one source field, execute assignments
            one_to_ones = list(xwalk[xwalk['calculation']=='map_source_to_destination_1_to_1'].destination.values)
            for dest_col in one_to_ones:
                src_col = xwalk[xwalk['destination']==dest_col].source.values[0]
                try:
                    xwalk_dict[schema][tbl]['tbl_load'][dest_col] = xwalk_dict[schema][tbl]['source'][src_col]
                except:
                    print(f"WARNING! 1:1 destination column `dict['{schema}']['{tbl}']['tbl_load']['{dest_col}']` failed because its source column `dict['{schema}']['{tbl}']['source']['{src_col}']` did not resolve correctly. Debug `dict['{schema}']['{tbl}']['xwalk']` in src.tbl_xwalks._{schema}_{tbl}()")

            # if destination column requires calculations, calculate
            mask = (xwalk['calculation']=='calculate_dest_field_from_source_field') & (xwalk['source']!='placeholder') # TODO: DELETE THIS LINE, FOR TESTING ONLY
            # mask = (xwalk['calculation']=='calculate_dest_field_from_source_field') # TODO: KEEP: for production
            calculates = list(xwalk[mask].destination.values)
            for dest_col in calculates:
                src_col = xwalk[xwalk['destination']==dest_col].source.values[0]
                # code_lines = xwalk[xwalk['destination']==dest_col].source.values[0].astype(str).split('$splithere$')
                code_lines = xwalk[xwalk['destination']==dest_col].source.values[0]
                code_lines = code_lines.split('$splithere$')
                for line in code_lines:
                    if line != 'placeholder':
                        # line = line.replace('xwalk_dict', 'testdict') # for debugging
                        try:
                            exec(line)
                        except:
                            print(f"WARNING! Calculated column `dict['{schema}']['{tbl}']['tbl_load']['{dest_col}']`, code line `{line}` failed. Debug `dict['{schema}']['{tbl}']['xwalk']` in src.tbl_xwalks._{schema}_{tbl}()")
            
            # if destination column is blank field, assign blank
            blanks = list(xwalk[xwalk['calculation']=='blank_field'].destination.values)
            for dest_col in blanks:
                src_col = xwalk[xwalk['destination']==dest_col].source.values[0]
                xwalk_dict[schema][tbl]['tbl_load'][dest_col] = np.NaN


        xwalk_dict = tx._add_row_id(xwalk_dict)
    
    return xwalk_dict

def _generate_payload(xwalk_dict:dict) -> dict:
    # TODO: this function should take `tbl_load` and make final changes before loading
    # e.g., `tbl_load` is allowed to hold NCRN's GUIDs but `payload` should either replace the GUIDs with INTs or leave out that column altogether
    return xwalk_dict

def _generate_tsql(xwalk_dict:dict) -> dict:
    # TODO: generate the INSERT INTO sql for each `payload`
    # e.g.,
    # INSERT INTO [NCRN_Landbirds].[lu].[ExperienceLevel] ([ID],[Code],[Label],[Description],[SortOrder]) VALUES (2,'EXP','Expert','An expert',2)

    # review db_loader.load_db.load_option_b() for details

    return xwalk_dict

