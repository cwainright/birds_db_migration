"""make the templates to which we crosswalk access tables"""
import assets.assets as assets
import pandas as pd
import pickle
import src.build_tbls as bt
import src.tbl_xwalks as tx
import numpy as np
import src.db_connect as dbc

# template_list = assets.DEST_LIST.copy()
template_list = list(assets.TBL_XWALK.keys())

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
    if dest !='':
        assert dest.endswith('.pkl'), print(f'You entered `{dest}`. If you want to save the output of `make_xwalks()`, `dest` must end in ".pkl"')

    source_dict = bt._get_src_tbls() # query the source data (i.e., the Access table(s))
    dest_dict = bt._get_dest_tbls() # query the destination data (i.e., the SQL Server table; usually an empty dataframe with the correct columns)

    # main object to hold data
    xwalk_dict = {}
    for tbl in template_list:
        xwalk_dict[tbl] = {
            'xwalk': pd.DataFrame(columns=['destination', 'source', 'calculation', 'note']) # the crosswalk to translate from `source` to `tbl_load`
            ,'source_name': assets.TBL_XWALK[tbl] # name of source table
            ,'source': pd.DataFrame() # source data
            ,'destination': dest_dict[tbl] # destination data (mostly just for its column names and order)
            ,'tbl_load': pd.DataFrame() # `source` data crosswalked to the destination schema
            ,'payload': pd.DataFrame() # `tbl_load` transformed for loading to destination database
            ,'tsql': '' # the t-sql to load the `payload` to the destination table
        }
        xwalk_dict[tbl]['source'] = source_dict[xwalk_dict[tbl]['source_name']] # route the source data to its placeholder
        xwalk_dict[tbl]['tbl_load'] = pd.DataFrame(columns=xwalk_dict[tbl]['destination'].columns)
        xwalk_dict[tbl]['xwalk']['destination'] = xwalk_dict[tbl]['destination'].columns # route the destination columns to their placeholder in the crosswalk

    # create xwalk for each destination table
    xwalk_dict = _create_xwalks(xwalk_dict)

    # execute exception-handling
    xwalk_dict = _execute_xwalk_exceptions(xwalk_dict)

    # validate
    xwalk_dict = _validate_xwalks(xwalk_dict)

    # execute xwalk to generate load
    xwalk_dict = _execute_xwalks(xwalk_dict)

    # validate
    xwalk_dict = _validate_tbl_loads(xwalk_dict)

    # generate payload
    xwalk_dict = _generate_payload(xwalk_dict)

    # validate payload
    xwalk_dict = _validate_payload(xwalk_dict)

    # generate t-sql
    xwalk_dict = _generate_tsql(xwalk_dict)

    # save output
    if dest !='':
        with open(dest, 'wb') as f:
            pickle.dump(xwalk_dict, f)
        print(f'Output saved to `{dest}`')
    
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

    return xwalk_dict

def _execute_xwalk_exceptions(xwalk_dict:dict) -> dict:
    # TODO: if a destination table requires the creation of another table (e.g., CTE, execution of a second query, or generation of a lookup), it doesn't fit our model, so we need to execute an exception
    xwalk_dict = _exception_ncrn_DetectionEvent(xwalk_dict)

    return xwalk_dict

def _exception_ncrn_DetectionEvent(xwalk_dict:dict) -> dict:
    """Exceptions associated with the generation of destination table ncrn.DetectionEvent"""
    # return xwalk_dict

    con = dbc._db_connect('access')
    with open(r'src\qry\qry_long_event_contacts.sql', 'r') as query:
        df = pd.read_sql_query(query.read(),con)
    con.close()

    mysorts = df.groupby(['Event_ID']).size().reset_index(name='count').sort_values(['count'], ascending=True)
    double_events = mysorts[mysorts['count']>1].Event_ID.unique()
    single_events = mysorts[mysorts['count']==1].Event_ID.unique()
    zero_events = mysorts[mysorts['count']==0].Event_ID.unique()
    # len(df) == len(singles) + len(doubles) # sanity check

    # edge case: >1 person was associated with the event
    mask = (df['Event_ID'].isin(double_events)) & (df['Contact_Role']!='Observer')
    df['Contact_Role'] = np.where(mask, 'Recorder', df['Contact_Role'])
    mask = (df['Event_ID'].isin(double_events))
    lookup = df[mask].copy()
    lookup['observer'] = lookup['Contact_ID']
    lookup['recorder'] = lookup['Contact_ID']
    lookup = lookup.drop_duplicates('Event_ID').reset_index()[['Event_ID', 'observer', 'recorder']]
    df = df.merge(lookup, on='Event_ID', how='left')

    # edge case: 0 people were associated with the event
    mask = (df['Event_ID'].isin(zero_events))
    df['observer'] = np.where(mask, np.NaN, df['observer'])
    df['recorder'] = np.where(mask, np.NaN, df['recorder'])

    # base case: one-and-only-one person was associated with the event
    mask = (df['Event_ID'].isin(single_events))
    df['observer'] = np.where(mask, df['Contact_ID'], df['observer'])
    df['recorder'] = np.where(mask, df['Contact_ID'], df['recorder'])

    # cleanup
    df = df.drop_duplicates('Event_ID')
    df = df[['Event_ID', 'observer', 'recorder', 'Position_Title']]
    df.rename(columns={'Event_ID':'event_id'}, inplace=True)

    xwalk_dict['ncrn.DetectionEvent']['source'] = xwalk_dict['ncrn.DetectionEvent']['source'].merge(df, on='event_id', how='left')

    return xwalk_dict

def _execute_xwalks(xwalk_dict:dict) -> dict:
    # TODO: this function should execute the instructions stored in each table's `xwalk` to produce a `tbl_load`

    for tbl in template_list:
        xwalk = xwalk_dict[tbl]['xwalk']
        
        # if destination column has a one-to-one source field, execute assignments
        one_to_ones = list(xwalk[xwalk['calculation']=='map_source_to_destination_1_to_1'].destination.values)
        for dest_col in one_to_ones:
            src_col = xwalk[xwalk['destination']==dest_col].source.values[0]
            xwalk_dict[tbl]['tbl_load'][dest_col] = xwalk_dict[tbl]['source'][src_col]

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
                    # line = line.replace('xwalk_dict', 'testdict')
                    try:
                        exec(line)
                    except:
                        print(f'WARNING! Code line {line} for tbl {tbl}, {dest_col} failed. Debug its xwalk in src.tbl_xwalks()')
        
        # if destination column is blank field, assign blank
        blanks = list(xwalk[xwalk['calculation']=='blank_field'].destination.values)
        for dest_col in blanks:
            src_col = xwalk[xwalk['destination']==dest_col].source.values[0]
            xwalk_dict[tbl]['tbl_load'][dest_col] = np.NaN

    return xwalk_dict

def _validate_tbl_loads(xwalk_dict:dict) -> dict:
    # TODO: this function should check that the `tbl_load` attr produced from each `source` and `xwalk` is valid

    # check that dims of `tbl_load` == dims of `source` (same number of rows and columns)
    xwalk_dict = _validate_dims(xwalk_dict)
    # check that each column in `tbl_load` exists in `destination`
    # check that the column order in `tbl_load` matches that of `destination`
    xwalk_dict = _validate_cols(xwalk_dict)
    # check constraints? may be more work than simply letting sqlserver do the checks
    xwalk_dict = _validate_referential_integrity(xwalk_dict)
    return xwalk_dict


def _validate_xwalks(xwalk_dict:dict) -> dict:
    # TODO: this function should check that the `xwalk` attr produced for each `tbl_load` is valid

    # find and report missing values
    # missing = xwalk_dict['ncrn.DetectionEvent']['xwalk'][xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'].isna()].destination.unique()
    # if len(missing) >0:
    #     for m in missing:
    #         print(f'[\'ncrn.DetectionEvent\'][\'xwalk\'] is missing a `source` value where `destination`==\'{m}\'.')

    # how else can the xwalk go sideways?

    return xwalk_dict

def _validate_dims(xwalk_dict:dict) -> dict:
    # TODO: check that dims of `tbl_load` == dims of `source` (same number of rows and columns)
    return xwalk_dict

def _validate_cols(xwalk_dict:dict) -> dict:
    # TODO: check that each column in `tbl_load` exists in `destination`
    # TODO: check that the column order in `tbl_load` matches that of `destination`
    return xwalk_dict

def _validate_referential_integrity(xwalk_dict:dict) -> dict:
    # TODO: check that the INT id for each GUID lines up among related tables
    return xwalk_dict

def _generate_payload(xwalk_dict:dict) -> dict:
    # TODO: this function should take `tbl_load` and make final changes before loading
    # e.g., `tbl_load` is allowed to hold NCRN's GUIDs but `payload` should either replace the GUIDs with INTs or leave out that column altogether
    return xwalk_dict

def _validate_payload(xwalk_dict:dict) -> dict:
    # TODO: this function should validate `payload`
    # TODO: All `payload`s should have >0 columns; if there are no columns, payload-generation failed.
    return xwalk_dict

def _generate_tsql(xwalk_dict:dict) -> dict:
    # TODO: generate the INSERT INTO sql for each `payload`
    # e.g.,
    # INSERT INTO [NCRN_Landbirds].[dbo].[lu.ExperienceLevel] ([ID],[Code],[Label],[Description],[SortOrder]) VALUES (2,'EXP','Expert','An expert',2)

    # review db_loader.load_db.load_option_b() for details

    return xwalk_dict
