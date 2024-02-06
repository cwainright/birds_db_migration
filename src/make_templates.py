"""make the templates to which we crosswalk access tables"""
import assets.assets as assets
import pyodbc
import pandas as pd
import pickle
import src.build_tbls as bt
import numpy as np

# template_list = assets.DEST_LIST.copy()
template_list = list(assets.TBL_XWALK.keys())

def _make_templates(dest:str='assets/templates/templates.pkl', template_list:list=template_list) -> dict:
    """Make empty dataframes with the correct column names and order for each table to be loaded

    Args:
        dest (str, optional): Absolute or relative filepath where you want to save the dictionary of dataframes

    Returns:
        dict: Dictionary of dataframes

    Examples:
        import src.make_templates as mt
        testdict = mt._make_templates()
        with open('saved_dictionary.pkl', 'rb') as f:
            loaded_dict = pickle.load(f)
    """
    conn_str = (
        r'driver={SQL Server};'
        r'server=(local);'
        f'database={assets.LOC_DB};'
        r'trusted_connection=yes;'
        )
    con = pyodbc.connect(conn_str)
    
    template_dict = {}

    for tbl in template_list:
        try:
            SQL_QUERY = f"""SELECT TOP 5 * FROM [{assets.LOC_DB}].[dbo].[{tbl}];"""
            template_dict[tbl] = pd.read_sql_query(SQL_QUERY,con)
        except:
            print(f'There is no table {tbl}')
    
    # with open(dest, 'wb') as f:
    #     pickle.dump(template_dict, f)

    # SQL_QUERY = f"""SELECT TOP 5 * FROM [{assets.LOC_DB}].[dbo].[ncrn.BirdDetection];"""
    # df = pd.read_sql_query(SQL_QUERY,con)
    # df.to_csv('assets/templates/ncrn_BirdDetection.csv', index=False)

    con.close()

    return template_dict


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
        assert dest.endswith('.pkl'), print(f'You entered `{dest}`. `dest` must end in ".pkl"')

    source_dict = bt._get_tbls()
    dest_dict = _make_templates()

    # create empty structure to receive data
    xwalk_dict = {}
    for tbl in template_list:
        xwalk_dict[tbl] = {
            'xwalk': pd.DataFrame(columns=['source', 'destination'])
            ,'source': pd.DataFrame()
            ,'source_name': assets.TBL_XWALK[tbl]
            ,'destination': dest_dict[tbl]
        }
        xwalk_dict[tbl]['xwalk']['destination'] = xwalk_dict[tbl]['destination'].columns

    # add xwalk to empty structure for each destination table
    xwalk_dict = _detection_event_xwalk(xwalk_dict)
    xwalk_dict = _bird_detection_xwalk(xwalk_dict)

    # add source dataframe to structure for each destination table
    for tbl in template_list:
        source_tbl = xwalk_dict[tbl]['source_name']
        try:
            print(source_tbl)
            xwalk_dict[tbl]['source'] = source_dict[source_tbl]
        except:
            pass

    # xwalk each source dataframe to destination structure, according to xwalk

    # validate
    # TODO: write logic to check that each column we hard-coded into ['xwalk']['source'] and ['xwalk']['destination'] actually exists in the dataframe so we can't go sideways on column reassignment
    
    # save output
    if dest !='':
        with open(dest, 'wb') as f:
            pickle.dump(xwalk_dict, f)
        print(f'Output saved to `{dest}`')
    
    return xwalk_dict

def _detection_event_xwalk(xwalk_dict:dict) -> dict:
    """Crosswalk source.tbl_Events to destination.ncrn.DetectionEvent

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    
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
