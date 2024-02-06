"""make the templates to which we crosswalk access tables"""
import assets.assets as assets
import pyodbc
import pandas as pd
import pickle
import src.build_tbls as bt
import numpy as np

template_list = [ # add destination tables here as we build them
    'ncrn.DetectionEvent'
    # ,'ncrn.BirdDetection'
    ]

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
    
    with open(dest, 'wb') as f:
        pickle.dump(template_dict, f)

    # SQL_QUERY = f"""SELECT TOP 5 * FROM [{assets.LOC_DB}].[dbo].[ncrn.BirdDetection];"""
    # df = pd.read_sql_query(SQL_QUERY,con)
    # df.to_csv('assets/templates/ncrn_BirdDetection.csv', index=False)

    con.close()

    return template_dict


def make_xwalks(dest:str='assets/templates/xwalks.pkl', template_list:list=template_list) -> dict:
    """Create a dictionary of crosswalks for each table in the source (Access) and destination (SQL Server) databases

    Args:
        dest (str, optional): _description_. Defaults to 'assets/templates/xwalks.pkl'.
        template_list (list, optional): _description_. Defaults to `template_list`.

    Returns:
        dict: a dictionary 

    Examples:
        import src.make_templates as mt
        testdict = mt.make_xwalks()
        with open('saved_dictionary.pkl', 'rb') as f:
            loaded_dict = pickle.load(f) 
    """

    source_dict = bt.build_tbl()
    dest_dict = _make_templates()

    # create empty structure to receive data
    xwalk_dict = {}
    for tbl in template_list:
        xwalk_dict[tbl] = {
            'xwalk': pd.DataFrame(columns=['source', 'destination'])
            ,'source': {}
            ,'destination': dest_dict[tbl]
        }

    # add xwalk to empty structure for each destination table
    xwalk_dict = _detection_event_xwalk(xwalk_dict)

    # add source dataframe to structure for each destination table
    for tbl in template_list:
        source_tbl = list(xwalk_dict[tbl]['source'].keys())[0]
        try:
            print(source_tbl)
            xwalk_dict[tbl]['source'][source_tbl] = source_dict[source_tbl]
        except:
            pass

    # xwalk each source dataframe to destination structure, according to xwalk
    
    
    return xwalk_dict

def _detection_event_xwalk(xwalk_dict:dict) -> dict:
    """Crosswalk source.tbl_field_data to destination.ncrn.BirdDetection

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    
    xwalk_dict['ncrn.DetectionEvent']['source'] = {'tbl_Events': pd.DataFrame()}
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] = xwalk_dict['ncrn.DetectionEvent']['destination'].columns
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'Event_ID', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'LocationID')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'Location_ID', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'ProtocolID')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'Protocol_ID', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'ProtocolID')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'Protocol_ID', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])

    return xwalk_dict

def _bird_detection_xwalk(xwalk_dict:dict) -> dict:
    """Crosswalk source.tbl_Event to destination.ncrn.DetectionEvent

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    xwalk_dict['ncrn.DetectionEvent']['source_tbl_name'] = 'tbl_Field_Data'
    xwalk_dict['ncrn.DetectionEvent']['xwalk']

    return xwalk_dict
