"""Build SQL server tables from Access tables""" 
import pandas as pd
import numpy as np
import src.db_connect as dbc

def build_tbl(tbls:list=[]) -> dict:

    # get the source data
    tbl_dict = _get_tbls(tbls)

    tbl_dict = _xwalk_tbls(tbl_dict)

    

    return tbl_dict

def build_detection_event() -> pd.DataFrame:
    # connect to db
    con = dbc._db_connect('access')
    # execute qry against db
    df = dbc._exec_qry(con=con, qry='get_tbl_events')
    # close connection
    con.close()
    # preprocess
    # xwalk
    # postprocess
    return df

def build_bird_detection() -> pd.DataFrame:
    # connect to db
    con = dbc._db_connect('access')
    # execute qry against db
    df = dbc._exec_qry(con=con, qry='get_tbl_field_data')
    # close connection
    con.close()
    # preprocess
    # xwalk
    # postprocess
    return df

def _get_tbls(tbls:list=[]) -> dict:
    tbl_list = [
        'tbl_events'
        ,'tbl_field_data'
    ]

    if len(tbls)==0:
        tbls = tbl_list.copy() # base case: query all tables
    else:
        no_tbls = [x for x in tbls if x not in tbl_list]
        tbls = [x for x in tbls if x in tbl_list]
        if len(no_tbls)>0:
            for tbl in no_tbls:
                print(f'You provided {tbl} which is not a valid table name.')
    # connect to db
    con = dbc._db_connect('access')
    tbl_dict = {}
    for tbl in tbls:
        tbl_dict[tbl] = dbc._exec_qry(con=con, qry=f'get_{tbl}')

    # close connection
    con.close()

    return tbl_dict

def _xwalk_tbls(tbl_dict:dict) -> dict:
    """Crosswalk source tables to destination table schemas

    Args:
        tbl_dict (dict): A dictionary of dataframes from the source Access db

    Returns:
        dict: `tbl_dict` dataframes crosswalked to their destination schemas
    """

    for tbl in tbl_dict.keys():
        xwalk = pd.read_csv(f'assets/xwalks/{tbl}.csv')

    return tbl_dict