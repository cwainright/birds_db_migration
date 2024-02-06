"""Build SQL server (i.e., destination) tables from Access (i.e., source) tables""" 
import pandas as pd
import numpy as np
import src.db_connect as dbc
import assets.assets as assets

tbl_list = list(assets.TBL_XWALK.keys())

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

def _get_tbls(tbls:list=[], tbl_list:list=tbl_list) -> dict:

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
