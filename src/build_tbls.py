"""build dataframes by querying databases (destination==SQL Server; source==Access)""" 
import pandas as pd
import src.db_connect as dbc
import assets.assets as assets
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

TBL_XWALK = assets.TBL_XWALK
TBL_ADDITIONS = assets.TBL_ADDITIONS

def _get_dest_tbls() -> dict:
    """Make empty dataframes with the correct column names and order for each table to be loaded

    Returns:
        dict: Dictionary of dataframes

    Examples:
        import src.make_templates as bt
        testdict = bt._get_dest_tbls()
    """
    print('Retrieving target schemas...')
    con = dbc._db_connect('local')
    template_dict = {}
    counter = 0
    for schema in TBL_XWALK.keys():
        for tbl in TBL_XWALK[schema].keys():
            SQL_QUERY = f"""SELECT TOP 5 * FROM [{assets.LOC_DB}].[{schema}].[{tbl}];"""
            template_dict[tbl] = pd.read_sql_query(SQL_QUERY,con)
            counter += 1
    for schema in TBL_ADDITIONS.keys():
        for tbl in TBL_ADDITIONS[schema]:
            SQL_QUERY = f"""SELECT TOP 5 * FROM [{assets.LOC_DB}].[{schema}].[{tbl}];"""
            template_dict[tbl] = pd.read_sql_query(SQL_QUERY,con)
            counter += 1
    print(f'Retrieved {counter} target schemas.')
    # close connection
    con.close()

    return template_dict

def _get_src_tbls() -> dict:
    """Make empty dataframes with the correct column names and order for each table to be loaded

    Returns:
        dict: Dictionary of dataframes

    Examples:
        import src.make_templates as bt
        testdict = bt._get_src_tbls()
    """
    # connect to db
    print('Retrieving source data...')
    con = dbc._db_connect('access')
    tbl_dict = {}
    counter = 0
    for schema in TBL_XWALK.keys():
        for tbl in TBL_XWALK[schema].values():
            tbl_dict[tbl] = dbc._exec_qry(con=con, qry=f'get_{tbl}')
            counter += 1
    print(f'Retrieved {counter} source tables.')
    # close connection
    con.close()

    return tbl_dict
