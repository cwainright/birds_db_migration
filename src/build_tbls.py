"""build dataframes by querying databases (destination==SQL Server; source==Access)""" 
import pandas as pd
import src.db_connect as dbc
import assets.assets as assets

# tbl_list = list(assets.TBL_XWALK.values())
# template_list = list(assets.TBL_XWALK.keys())
TBL_XWALK = assets.TBL_XWALK

def _get_dest_tbls() -> dict:
    """Make empty dataframes with the correct column names and order for each table to be loaded

    Returns:
        dict: Dictionary of dataframes

    Examples:
        import src.make_templates as bt
        testdict = bt._get_dest_tbls()
        with open('saved_dictionary.pkl', 'rb') as f:
            loaded_dict = pickle.load(f)
    """
    con = dbc._db_connect('local')
    template_dict = {}
    for schema in TBL_XWALK.keys():
        for tbl in TBL_XWALK[schema].keys():
            SQL_QUERY = f"""SELECT TOP 5 * FROM [{assets.LOC_DB}].[{schema}].[{tbl}];"""
            template_dict[tbl] = pd.read_sql_query(SQL_QUERY,con)
    
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
        with open('saved_dictionary.pkl', 'rb') as f:
            loaded_dict = pickle.load(f)
    """
    # connect to db
    con = dbc._db_connect('access')
    tbl_dict = {}
    for schema in TBL_XWALK.keys():
        for tbl in TBL_XWALK[schema].values():
            tbl_dict[tbl] = dbc._exec_qry(con=con, qry=f'get_{tbl}')

    # close connection
    con.close()

    return tbl_dict
