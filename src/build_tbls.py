"""build dataframes by querying databases (destination==SQL Server; source==Access)""" 
import pandas as pd
import src.db_connect as dbc
import assets.assets as assets

tbl_list = list(assets.TBL_XWALK.values())
template_list = list(assets.TBL_XWALK.keys())

def _get_dest_tbls(dest:str='assets/templates/templates.pkl', template_list:list=template_list) -> dict:
    """Make empty dataframes with the correct column names and order for each table to be loaded

    Args:
        dest (str, optional): Absolute or relative filepath where you want to save the dictionary of dataframes

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
    for tbl in template_list:
        SQL_QUERY = f"""SELECT TOP 5 * FROM [{assets.LOC_DB}].[dbo].[{tbl}];"""
        template_dict[tbl] = pd.read_sql_query(SQL_QUERY,con)
    
    # close connection
    con.close()

    return template_dict

def _get_src_tbls(tbl_list:list=tbl_list) -> dict:
    """Make empty dataframes with the correct column names and order for each table to be loaded

    Args:
        dest (str, optional): Absolute or relative filepath where you want to save the dictionary of dataframes

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
    for tbl in tbl_list:
        tbl_dict[tbl] = dbc._exec_qry(con=con, qry=f'get_{tbl}')

    # close connection
    con.close()

    return tbl_dict
