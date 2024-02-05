"""Connect to databases"""
import assets.assets as assets
import pyodbc
import pandas as pd

def _db_connect(db:str) -> None:
    assert db in assets.DB_CHOICES, print(f'You entered `{db}`; `db` must be in {assets.DB_CHOICES}')

    if db.lower() == 'local':
        con_str = (
        r'driver={SQL Server};'
        r'server=(local);'
        f'database={assets.LOC_DB};'
        r'trusted_connection=yes;'
        )
    elif db.lower() == 'dev':
        con_str = f'DRIVER={{SQL Server}};SERVER={assets.DEV_SRV};DATABASE={assets.DEV_DB}'
    elif db.lower() == 'access':
        con_str = 'Driver={Microsoft Access Driver (*.mdb, *.accdb)};' + 'DBQ=' + f'{assets.ACC_DB}' + ';'
    try:
        con = pyodbc.connect(con_str)
        return con
    except:
        print('Connection to `{db}` failed. If connecting to "dev", you must be on-network.')

def _exec_qry(con:pyodbc.connect, qry:str) -> pd.DataFrame:
    with open(f'src/qry/{qry}.sql', 'r') as query:
        df = pd.read_sql_query(query.read(),con)
    return df

