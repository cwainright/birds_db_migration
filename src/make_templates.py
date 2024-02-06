"""make the templates to which we crosswalk access tables"""
import assets.assets as assets
import pyodbc
import pandas as pd

conn_str = (
    r'driver={SQL Server};'
    r'server=(local);'
    f'database={assets.LOC_DB};'
    r'trusted_connection=yes;'
    )
con = pyodbc.connect(conn_str)

SQL_QUERY = f"""SELECT TOP 5 * FROM [{assets.LOC_DB}].[dbo].[ncrn.DetectionEvent];"""
df = pd.read_sql_query(SQL_QUERY,con)
df.to_csv('assets/templates/ncrn_DetectionEvent.csv', index=False)

SQL_QUERY = f"""SELECT TOP 5 * FROM [{assets.LOC_DB}].[dbo].[ncrn.BirdDetection];"""
df = pd.read_sql_query(SQL_QUERY,con)
df.to_csv('assets/templates/ncrn_BirdDetection.csv', index=False)

con.close()