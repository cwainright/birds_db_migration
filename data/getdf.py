"""query and flatten the whole dataset"""

import pandas as pd
import src.db_connect as dbc
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

def _get_df() -> pd.DataFrame:
    # query non-camp
    con = dbc._db_connect('access')
    with open(r'src\qry\get_tbl_Events.sql', 'r') as qry:
        tbl_Events = pd.read_sql_query(qry.read(),con)
    with open(r'src\qry\get_tbl_Field_Data.sql', 'r') as qry:
        tbl_Field_Data = pd.read_sql_query(qry.read(),con)
    with open(r'src\qry\get_tlu_Sex_Code.sql', 'r') as qry:
        tlu_Sex_Code = pd.read_sql_query(qry.read(),con)
    with open(r'src\qry\get_tbl_Locations.sql', 'r') as qry:
        tbl_Locations = pd.read_sql_query(qry.read(),con)
    with open(r'src\qry\get_tlu_interval.sql', 'r') as qry:
        tlu_interval = pd.read_sql_query(qry.read(),con)
    with open(r'src\qry\get_dbo_tlu_Distance_Estimate.sql', 'r') as qry:
        dbo_tlu_Distance_Estimate = pd.read_sql_query(qry.read(),con)
    # close connection
    con.close()

    df = pd.merge(tbl_Events, tbl_Field_Data, left_on='event_id', right_on='Event_ID')
    df = pd.merge(df, tbl_Locations, left_on='location_id', right_on='Location_ID')
    df = pd.merge(df, tlu_interval, left_on='Interval', right_on='Interval')
    df = pd.merge(df, dbo_tlu_Distance_Estimate, on='Distance_id')

    # query camp bird data
    con = dbc._db_connect('c')
    with open(r'src\qry\get_tbl_Events.sql', 'r') as qry:
        tbl_Events = pd.read_sql_query(qry.read(),con)
    with open(r'src\qry\get_tbl_Field_Data.sql', 'r') as qry:
        tbl_Field_Data = pd.read_sql_query(qry.read(),con)
    with open(r'src\qry\get_tlu_Sex_Code.sql', 'r') as qry:
        tlu_Sex_Code = pd.read_sql_query(qry.read(),con)
    with open(r'src\qry\get_tbl_Locations.sql', 'r') as qry:
        tbl_Locations = pd.read_sql_query(qry.read(),con)
    # close connection
    con.close()
    df2 = pd.merge(tbl_Events, tbl_Field_Data, left_on='event_id', right_on='Event_ID')
    df2 = pd.merge(df2, tbl_Locations, left_on='location_id', right_on='Location_ID')
    df2 = pd.merge(df2, tlu_interval, on='Interval')
    df2 = pd.merge(df2, dbo_tlu_Distance_Estimate, on='Distance_id')

    df = pd.concat([df, df2]).reset_index()
    df['GRTS_Order'] = df['GRTS_Order'].astype(int)
    # mock-up with the filepath so we can match against `datasheets.filepath`
    df['filename'] = "birds_"+df['Unit_Code'].str.lower()+"_grts"+df['GRTS_Order'].astype(str)+"_"+df['Date'].astype(str).str.replace('-','')+".pdf"

    return df
