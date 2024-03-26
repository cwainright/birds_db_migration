"""Load SQL server tables from dataframes xwalked from Access tables"""
import pandas as pd
import numpy as np
import sqlalchemy as sa
import assets.assets as assets
import pyodbc
import time
import datetime as dt

def load_birds(xwalk_dict:dict) -> list:
    print('')
    print('Loading birds to database...')
    print('')
    print('')
    start_time = time.time()
    
    # connections
    engine = sa.create_engine(assets.SACXN_STR)
    cnxn = pyodbc.connect(assets.PYCXN_STR)
    cursor = cnxn.cursor()
    fails = []
    successes = []
    all_tables = []
    for schema in xwalk_dict.keys():
        for tbl in xwalk_dict[schema].keys():
            target = f"birds['{schema}']['{tbl}']"
            all_tables.append(target)

    #---------------------------------------------------------------------------------------------------------------------
    schemas_to_load = ['lu']
    for schema in schemas_to_load:
        for tbl in xwalk_dict[schema].keys():
            target = f"birds['{schema}']['{tbl}']"
            payload = xwalk_dict[schema][tbl]['payload']
            try:
                payload.to_sql(tbl,engine,index=False,if_exists="append",schema=schema)
                successes.append(target)
            except:
                fails.append(target)
                print(f"FAIL: {target}")
    #---------------------------------------------------------------------------------------------------------------------
    schemas_to_load = ['ncrn']
    independent_tables_sa = ['Park', 'BirdGroups', 'Contact'] # for reasons, some tables just won't load with sqlalchemy but they will load with pyodbc...
    independent_tables_odbc = ['Protocol']

    for schema in schemas_to_load:
        for tbl in independent_tables_sa:
            target = f"birds['{schema}']['{tbl}']"
            payload = xwalk_dict[schema][tbl]['payload']
            try:
                payload.to_sql(tbl,engine,index=False,if_exists="append",schema=schema)
                successes.append(target)
            except:
                fails.append(target)
                print(f"FAIL: {target}")
    
    for schema in schemas_to_load:
        for tbl in independent_tables_odbc:
            target = f"birds['{schema}']['{tbl}']"
            try:
                for line in xwalk_dict[schema][tbl]['tsql'].split('\n'):
                    cursor.execute(line)
                successes.append(target)
            except:
                fails.append(target)
                print(f"FAIL: {target}")
    cnxn.commit()
    
    #---------------------------------------------------------------------------------------------------------------------
    covered_above = [
        'Site'
        ,'ProtocolTimeInterval'
        ,'AuditLog'
        ,'ProtocolDetectionType'
        ,'ProtocolDistanceClass'
        ,'ProtocolNoiseLevel'
        ,'ProtocolWindCode'
        ,'ProtocolPrecipitationType'
        ]
    for schema in schemas_to_load:
        for tbl in covered_above:
            target = f"birds['{schema}']['{tbl}']"
            payload = xwalk_dict[schema][tbl]['payload']
            try:
                payload.to_sql(tbl,engine,index=False,if_exists="append",schema=schema)
                successes.append(target)
            except:
                fails.append(target)
                print(f"FAIL: {target}")
    #---------------------------------------------------------------------------------------------------------------------
    covered_above = [
        'Location'
        ,'AuditLogDetail'
        ]
    for schema in schemas_to_load:
        for tbl in covered_above:
            target = f"birds['{schema}']['{tbl}']"
            payload = xwalk_dict[schema][tbl]['payload']
            try:
                payload.to_sql(tbl,engine,index=False,if_exists="append",schema=schema)
                successes.append(target)
            except:
                fails.append(target)
                print(f"FAIL: {target}")
    
    covered_above = [
        'BirdSpecies'
        ]
    for schema in schemas_to_load:
        for tbl in covered_above:
            target = f"birds['{schema}']['{tbl}']"
            try:
                for line in xwalk_dict[schema][tbl]['tsql'].split('\n'):
                    cursor.execute(line)
                successes.append(target)
            except:
                fails.append(target)
                print(f"FAIL: {target}")
    cnxn.commit()
    #---------------------------------------------------------------------------------------------------------------------
    covered_above = [
        'BirdSpeciesPark'
        ,'BirdSpeciesGroups'
        ]
    for schema in schemas_to_load:
        for tbl in covered_above:
            target = f"birds['{schema}']['{tbl}']"
            payload = xwalk_dict[schema][tbl]['payload']
            try:
                payload.to_sql(tbl,engine,index=False,if_exists="append",schema=schema)
                successes.append(target)
            except:
                fails.append(target)
                print(f"FAIL: {target}")
    #---------------------------------------------------------------------------------------------------------------------
    independent_tables = [
        'Role'
        ]
    schemas_to_load = ['dbo']
    for schema in schemas_to_load:
        for tbl in independent_tables:
            target = f"birds['{schema}']['{tbl}']"
            payload = xwalk_dict[schema][tbl]['payload']
            try:
                payload.to_sql(tbl,engine,index=False,if_exists="append",schema=schema)
                successes.append(target)
            except:
                fails.append(target)
                print(f"FAIL: {target}")
    independent_tables = [
        'User'
        ]
    for schema in schemas_to_load:
        for tbl in independent_tables:
            target = f"birds['{schema}']['{tbl}']"
            try:
                for line in xwalk_dict[schema][tbl]['tsql'].split('\n'):
                    cursor.execute(line)
                successes.append(target)
            except:
                fails.append(target)
                print(f"FAIL: {target}")
    cnxn.commit()
    #---------------------------------------------------------------------------------------------------------------------
    covered_above_dbo = [
        'ParkUser'
        ]
    schemas_to_load = ['dbo']
    for schema in schemas_to_load:
        for tbl in covered_above_dbo:
            target = f"birds['{schema}']['{tbl}']"
            payload = xwalk_dict[schema][tbl]['payload']
            try:
                payload.to_sql(tbl,engine,index=False,if_exists="append",schema=schema)
                successes.append(target)
            except:
                fails.append(target)
                print(f"FAIL: {target}")
    
    covered_above_dbo = [
        'UserRole'
        ]
    for schema in schemas_to_load:
        for tbl in covered_above_dbo:
            target = f"birds['{schema}']['{tbl}']"
            try:
                for line in xwalk_dict[schema][tbl]['tsql'].split('\n'):
                    cursor.execute(line)
                successes.append(target)
            except:
                fails.append(target)
                print(f"FAIL: {target}")
    cnxn.commit()
    #---------------------------------------------------------------------------------------------------------------------
    covered_above = [
        'DetectionEvent'
        ]
    schemas_to_load = ['ncrn']
    for schema in schemas_to_load:
        for tbl in covered_above:
            target = f"birds['{schema}']['{tbl}']"
            try:
                for line in xwalk_dict[schema][tbl]['tsql'].split('\n'):
                    cursor.execute(line)
                successes.append(target)
            except:
                fails.append(target)
                print(f"FAIL: {target}")
    cnxn.commit()
    # #---------------------------------------------------------------------------------------------------------------------
    covered_above = [
        'BirdDetection'
        ]
    schemas_to_load = ['ncrn']
    for schema in schemas_to_load:
        for tbl in covered_above:
            target = f"birds['{schema}']['{tbl}']"
            try:
                for line in xwalk_dict[schema][tbl]['tsql'].split('\n'):
                    cursor.execute(line)
                successes.append(target)
            except:
                fails.append(target)
                print(f"FAIL: {target}")
    cnxn.commit()
    # #---------------------------------------------------------------------------------------------------------------------
    mytables = {
        'fails':fails
        ,'successes':successes
        ,'remaining':[]
        ,'all_tables':all_tables
    }
    # ncrn.ScannedFile is an empty table that'll never be loaded so we need to "mark it off the to-do list"
    scannedfile = "birds['ncrn']['ScannedFile']"
    mytables['successes'].append(scannedfile)
    mytables['remaining'] = [x for x in mytables['all_tables'] if x not in mytables['successes'] and x not in mytables['fails']]

    cnxn.close()
    print(f"SUCCESS: loaded {len(mytables['successes'])} tables")
    if len(mytables['fails'])>0:
        print(f"FAIL: {len(mytables['fails'])} tables")
        for t in mytables['fails']:
            print(f"    {t}")
    if len(mytables['remaining'])>0:
        print(f"REMAINING: {len(mytables['remaining'])} tables")
        for t in mytables['remaining']:
            print(f"    {t}")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    elapsed_time = str(dt.timedelta(seconds=elapsed_time))
    elapsed_time = elapsed_time.split('.')[0]
    print('')
    print(f'`load_birds()` succeeded in: {elapsed_time}')

    return mytables
