"""
Hard code the crosswalk for each destination table.

A crosswalk is a dataframe that documents where `destination` data come from in the `source` data.
Downstream in this pipeline (`src.make_templates._execute_xwalks()`), the program reads the crosswalk and executes the information stored there.

Crosswalks divide `destination` fields into three types and assign values to three columns for each field:
1. 1:1 fields. The destination field has a matching source field.
    a. `source` captures the source field name
    b. `calculation` is 'map_source_to_destination_1_to_1'.
    c. `note` is usually np.NaN
2. Calculated fields. The destination field is calculated from one or more source fields.
    a. `source` captures the python code to generate the `destination` field.
    b. `calculation` is 'calculate_dest_field_from_source_field'.
    c. `note` is a plain-english explanation of the code in `source`.
3. Blanks. The destination field does not fit into the first two categories and is not required; it has no matching source field and cannot be calculated from source fields.
    These are usually fields NCRN doesn't collect.
    a. `source` is np.NaN.
    b. `calculation` is 'blank_field'.
    c. `note` is 'this field was not collected by NCRN and has no NCRN equivalent'.

"""

import pandas as pd
import numpy as np
import src.db_connect as dbc
import datetime
import assets.assets as assets
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
import re

TBL_XWALK = assets.TBL_XWALK
deletes = assets.DELETES

def _ncrn_DetectionEvent(xwalk_dict:dict) -> dict:
    """Make the crosswalk for source.tbl_Events to destination.ncrn.DetectionEvent

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    
    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'LocationID'
        ,'ProtocolID'
        ,'EnteredBy'
        ,'AirTemperature'
        ,'Notes'
        ,'EnteredDateTime'
        ,'DataProcessingLevelID'
        ,'DataProcessingLevelDate'
        ,'RelativeHumidity'
        # ,'SamplingMethodID'
        ,'Observer_ContactID'
        ,'Recorder_ContactID'
        ,'ProtocolNoiseLevelID'
        ,'ProtocolWindCodeID'
        ,'ProtocolPrecipitationTypeID'
        ,'Observer_ExperienceLevelID'
        ,'StartDateTime'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'event_id', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # LocationID
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'LocationID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'location_id', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # ProtocolID
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'ProtocolID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'protocol_id', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # EnteredBy
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'EnteredBy')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'entered_by', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # AirTemperature
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'AirTemperature')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'temperature', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # Notes
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'Notes')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'event_notes', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # EnteredDateTime
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'EnteredDateTime')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'entered_date', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['note'] =  np.where(mask, 'ncrn.entered_date is collected as a date, not a datetime, so the entered time is unknown', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['note'])
    # DataProcessingLevelID
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'DataProcessingLevelID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'dataprocessinglevelid', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # DataProcessingLevelDate
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'DataProcessingLevelDate')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'dataprocessingleveldate', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # RelativeHumidity
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'RelativeHumidity')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'humidity', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `SamplingMethodID`
    # mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'SamplingMethodID')
    # xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'protocol_name', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `Observer_ContactID`
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'Observer_ContactID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'observer', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `Recorder_ContactID`
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'Recorder_ContactID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'recorder', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `ProtocolNoiseLevelID`: [lu.NoiseLevel]([ID]) # does NCRN capture an equivalent to this?
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'ProtocolNoiseLevelID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'disturbance_level', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `ProtocolWindCodeID`: [lu.WindCode]([ID]) # does NCRN capture an equivalent to this?
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'ProtocolWindCodeID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'wind_speed', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `ProtocolPrecipitationTypeID`: [lu.PrecipitationType]([ID]) # does NCRN capture an equivalent to this?
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'ProtocolPrecipitationTypeID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'sky_condition', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `Observer_ExperienceLevelID`: [lu.ExperienceLevel]([ID])
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'Observer_ExperienceLevelID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'Position_Title', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `StartDateTime`
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'StartDateTime')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'activity_start_datetime', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['note'] =  np.where(mask, "a concatenation of tbl_Events.Date and tbl_Events.start_time", xwalk_dict['ncrn']['DetectionEvent']['xwalk']['note'])

    # Calculated fields
    calculated_fields = [
        'AirTemperatureRecorded'
        ,'IsSampled'
        ,'TemperatureUnitCode'
        ,'ExcludeNote'
        ,'ExcludeEvent'
        ,'SamplingMethodID'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['calculation'])
    # `AirTemperatureRecorded` BIT type (bool as 0 or 1); not a NCRN field. Should be 1 when the row has a value in `AirTemperature`
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'AirTemperatureRecorded')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['DetectionEvent']['tbl_load']['AirTemperatureRecorded']=np.where((xwalk_dict['ncrn']['DetectionEvent']['tbl_load'].AirTemperature.isna()), 0, 1)", xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `IsSampled` BIT type (bool as 0 or 1) I think this is the inverse of src_tbl.is_excluded, which is a boolean in Access with one unique value: [0]
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'IsSampled')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, "xwalk_dict['ncrn']['DetectionEvent']['tbl_load']['IsSampled'] = np.where((xwalk_dict['ncrn']['DetectionEvent']['source']['flaggroup'].isna()), 1, 0)", xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['note'] =  np.where(mask, "BIT type (bool as 0 or 1) correlates to ncrn.tbl_Events.flaggroup, which is a pick-list that defaults to NA. NA indicates a sample was collected.", xwalk_dict['ncrn']['DetectionEvent']['xwalk']['note'])
    # `TemperatureUnitCode`: [lu.TemperatureUnit]([ID]) # does NCRN capture an equivalent to this?
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'TemperatureUnitCode')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, "xwalk_dict['ncrn']['DetectionEvent']['tbl_load']['TemperatureUnitCode'] = 'C'", xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['note'] =  np.where(mask, "temperature collected in celsius", xwalk_dict['ncrn']['DetectionEvent']['xwalk']['note'])
    # ExcludeNote
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'ExcludeNote')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, "xwalk_dict['ncrn']['DetectionEvent']['tbl_load']['ExcludeNote']=xwalk_dict['ncrn']['DetectionEvent']['source']['flaggroup']+': '+xwalk_dict['ncrn']['DetectionEvent']['source']['label']", xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `ExcludeEvent`: this is a 1 or 0 depending on `ExcludeNote`
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'ExcludeEvent')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, "xwalk_dict['ncrn']['DetectionEvent']['tbl_load']['ExcludeEvent'] = np.where((xwalk_dict['ncrn']['DetectionEvent']['source']['label'].isna()), 0, 1)", xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `SamplingMethodID`: this is 'Grassland Bird Monitoring' or 'Forest Bird Monitoring', depending on whether ncrn.DetectionEvent.ProtocolID is 1 or 2
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'SamplingMethodID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, "xwalk_dict['ncrn']['DetectionEvent']['tbl_load']['SamplingMethodID'] = np.where((xwalk_dict['ncrn']['DetectionEvent']['source']['protocol_id']==1), 'Forest Bird Monitoring', 'Grassland Bird Monitoring')", xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['note'] =  np.where(mask, "In some cases, SamplingMethodID was left NULL in source so we need to calculate. This is either 'Grassland Bird Monitoring' or 'Forest Bird Monitoring', depending on whether ncrn.DetectionEvent.ProtocolID is 1 or 2", xwalk_dict['ncrn']['DetectionEvent']['xwalk']['note'])

    # Blanks
    blank_fields = [
        'DataProcessingLevelNote'
        ,'Rowversion'
        ,'UserCode'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['calculation'])
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['note'])

    return xwalk_dict

def _ncrn_BirdDetection(xwalk_dict:dict) -> dict:
    """Crosswalk source.tbl_Event to destination.ncrn.BirdDetection

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'DetectionEventID'
        ,'SexID'
        ,'ProtocolDistanceClassID'
        ,'ProtocolTimeIntervalID'
        ,'BirdSpeciesParkID'
        ,'ProtocolDetectionTypeID'
        ,'ExcludeReason'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['BirdDetection']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['BirdDetection']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['BirdDetection']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'] =  np.where(mask, 'Data_ID', xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'])
    # DetectionEventID
    mask = (xwalk_dict['ncrn']['BirdDetection']['xwalk']['destination'] == 'DetectionEventID')
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'] =  np.where(mask, 'Event_ID', xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'])
    # SexID
    mask = (xwalk_dict['ncrn']['BirdDetection']['xwalk']['destination'] == 'SexID')
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'] =  np.where(mask, 'Sex_ID', xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'])
    # ProtocolDistanceClassID
    mask = (xwalk_dict['ncrn']['BirdDetection']['xwalk']['destination'] == 'ProtocolDistanceClassID')
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'] =  np.where(mask, 'Distance_id', xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'])
    # ProtocolTimeIntervalID
    mask = (xwalk_dict['ncrn']['BirdDetection']['xwalk']['destination'] == 'ProtocolTimeIntervalID')
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'] =  np.where(mask, 'Interval', xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'])
    # BirdSpeciesParkID
    mask = (xwalk_dict['ncrn']['BirdDetection']['xwalk']['destination'] == 'BirdSpeciesParkID')
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'] =  np.where(mask, 'AOU_Code', xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'])
    # ProtocolDetectionTypeID
    mask = (xwalk_dict['ncrn']['BirdDetection']['xwalk']['destination'] == 'ProtocolDetectionTypeID')
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'] =  np.where(mask, 'ID_Method_Code', xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'])
    # ExcludeReason
    mask = (xwalk_dict['ncrn']['BirdDetection']['xwalk']['destination'] == 'ExcludeReason')
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'] =  np.where(mask, 'FlagDescription', xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
        'ExcludeDetection'
        ,'NumberIndividuals'
        ,'DetectionNotes'
        ,'ImportNotes'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['BirdDetection']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['BirdDetection']['xwalk']['source']) 
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['BirdDetection']['xwalk']['calculation'])
    # ExcludeDetection  BIT type (bool as 0 or 1); should be 0 when `DataFlag` == False
    mask = (xwalk_dict['ncrn']['BirdDetection']['xwalk']['destination'] == 'ExcludeDetection')
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['BirdDetection']['tbl_load']['ExcludeDetection']=np.where((xwalk_dict['ncrn']['BirdDetection']['source']['DataFlag']==False), 0, 1)", xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['note'] = np.where(mask, "BIT type (bool as 0 or 1); should be 0 when `DataFlag` == False", xwalk_dict['ncrn']['BirdDetection']['xwalk']['note'])
    # NumberIndividuals  NCRN recorded bird detections as one-row-per-individual in tbl_Field_Data
    mask = (xwalk_dict['ncrn']['BirdDetection']['xwalk']['destination'] == 'NumberIndividuals')
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['BirdDetection']['tbl_load']['NumberIndividuals']=1", xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['note'] = np.where(mask, "NCRN recorded bird detections as one-row-per-individual in tbl_Field_Data", xwalk_dict['ncrn']['BirdDetection']['xwalk']['note'])
    # DetectionNotes  NCRN recorded bird detections as one-row-per-individual in tbl_Field_Data
    mask = (xwalk_dict['ncrn']['BirdDetection']['xwalk']['destination'] == 'DetectionNotes')
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['BirdDetection']['tbl_load']['DetectionNotes']='Previously_Obs: ' + xwalk_dict['ncrn']['BirdDetection']['source']['Previously_Obs'].astype(str)", xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['note'] = np.where(mask, "NCRN's access db has a field `tbl_Field_Data.Previously_Obs` which has no equivalent in sql server. Recording this field as a note to preserve data short-term. Long-term solution is feature-request to add field ncrn.BirdDetection.Previously_Obs", xwalk_dict['ncrn']['BirdDetection']['xwalk']['note'])
    # ImportNotes  NCRN recorded bird detections as one-row-per-individual in tbl_Field_Data
    mask = (xwalk_dict['ncrn']['BirdDetection']['xwalk']['destination'] == 'ImportNotes')
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['BirdDetection']['tbl_load']['ImportNotes']='Initial_Three_Min_Cnt: ' + xwalk_dict['ncrn']['BirdDetection']['source']['Initial_Three_Min_Cnt'].astype(str)", xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['note'] = np.where(mask, "NCRN's access db has a field `tbl_Field_Data.Initial_Three_Min_Cnt` which has no equivalent in sql server. Recording this field as a note to preserve data short-term. Long-term solution is feature-request to add field ncrn.BirdDetection.Initial_Three_Min_Cnt", xwalk_dict['ncrn']['BirdDetection']['xwalk']['note'])

    # Blanks
    blank_fields = [
        'DataProcessingLevelNote'
        ,'Rowversion'
        ,'UserCode'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['BirdDetection']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['BirdDetection']['xwalk']['calculation'])
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn']['BirdDetection']['xwalk']['note'])

    return xwalk_dict

def _ncrn_Protocol(xwalk_dict:dict) -> dict:
    """Crosswalk source.tbl_Protocol to destination.ncrn.Protocol

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'Title'
        # ,'Version'
        ,'EffectiveBeginDate'
        ,'Comments'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['Protocol']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['Protocol']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['Protocol']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['Protocol']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['Protocol']['xwalk']['source'] =  np.where(mask, 'Protocol_ID', xwalk_dict['ncrn']['Protocol']['xwalk']['source'])
    # Title
    mask = (xwalk_dict['ncrn']['Protocol']['xwalk']['destination'] == 'Title')
    xwalk_dict['ncrn']['Protocol']['xwalk']['source'] =  np.where(mask, 'Protocol_Name', xwalk_dict['ncrn']['Protocol']['xwalk']['source'])
    # Version
    # mask = (xwalk_dict['ncrn']['Protocol']['xwalk']['destination'] == 'Version')
    # xwalk_dict['ncrn']['Protocol']['xwalk']['source'] =  np.where(mask, 'Protocol_Version', xwalk_dict['ncrn']['Protocol']['xwalk']['source'])
    # EffectiveBeginDate
    mask = (xwalk_dict['ncrn']['Protocol']['xwalk']['destination'] == 'EffectiveBeginDate')
    xwalk_dict['ncrn']['Protocol']['xwalk']['source'] =  np.where(mask, 'Version_Date', xwalk_dict['ncrn']['Protocol']['xwalk']['source'])
    # Comments
    mask = (xwalk_dict['ncrn']['Protocol']['xwalk']['destination'] == 'Comments')
    xwalk_dict['ncrn']['Protocol']['xwalk']['source'] =  np.where(mask, 'Protocol_Desc', xwalk_dict['ncrn']['Protocol']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
        'IsActive'
        ,'IsDefault'
        ,'ObserverExperienceRequired'
        ,'Version'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['Protocol']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['Protocol']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['Protocol']['xwalk']['calculation'])
    # IsActive  BIT type (bool as 0 or 1); NCRN only stored active protocols in the db
    mask = (xwalk_dict['ncrn']['Protocol']['xwalk']['destination'] == 'IsActive')
    xwalk_dict['ncrn']['Protocol']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['Protocol']['tbl_load']['IsActive']=1", xwalk_dict['ncrn']['Protocol']['xwalk']['source'])
    xwalk_dict['ncrn']['Protocol']['xwalk']['note'] = np.where(mask, "BIT type (bool as 0 or 1); NCRN only stored active protocols in the db", xwalk_dict['ncrn']['Protocol']['xwalk']['note'])
    # IsDefault  BIT type (bool as 0 or 1); NCRN only stored default protocols in the db
    mask = (xwalk_dict['ncrn']['Protocol']['xwalk']['destination'] == 'IsDefault')
    xwalk_dict['ncrn']['Protocol']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['Protocol']['tbl_load']['IsDefault']=1", xwalk_dict['ncrn']['Protocol']['xwalk']['source'])
    xwalk_dict['ncrn']['Protocol']['xwalk']['note'] = np.where(mask, "BIT type (bool as 0 or 1); NCRN only stored default protocols in the db", xwalk_dict['ncrn']['Protocol']['xwalk']['note'])
    # ObserverExperienceRequired  BIT type (bool as 0 or 1); NCRN did not require observer experience
    mask = (xwalk_dict['ncrn']['Protocol']['xwalk']['destination'] == 'ObserverExperienceRequired')
    xwalk_dict['ncrn']['Protocol']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['Protocol']['tbl_load']['ObserverExperienceRequired']=0", xwalk_dict['ncrn']['Protocol']['xwalk']['source'])
    xwalk_dict['ncrn']['Protocol']['xwalk']['note'] = np.where(mask, "BIT type (bool as 0 or 1); NCRN did not require observer experience", xwalk_dict['ncrn']['Protocol']['xwalk']['note'])
    # Version
    mask = (xwalk_dict['ncrn']['Protocol']['xwalk']['destination'] == 'Version')
    xwalk_dict['ncrn']['Protocol']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['Protocol']['tbl_load']['Version']=np.where((xwalk_dict['ncrn']['Protocol']['source']['Protocol_Name']=='Grassland Bird Monitoring'),'G.1.0','F.1.0')", xwalk_dict['ncrn']['Protocol']['xwalk']['source'])
    xwalk_dict['ncrn']['Protocol']['xwalk']['note'] = np.where(mask, "VARCHAR (10); required unique; NCRN stored this as a float that was independent of the protocol name; new schema requires the version to be protocol-dependent", xwalk_dict['ncrn']['Protocol']['xwalk']['note'])

    # Blanks
    blank_fields = [
        'EffectiveEndDate'
        ,'Rowversion'
        ,'Duration'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['Protocol']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['Protocol']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['Protocol']['xwalk']['calculation'])
    xwalk_dict['ncrn']['Protocol']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['Protocol']['xwalk']['source'])
    xwalk_dict['ncrn']['Protocol']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn']['Protocol']['xwalk']['note'])

    return xwalk_dict

def _ncrn_Location(xwalk_dict:dict) -> dict:
    """Crosswalk source.tbl_Locations to destination.ncrn.Location

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'SiteID'
        ,'X_Coord_DD_NAD83'
        ,'Y_Coord_DD_NAD83'
        # ,'GeodeticDatumID'
        ,'EnteredDate'
        # ,'Code'
        ,'Label'
        ,'Notes'

    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['Location']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['Location']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['Location']['xwalk']['source'] =  np.where(mask, 'Location_ID', xwalk_dict['ncrn']['Location']['xwalk']['source'])
    # SiteID
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'] == 'SiteID')
    xwalk_dict['ncrn']['Location']['xwalk']['source'] =  np.where(mask, 'Site_ID', xwalk_dict['ncrn']['Location']['xwalk']['source'])
    # X_Coord_DD_NAD83
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'] == 'X_Coord_DD_NAD83')
    xwalk_dict['ncrn']['Location']['xwalk']['source'] =  np.where(mask, 'Long_WGS84', xwalk_dict['ncrn']['Location']['xwalk']['source'])
    # Y_Coord_DD_NAD83
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'] == 'Y_Coord_DD_NAD83')
    xwalk_dict['ncrn']['Location']['xwalk']['source'] =  np.where(mask, 'Lat_WGS84', xwalk_dict['ncrn']['Location']['xwalk']['source'])
    # # GeodeticDatumID
    # mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'] == 'GeodeticDatumID')
    # xwalk_dict['ncrn']['Location']['xwalk']['source'] =  np.where(mask, 'Datum', xwalk_dict['ncrn']['Location']['xwalk']['source'])
    # EnteredDate
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'] == 'EnteredDate')
    xwalk_dict['ncrn']['Location']['xwalk']['source'] =  np.where(mask, 'Establish_Date', xwalk_dict['ncrn']['Location']['xwalk']['source'])
    # Label - this is the field that shows up in the picklist for users in the web interface
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'] == 'Label')
    xwalk_dict['ncrn']['Location']['xwalk']['source'] =  np.where(mask, 'Plot_Name', xwalk_dict['ncrn']['Location']['xwalk']['source'])
    # Notes
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'] == 'Notes')
    xwalk_dict['ncrn']['Location']['xwalk']['source'] =  np.where(mask, 'Notes', xwalk_dict['ncrn']['Location']['xwalk']['source'])
    xwalk_dict['ncrn']['Location']['xwalk']['note'] = np.where(mask, "VARCHAR (1000) NULL concatenation of attributes present in source and absent from db schema", xwalk_dict['ncrn']['Location']['xwalk']['note'])

    # Calculated fields
    calculated_fields = [
        'HabitatID'
        ,'EnteredBy'
        ,'IsActive'
        ,'IsSensitive'
        # ,'Label'
        ,'Code'
        ,'OldCode'
        ,'GeodeticDatumID'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['Location']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['Location']['xwalk']['source'])
    xwalk_dict['ncrn']['Location']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['Location']['xwalk']['calculation'])
    # HabitatID  INT NOT NULL map from dict lu.Habitat
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'] == 'HabitatID')
    xwalk_dict['ncrn']['Location']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['Location']['tbl_load']['HabitatID']=np.where((xwalk_dict['ncrn']['Location']['source']['Location_Type']=='Grassland'),1,2)", xwalk_dict['ncrn']['Location']['xwalk']['source'])
    xwalk_dict['ncrn']['Location']['xwalk']['note'] = np.where(mask, "INT NOT NULL map from dict lu.Habitat", xwalk_dict['ncrn']['Location']['xwalk']['note'])
    # EnteredBy  VARCHAR (100)  NOT NULL
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'] == 'EnteredBy')
    xwalk_dict['ncrn']['Location']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['Location']['tbl_load']['EnteredBy']='C.Wainright initial data load 2024-02-20'", xwalk_dict['ncrn']['Location']['xwalk']['source'])
    xwalk_dict['ncrn']['Location']['xwalk']['note'] = np.where(mask, "INT NOT NULL map from dict lu.Habitat", xwalk_dict['ncrn']['Location']['xwalk']['note'])
    # IsSensitive  BIT NOT NULL
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'] == 'IsSensitive')
    xwalk_dict['ncrn']['Location']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['Location']['tbl_load']['IsSensitive']=np.where((xwalk_dict['ncrn']['Location']['source']['Plot_Name'].str.contains('CAMP', regex=False)),1,0)", xwalk_dict['ncrn']['Location']['xwalk']['source'])
    xwalk_dict['ncrn']['Location']['xwalk']['note'] = np.where(mask, "BIT NOT NULL, mapped from source.Plot_Name", xwalk_dict['ncrn']['Location']['xwalk']['note'])
    # IsActive  BIT NOT NULL
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'] == 'IsActive')
    xwalk_dict['ncrn']['Location']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['Location']['tbl_load']['IsActive']=np.where((xwalk_dict['ncrn']['Location']['source']['Active']==True)|(xwalk_dict['ncrn']['Location']['source']['Plot_Name'].str.contains('CAMP', regex=False)),1,0)", xwalk_dict['ncrn']['Location']['xwalk']['source'])
    xwalk_dict['ncrn']['Location']['xwalk']['note'] = np.where(mask, "BIT NOT NULL map from dict['ncrn']['Location']['xwalk']['source']['Active']", xwalk_dict['ncrn']['Location']['xwalk']['note'])
    # Code  VARCHAR (255)  NULL,
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'] == 'Code')
    xwalk_dict['ncrn']['Location']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['Location']['tbl_load']['Code']=np.where((xwalk_dict['ncrn']['Location']['source']['Plot_Name'].str.contains('CAMP', regex=False)),xwalk_dict['ncrn']['Location']['source']['Plot_Name'],xwalk_dict['ncrn']['Location']['source']['GRTS_Order'].astype(int).astype(str))", xwalk_dict['ncrn']['Location']['xwalk']['source'])
    xwalk_dict['ncrn']['Location']['xwalk']['note'] = np.where(mask, "VARCHAR (255) NULL, if CAMP site: show plot name, else: show grts number", xwalk_dict['ncrn']['Location']['xwalk']['note'])
    # OldCode  VARCHAR (255)  NULL,
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'] == 'OldCode')
    xwalk_dict['ncrn']['Location']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['Location']['tbl_load']['OldCode']=xwalk_dict['ncrn']['Location']['source']['GRTS_Order'].astype(int).astype(str)", xwalk_dict['ncrn']['Location']['xwalk']['source'])
    xwalk_dict['ncrn']['Location']['xwalk']['note'] = np.where(mask, "VARCHAR (100) NULL, grts code cast to str", xwalk_dict['ncrn']['Location']['xwalk']['note'])
    # GeodeticDatumID  INT
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'] == 'GeodeticDatumID')
    xwalk_dict['ncrn']['Location']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['Location']['tbl_load']['GeodeticDatumID']=2", xwalk_dict['ncrn']['Location']['xwalk']['source']) # recode `ncrn.Location.GeodeticDatumID` from str 'NAD83' to int in birds['lu']['GeodeticDatum']['tbl_load']
    xwalk_dict['ncrn']['Location']['xwalk']['note'] = np.where(mask, "recode ncrn.Location.GeodeticDatumID from str 'NAD83' to int in birds['lu']['GeodeticDatum']['tbl_load']", xwalk_dict['ncrn']['Location']['xwalk']['note'])
    
    # Blanks
    blank_fields = [
        'Rowversion'
        ,'LegacyCode'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['Location']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['Location']['xwalk']['calculation'])
    xwalk_dict['ncrn']['Location']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['Location']['xwalk']['source'])
    xwalk_dict['ncrn']['Location']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn']['Location']['xwalk']['note'])

    return xwalk_dict

def _ncrn_Site(xwalk_dict:dict) -> dict:
    """Crosswalk source.tbl_Sites to destination.ncrn.Site

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'ParkUnitID'
        ,'Label'
        ,'Description'
        ,'SiteNotes'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['Site']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['Site']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['Site']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['Site']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['Site']['xwalk']['source'] =  np.where(mask, 'Site_ID', xwalk_dict['ncrn']['Site']['xwalk']['source'])
    # ParkUnitID
    mask = (xwalk_dict['ncrn']['Site']['xwalk']['destination'] == 'ParkUnitID')
    xwalk_dict['ncrn']['Site']['xwalk']['source'] =  np.where(mask, 'Unit_Code', xwalk_dict['ncrn']['Site']['xwalk']['source'])
    # Label
    mask = (xwalk_dict['ncrn']['Site']['xwalk']['destination'] == 'Label')
    xwalk_dict['ncrn']['Site']['xwalk']['source'] =  np.where(mask, 'Site_Name', xwalk_dict['ncrn']['Site']['xwalk']['source'])
    # Description
    mask = (xwalk_dict['ncrn']['Site']['xwalk']['destination'] == 'Description')
    xwalk_dict['ncrn']['Site']['xwalk']['source'] =  np.where(mask, 'Site_Desc', xwalk_dict['ncrn']['Site']['xwalk']['source'])
    # SiteNotes
    mask = (xwalk_dict['ncrn']['Site']['xwalk']['destination'] == 'SiteNotes')
    xwalk_dict['ncrn']['Site']['xwalk']['source'] =  np.where(mask, 'Site_Notes', xwalk_dict['ncrn']['Site']['xwalk']['source'])
    # IsActive
    mask = (xwalk_dict['ncrn']['Site']['xwalk']['destination'] == 'IsActive')
    xwalk_dict['ncrn']['Site']['xwalk']['source'] =  np.where(mask, 'Active', xwalk_dict['ncrn']['Site']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
        'IsActive'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['Site']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['Site']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['Site']['xwalk']['source'])
    xwalk_dict['ncrn']['Site']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['Site']['xwalk']['calculation'])
    # IsActive  BIT NOT NULL
    mask = (xwalk_dict['ncrn']['Site']['xwalk']['destination'] == 'IsActive')
    xwalk_dict['ncrn']['Site']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['Site']['tbl_load']['IsActive']=np.where((xwalk_dict['ncrn']['Site']['source']['Active']==False),0,1)", xwalk_dict['ncrn']['Site']['xwalk']['source'])
    xwalk_dict['ncrn']['Site']['xwalk']['note'] = np.where(mask, "BIT NOT NULL map from tbl_Locations.Active left-joined to tbl_Sites as dict['ncrn']['Site']['xwalk']['source']['Active']", xwalk_dict['ncrn']['Site']['xwalk']['note'])

    # Blanks
    blank_fields = [
        'ParentSiteID'
        ,'ImportNotes'
        ,'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['Site']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['Site']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['Site']['xwalk']['calculation'])
    xwalk_dict['ncrn']['Site']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['Site']['xwalk']['source'])
    xwalk_dict['ncrn']['Site']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn']['Site']['xwalk']['note'])

    return xwalk_dict

def _ncrn_Park(xwalk_dict:dict) -> dict:
    """Crosswalk source.tlu_Park_Code to destination.ncrn.Park

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    
    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'ParkCode'
        ,'Label'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['Park']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['Park']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['Park']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['Park']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['Park']['xwalk']['source'] =  np.where(mask, 'PARKCODE', xwalk_dict['ncrn']['Park']['xwalk']['source'])
    # ParkCode
    mask = (xwalk_dict['ncrn']['Park']['xwalk']['destination'] == 'ParkCode')
    xwalk_dict['ncrn']['Park']['xwalk']['source'] =  np.where(mask, 'PARKCODE', xwalk_dict['ncrn']['Park']['xwalk']['source'])
    # Label
    mask = (xwalk_dict['ncrn']['Park']['xwalk']['destination'] == 'Label')
    xwalk_dict['ncrn']['Park']['xwalk']['source'] =  np.where(mask, 'PARKNAME', xwalk_dict['ncrn']['Park']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
        'NetworkCode'
        ,'IsActive'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['Park']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['Park']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['Park']['xwalk']['source']) 
    xwalk_dict['ncrn']['Park']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['Park']['xwalk']['calculation'])
    # NetworkCode  varchar(4); 4-character network ID
    mask = (xwalk_dict['ncrn']['Park']['xwalk']['destination'] == 'NetworkCode')
    xwalk_dict['ncrn']['Park']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['Park']['tbl_load']['NetworkCode']='NCRN'", xwalk_dict['ncrn']['Park']['xwalk']['source'])
    xwalk_dict['ncrn']['Park']['xwalk']['note'] = np.where(mask, "varchar(4); 4-character network ID; 'NCRN'", xwalk_dict['ncrn']['Park']['xwalk']['note'])
    # IsActive  BIT not null; NCRN only stored active parks
    mask = (xwalk_dict['ncrn']['Park']['xwalk']['destination'] == 'IsActive')
    xwalk_dict['ncrn']['Park']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['Park']['tbl_load']['IsActive']=1", xwalk_dict['ncrn']['Park']['xwalk']['source'])
    xwalk_dict['ncrn']['Park']['xwalk']['note'] = np.where(mask, "BIT not null; NCRN only stored active parks", xwalk_dict['ncrn']['Park']['xwalk']['note'])

    # Blanks
    blank_fields = [
        'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['Park']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['Park']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['Park']['xwalk']['calculation'])
    xwalk_dict['ncrn']['Park']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['Park']['xwalk']['source'])
    xwalk_dict['ncrn']['Park']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn']['Park']['xwalk']['note'])

    return xwalk_dict

def _lu_TimeInterval(xwalk_dict:dict) -> dict:
    """Crosswalk source.tlu_interval to destination.lu.TimeInterval

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'Code'
        ,'Code'
        ,'Label'
        ,'Summary'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['lu']['TimeInterval']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['lu']['TimeInterval']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['lu']['TimeInterval']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['lu']['TimeInterval']['xwalk']['destination'] == 'ID')
    xwalk_dict['lu']['TimeInterval']['xwalk']['source'] =  np.where(mask, 'Interval', xwalk_dict['lu']['TimeInterval']['xwalk']['source'])
    # Code
    mask = (xwalk_dict['lu']['TimeInterval']['xwalk']['destination'] == 'Code')
    xwalk_dict['lu']['TimeInterval']['xwalk']['source'] =  np.where(mask, 'Interval', xwalk_dict['lu']['TimeInterval']['xwalk']['source'])
    # Label
    mask = (xwalk_dict['lu']['TimeInterval']['xwalk']['destination'] == 'Label')
    xwalk_dict['lu']['TimeInterval']['xwalk']['source'] =  np.where(mask, 'Interval_Length', xwalk_dict['lu']['TimeInterval']['xwalk']['source'])
    # Summary
    mask = (xwalk_dict['lu']['TimeInterval']['xwalk']['destination'] == 'Summary')
    xwalk_dict['lu']['TimeInterval']['xwalk']['source'] =  np.where(mask, 'Interval_Length', xwalk_dict['lu']['TimeInterval']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['lu']['TimeInterval']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['lu']['TimeInterval']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['TimeInterval']['xwalk']['source']) 
    xwalk_dict['lu']['TimeInterval']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['lu']['TimeInterval']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
        'SortOrder'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['lu']['TimeInterval']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['lu']['TimeInterval']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['TimeInterval']['xwalk']['calculation'])
    xwalk_dict['lu']['TimeInterval']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['TimeInterval']['xwalk']['source'])
    xwalk_dict['lu']['TimeInterval']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['lu']['TimeInterval']['xwalk']['note'])

    return xwalk_dict

def _lu_WindCode(xwalk_dict:dict) -> dict:
    """Crosswalk source.tlu_Wind_Code to destination.lu.WindCode

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'Code'
        ,'Description'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['lu']['WindCode']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['lu']['WindCode']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['lu']['WindCode']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['lu']['WindCode']['xwalk']['destination'] == 'ID')
    xwalk_dict['lu']['WindCode']['xwalk']['source'] =  np.where(mask, 'Wind_Code', xwalk_dict['lu']['WindCode']['xwalk']['source'])
    # Code
    mask = (xwalk_dict['lu']['WindCode']['xwalk']['destination'] == 'Code')
    xwalk_dict['lu']['WindCode']['xwalk']['source'] =  np.where(mask, 'Wind_Code', xwalk_dict['lu']['WindCode']['xwalk']['source'])
    # Description
    mask = (xwalk_dict['lu']['WindCode']['xwalk']['destination'] == 'Description')
    xwalk_dict['lu']['WindCode']['xwalk']['source'] =  np.where(mask, 'Wind_Code_Description', xwalk_dict['lu']['WindCode']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
        'Label'
        ,'WindSpeedLabel_mph'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['lu']['WindCode']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['lu']['WindCode']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['WindCode']['xwalk']['source']) 
    xwalk_dict['lu']['WindCode']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['lu']['WindCode']['xwalk']['calculation'])
    # Label varchar(25) string-split tlu_Wind_Code.Wind_Code_Description
    mask = (xwalk_dict['lu']['WindCode']['xwalk']['destination'] == 'Label')
    xwalk_dict['lu']['WindCode']['xwalk']['source'] = np.where(mask, "xwalk_dict['lu']['WindCode']['tbl_load']['Label'] = xwalk_dict['lu']['WindCode']['source'].Wind_Code_Description.str.split(')').str[0].str.replace('(','',regex=False).str.replace(')','',regex=False).str.replace('movement','mvmnt',regex=False)", xwalk_dict['lu']['WindCode']['xwalk']['source'])
    xwalk_dict['lu']['WindCode']['xwalk']['note'] = np.where(mask, "varchar(25) string-split tlu_Wind_Code.Wind_Code_Description", xwalk_dict['lu']['WindCode']['xwalk']['note'])
    # WindSpeedLabel_mph varchar(20) string-split tlu_Wind_Code.Wind_Code_Description
    mask = (xwalk_dict['lu']['WindCode']['xwalk']['destination'] == 'WindSpeedLabel_mph')
    xwalk_dict['lu']['WindCode']['xwalk']['source'] = np.where(mask, "xwalk_dict['lu']['WindCode']['tbl_load']['WindSpeedLabel_mph'] = xwalk_dict['lu']['WindCode']['source'].Wind_Code_Description.str.split(')').str[0].str.split('(').str[1].str.replace(' mph','',regex=False)", xwalk_dict['lu']['WindCode']['xwalk']['source'])
    xwalk_dict['lu']['WindCode']['xwalk']['note'] = np.where(mask, "varchar(20) string-split tlu_Wind_Code.Wind_Code_Description", xwalk_dict['lu']['WindCode']['xwalk']['note'])

    # WindSpeedLabel_mph

    # Blanks
    blank_fields = [
        'SortOrder'
        ,'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['lu']['WindCode']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['lu']['WindCode']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['WindCode']['xwalk']['calculation'])
    xwalk_dict['lu']['WindCode']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['WindCode']['xwalk']['source'])
    xwalk_dict['lu']['WindCode']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['lu']['WindCode']['xwalk']['note'])

    return xwalk_dict

def _lu_DataProcessingLevel(xwalk_dict:dict) -> dict:
    """Crosswalk source.DataProcessingLevel to destination.lu.DataProcessingLevel

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'Code'
        ,'Label'
        ,'Summary'
        ,'IsActive'
        ,'ProcessOrder'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['lu']['DataProcessingLevel']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['lu']['DataProcessingLevel']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['lu']['DataProcessingLevel']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['lu']['DataProcessingLevel']['xwalk']['destination'] == 'ID')
    xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source'])
    # Code
    mask = (xwalk_dict['lu']['DataProcessingLevel']['xwalk']['destination'] == 'Code')
    xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source'] =  np.where(mask, 'Code', xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source'])
    # Label
    mask = (xwalk_dict['lu']['DataProcessingLevel']['xwalk']['destination'] == 'Label')
    xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source'] =  np.where(mask, 'Label', xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source'])
    # Summary
    mask = (xwalk_dict['lu']['DataProcessingLevel']['xwalk']['destination'] == 'Summary')
    xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source'] =  np.where(mask, 'Summary', xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source'])
    # IsActive
    mask = (xwalk_dict['lu']['DataProcessingLevel']['xwalk']['destination'] == 'IsActive')
    xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source'] =  np.where(mask, 'IsActive', xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source'])
    # ProcessOrder
    mask = (xwalk_dict['lu']['DataProcessingLevel']['xwalk']['destination'] == 'ProcessOrder')
    xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source'] =  np.where(mask, 'ProcessOrder', xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['lu']['DataProcessingLevel']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source']) 
    xwalk_dict['lu']['DataProcessingLevel']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['lu']['DataProcessingLevel']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['lu']['DataProcessingLevel']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['lu']['DataProcessingLevel']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['DataProcessingLevel']['xwalk']['calculation'])
    xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source'])
    xwalk_dict['lu']['DataProcessingLevel']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['lu']['DataProcessingLevel']['xwalk']['note'])

    return xwalk_dict

def _lu_DetectionType(xwalk_dict:dict) -> dict:
    """Crosswalk source.tlu_Bird_ID_Method to destination.lu.DetectionType

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    # 1:1 fields
    one_to_one_fields = [
        # 'ID'
        'Code'
        ,'Label'
        ,'Description'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['lu']['DetectionType']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['lu']['DetectionType']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['lu']['DetectionType']['xwalk']['calculation'])
    # ID
    # mask = (xwalk_dict['lu']['DetectionType']['xwalk']['destination'] == 'ID')
    # xwalk_dict['lu']['DetectionType']['xwalk']['source'] =  np.where(mask, 'ID_Code', xwalk_dict['lu']['DetectionType']['xwalk']['source'])
    # Code
    mask = (xwalk_dict['lu']['DetectionType']['xwalk']['destination'] == 'Code')
    xwalk_dict['lu']['DetectionType']['xwalk']['source'] =  np.where(mask, 'ID_Code', xwalk_dict['lu']['DetectionType']['xwalk']['source'])
    # Label
    mask = (xwalk_dict['lu']['DetectionType']['xwalk']['destination'] == 'Label')
    xwalk_dict['lu']['DetectionType']['xwalk']['source'] =  np.where(mask, 'ID_Text', xwalk_dict['lu']['DetectionType']['xwalk']['source'])
    # Description
    mask = (xwalk_dict['lu']['DetectionType']['xwalk']['destination'] == 'Description')
    xwalk_dict['lu']['DetectionType']['xwalk']['source'] =  np.where(mask, 'ID_Text', xwalk_dict['lu']['DetectionType']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
        'ID'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['lu']['DetectionType']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['lu']['DetectionType']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['DetectionType']['xwalk']['source']) 
    xwalk_dict['lu']['DetectionType']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['lu']['DetectionType']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['lu']['DetectionType']['xwalk']['destination'] == 'ID')
    xwalk_dict['lu']['DetectionType']['xwalk']['source'] = np.where(mask, "xwalk_dict['lu']['DetectionType']['tbl_load']['ID'] = xwalk_dict['lu']['DetectionType']['tbl_load'].index+1", xwalk_dict['lu']['DetectionType']['xwalk']['source'])
    xwalk_dict['lu']['DetectionType']['xwalk']['note'] = np.where(mask, "generate an int ID for a lookup table that used str logical keys in source", xwalk_dict['lu']['DetectionType']['xwalk']['note'])

    # Blanks
    blank_fields = [
        'SortOrder'
        ,'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['lu']['DetectionType']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['lu']['DetectionType']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['DetectionType']['xwalk']['calculation'])
    xwalk_dict['lu']['DetectionType']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['DetectionType']['xwalk']['source'])
    xwalk_dict['lu']['DetectionType']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['lu']['DetectionType']['xwalk']['note'])

    return xwalk_dict

def _lu_DistanceClass(xwalk_dict:dict) -> dict:
    """Crosswalk source.dbo_tlu_Distance_Estimate to destination.lu.DistanceClass

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'Code'
        ,'Label'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['lu']['DistanceClass']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['lu']['DistanceClass']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['lu']['DistanceClass']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['lu']['DistanceClass']['xwalk']['destination'] == 'ID')
    xwalk_dict['lu']['DistanceClass']['xwalk']['source'] =  np.where(mask, 'Distance_id', xwalk_dict['lu']['DistanceClass']['xwalk']['source'])
    # Code
    mask = (xwalk_dict['lu']['DistanceClass']['xwalk']['destination'] == 'Code')
    xwalk_dict['lu']['DistanceClass']['xwalk']['source'] =  np.where(mask, 'Distance_id', xwalk_dict['lu']['DistanceClass']['xwalk']['source'])
    # Label
    mask = (xwalk_dict['lu']['DistanceClass']['xwalk']['destination'] == 'Label')
    xwalk_dict['lu']['DistanceClass']['xwalk']['source'] =  np.where(mask, 'Distance_Text', xwalk_dict['lu']['DistanceClass']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
        'Description'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['lu']['DistanceClass']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['lu']['DistanceClass']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['DistanceClass']['xwalk']['source']) 
    xwalk_dict['lu']['DistanceClass']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['lu']['DistanceClass']['xwalk']['calculation'])
    # Description varchar(100) concat dbo_tlu_Distance_Estimate.ActiveDate and dbo_tlu_Distance_Estimate.RetireDate
    mask = (xwalk_dict['lu']['DistanceClass']['xwalk']['destination'] == 'Description')
    xwalk_dict['lu']['DistanceClass']['xwalk']['source'] = np.where(mask, "xwalk_dict['lu']['DistanceClass']['tbl_load']['Description'] = 'ActiveDate:' + xwalk_dict['lu']['DistanceClass']['source'].ActiveDate.astype(str) + ';' + 'RetireDate:' + xwalk_dict['lu']['DistanceClass']['source'].RetireDate.astype(str)", xwalk_dict['lu']['DistanceClass']['xwalk']['source'])
    xwalk_dict['lu']['DistanceClass']['xwalk']['note'] = np.where(mask, "varchar(100) concat dbo_tlu_Distance_Estimate.ActiveDate and dbo_tlu_Distance_Estimate.RetireDate", xwalk_dict['lu']['DistanceClass']['xwalk']['note'])

    # Blanks
    blank_fields = [
        'SortOrder'
        ,'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['lu']['DistanceClass']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['lu']['DistanceClass']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['DistanceClass']['xwalk']['calculation'])
    xwalk_dict['lu']['DistanceClass']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['DistanceClass']['xwalk']['source'])
    xwalk_dict['lu']['DistanceClass']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['lu']['DistanceClass']['xwalk']['note'])

    return xwalk_dict

def _lu_GeodeticDatum(xwalk_dict:dict) -> dict:
    """Crosswalk source.tlu_Datum to destination.lu.GeodeticDatum

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'Code'
        ,'Label'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['lu']['GeodeticDatum']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['lu']['GeodeticDatum']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['lu']['GeodeticDatum']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['lu']['GeodeticDatum']['xwalk']['destination'] == 'ID')
    xwalk_dict['lu']['GeodeticDatum']['xwalk']['source'] =  np.where(mask, 'Datum_ID', xwalk_dict['lu']['GeodeticDatum']['xwalk']['source'])
    # Code
    mask = (xwalk_dict['lu']['GeodeticDatum']['xwalk']['destination'] == 'Code')
    xwalk_dict['lu']['GeodeticDatum']['xwalk']['source'] =  np.where(mask, 'Datum_ID', xwalk_dict['lu']['GeodeticDatum']['xwalk']['source'])
    # Label
    mask = (xwalk_dict['lu']['GeodeticDatum']['xwalk']['destination'] == 'Label')
    xwalk_dict['lu']['GeodeticDatum']['xwalk']['source'] =  np.where(mask, 'Datum', xwalk_dict['lu']['GeodeticDatum']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['lu']['GeodeticDatum']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['lu']['GeodeticDatum']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['GeodeticDatum']['xwalk']['source']) 
    xwalk_dict['lu']['GeodeticDatum']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['lu']['GeodeticDatum']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
        'Summary'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['lu']['GeodeticDatum']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['lu']['GeodeticDatum']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['GeodeticDatum']['xwalk']['calculation'])
    xwalk_dict['lu']['GeodeticDatum']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['GeodeticDatum']['xwalk']['source'])
    xwalk_dict['lu']['GeodeticDatum']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['lu']['GeodeticDatum']['xwalk']['note'])

    return xwalk_dict

def _lu_Sex(xwalk_dict:dict) -> dict:
    """Crosswalk source.tlu_Sex_Code to destination.lu.Sex

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'Code'
        ,'Label'
        ,'Description'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['lu']['Sex']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['lu']['Sex']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['lu']['Sex']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['lu']['Sex']['xwalk']['destination'] == 'ID')
    xwalk_dict['lu']['Sex']['xwalk']['source'] =  np.where(mask, 'Sex_Code_Value', xwalk_dict['lu']['Sex']['xwalk']['source'])
    # Code
    mask = (xwalk_dict['lu']['Sex']['xwalk']['destination'] == 'Code')
    xwalk_dict['lu']['Sex']['xwalk']['source'] =  np.where(mask, 'Sex_Code', xwalk_dict['lu']['Sex']['xwalk']['source'])
    # Label
    mask = (xwalk_dict['lu']['Sex']['xwalk']['destination'] == 'Label')
    xwalk_dict['lu']['Sex']['xwalk']['source'] =  np.where(mask, 'Sex_Code_Description', xwalk_dict['lu']['Sex']['xwalk']['source'])
    # Description
    mask = (xwalk_dict['lu']['Sex']['xwalk']['destination'] == 'Description')
    xwalk_dict['lu']['Sex']['xwalk']['source'] =  np.where(mask, 'Sex_Code_Description', xwalk_dict['lu']['Sex']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['lu']['Sex']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['lu']['Sex']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['Sex']['xwalk']['source']) 
    xwalk_dict['lu']['Sex']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['lu']['Sex']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
        'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['lu']['Sex']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['lu']['Sex']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['Sex']['xwalk']['calculation'])
    xwalk_dict['lu']['Sex']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['Sex']['xwalk']['source'])
    xwalk_dict['lu']['Sex']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['lu']['Sex']['xwalk']['note'])

    return xwalk_dict

def _exception_lu_Sex(xwalk_dict:dict) -> dict:
    # EXCEPTION 1: remove deprecated "Fledglings" choice from picklist
    # Sex_Code == 'J' refers to 'Fledglihgs'
    # NCRN has never recorded this choice
    # Further, 'Fledglings' are not a choice on the paper datasheet that field crews take into the field
    # I have no idea why this is here but we don't use it, never have, and it's inconsistent with NETNMIDN, so I'm deprecating it
    xwalk_dict['lu']['Sex']['source'] = xwalk_dict['lu']['Sex']['source'][xwalk_dict['lu']['Sex']['source']['Sex_Code']!='J']
    xwalk_dict['lu']['Sex']['source'] = xwalk_dict['lu']['Sex']['source'].sort_values('Sex_Code_Value', ascending=True)
    xwalk_dict['lu']['Sex']['source'].reset_index(drop=True, inplace=True)

    return xwalk_dict

def _ncrn_Contact(xwalk_dict:dict) -> dict:
    """Crosswalk source.tlu_Contacts to destination.ncrn.Contact

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    # use tlu_Contacts.Position_Title as experience level
    # if tlu_Contacts.Position_Title as experience level

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'IsActive'
        ,'LastName'
        ,'FirstName'
        ,'Organization'
        ,'ExperienceLevelID'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['Contact']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['Contact']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['Contact']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['Contact']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['Contact']['xwalk']['source'] =  np.where(mask, 'Contact_ID', xwalk_dict['ncrn']['Contact']['xwalk']['source'])
    # IsActive
    mask = (xwalk_dict['ncrn']['Contact']['xwalk']['destination'] == 'IsActive')
    xwalk_dict['ncrn']['Contact']['xwalk']['source'] =  np.where(mask, 'Active_Contact', xwalk_dict['ncrn']['Contact']['xwalk']['source'])
    # LastName
    mask = (xwalk_dict['ncrn']['Contact']['xwalk']['destination'] == 'LastName')
    xwalk_dict['ncrn']['Contact']['xwalk']['source'] =  np.where(mask, 'Last_Name', xwalk_dict['ncrn']['Contact']['xwalk']['source'])
    # FirstName
    mask = (xwalk_dict['ncrn']['Contact']['xwalk']['destination'] == 'FirstName')
    xwalk_dict['ncrn']['Contact']['xwalk']['source'] =  np.where(mask, 'First_Name', xwalk_dict['ncrn']['Contact']['xwalk']['source'])
    # Organization
    mask = (xwalk_dict['ncrn']['Contact']['xwalk']['destination'] == 'Organization')
    xwalk_dict['ncrn']['Contact']['xwalk']['source'] =  np.where(mask, 'Organization', xwalk_dict['ncrn']['Contact']['xwalk']['source'])
    # ExperienceLevelID
    mask = (xwalk_dict['ncrn']['Contact']['xwalk']['destination'] == 'ExperienceLevelID')
    xwalk_dict['ncrn']['Contact']['xwalk']['source'] =  np.where(mask, 'ExperienceLevelID', xwalk_dict['ncrn']['Contact']['xwalk']['source'])
    xwalk_dict['ncrn']['Contact']['xwalk']['note'] = np.where(mask, "mapped from lu.ExperienceLevel via tbl_xwalks._exception_ncrn_Contact()", xwalk_dict['ncrn']['Contact']['xwalk']['note'])

    # Calculated fields
    calculated_fields = [
        'Notes'
        ,'NetworkCode'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['Contact']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['Contact']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['Contact']['xwalk']['source']) 
    xwalk_dict['ncrn']['Contact']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['Contact']['xwalk']['calculation'])
    # Notes
    mask = (xwalk_dict['ncrn']['Contact']['xwalk']['destination'] == 'Notes')
    xwalk_dict['ncrn']['Contact']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['Contact']['tbl_load']['Notes']='Email_Address:'+xwalk_dict['ncrn']['Contact']['source']['Email_Address'].astype(str)+';Work_Phone:'+xwalk_dict['ncrn']['Contact']['source']['Work_Phone'].astype(str)+';Contact_Notes:'+xwalk_dict['ncrn']['Contact']['source']['Contact_Notes'].astype(str)", xwalk_dict['ncrn']['Contact']['xwalk']['source'])
    xwalk_dict['ncrn']['Contact']['xwalk']['note'] = np.where(mask, "VARCHAR (100) NULL; concatenation of fields present in ncrn source and absent in db schema", xwalk_dict['ncrn']['Contact']['xwalk']['note'])
    # NetworkCode
    mask = (xwalk_dict['ncrn']['Contact']['xwalk']['destination'] == 'NetworkCode')
    xwalk_dict['ncrn']['Contact']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['Contact']['tbl_load']['NetworkCode']='NCRN'", xwalk_dict['ncrn']['Contact']['xwalk']['source'])
    xwalk_dict['ncrn']['Contact']['xwalk']['note'] = np.where(mask, "VARCHAR(4) NOT NULL; hard-code to NCRN", xwalk_dict['ncrn']['Contact']['xwalk']['note'])

    # Blanks
    blank_fields = [
        'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['Contact']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['Contact']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['Contact']['xwalk']['calculation'])
    xwalk_dict['ncrn']['Contact']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['Contact']['xwalk']['source'])
    xwalk_dict['ncrn']['Contact']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn']['Contact']['xwalk']['note'])

    return xwalk_dict

def _lu_PrecipitationType(xwalk_dict:dict) -> dict:
    """Crosswalk source.tlu_Sky_Code to destination.lu.PrecipitationType

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'Code'
        ,'Label'
        ,'Description'
        ,'SortOrder'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['lu']['PrecipitationType']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['lu']['PrecipitationType']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['lu']['PrecipitationType']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['lu']['PrecipitationType']['xwalk']['destination'] == 'ID')
    xwalk_dict['lu']['PrecipitationType']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['lu']['PrecipitationType']['xwalk']['source'])
    # Code
    mask = (xwalk_dict['lu']['PrecipitationType']['xwalk']['destination'] == 'Code')
    xwalk_dict['lu']['PrecipitationType']['xwalk']['source'] =  np.where(mask, 'Code', xwalk_dict['lu']['PrecipitationType']['xwalk']['source'])
    # Label
    mask = (xwalk_dict['lu']['PrecipitationType']['xwalk']['destination'] == 'Label')
    xwalk_dict['lu']['PrecipitationType']['xwalk']['source'] =  np.where(mask, 'Label', xwalk_dict['lu']['PrecipitationType']['xwalk']['source'])
    # Description
    mask = (xwalk_dict['lu']['PrecipitationType']['xwalk']['destination'] == 'Description')
    xwalk_dict['lu']['PrecipitationType']['xwalk']['source'] =  np.where(mask, 'Description', xwalk_dict['lu']['PrecipitationType']['xwalk']['source'])
    # SortOrder
    mask = (xwalk_dict['lu']['PrecipitationType']['xwalk']['destination'] == 'SortOrder')
    xwalk_dict['lu']['PrecipitationType']['xwalk']['source'] =  np.where(mask, 'SortOrder', xwalk_dict['lu']['PrecipitationType']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['lu']['PrecipitationType']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['lu']['PrecipitationType']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['PrecipitationType']['xwalk']['source']) 
    xwalk_dict['lu']['PrecipitationType']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['lu']['PrecipitationType']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
        'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['lu']['PrecipitationType']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['lu']['PrecipitationType']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['PrecipitationType']['xwalk']['calculation'])
    xwalk_dict['lu']['PrecipitationType']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['PrecipitationType']['xwalk']['source'])
    xwalk_dict['lu']['PrecipitationType']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['lu']['PrecipitationType']['xwalk']['note'])

    return xwalk_dict

def _exception_lu_PrecipitationType(xwalk_dict:dict) -> dict:
    """Add codes that NETNMIDN use but NCRN didn't historically use"""

    df = pd.read_csv(assets.PRECIPTYPE)
    xwalk_dict['lu']['PrecipitationType']['source'] = df.copy()
    xwalk_dict['lu']['PrecipitationType']['source_name'] = assets.PRECIPTYPE

    return xwalk_dict

def _ncrn_BirdSpeciesPark(xwalk_dict:dict) -> dict:
    """Crosswalk source.tlu_Species to destination.ncrn.BirdSpeciesPark

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
        
    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'BirdSpeciesID'
        ,'ParkID'
        ,'ProtectedStatusID'
        ,'Comment'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['source'])
    # BirdSpeciesID
    mask = (xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['destination'] == 'BirdSpeciesID')
    xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['source'] =  np.where(mask, 'BirdSpeciesID', xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['source'])
    # ParkID
    mask = (xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['destination'] == 'ParkID')
    xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['source'] =  np.where(mask, 'ParkID', xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['source'])
    # ProtectedStatusID
    mask = (xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['destination'] == 'ProtectedStatusID')
    xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['source'] =  np.where(mask, 'ProtectedStatusID', xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['source'])
    # Comment
    mask = (xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['destination'] == 'Comment')
    xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['source'] =  np.where(mask, 'Comment', xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['source'])


    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['calculation'])
    xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn']['BirdSpeciesPark']['xwalk']['note'])

    return xwalk_dict

def _ncrn_BirdGroups(xwalk_dict:dict) -> dict:
    """Crosswalk source.dbo_tlu_Guild_Assignment to destination.ncrn.BirdGroups

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    # ncrn.BirdGroups
    # looks like dbo_tlu_Guild_Assignment LEFT JOIN dbo_tbl_Guilds?
    # dbo_tlu_Guild_Assignment.Guild_Assignment_ID == dbo.ncrn.BirdGroups.ID
    # dbo_tlu_GuildAssignment.Guild_Level == dbo.ncrn.BirdGroups.GroupName
    # dbo.ncrn.BirdGroups.IsGuild = 1 --BIT; all of our groups are guilds
    # dbo_tlu_GuildAssignment.Active == dbo.ncrn.BirdGroups.IsActive
    # dbo_tbl_Guilds.Integrity_Element + dbo_tbl_Guilds.Guild_Name == dbo.ncrn.BirdGroups.Comment

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'GroupName'
        ,'IsActive'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['BirdGroups']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] =  np.where(mask, 'Guild_Assignment_ID', xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'])
    # GroupName
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'] == 'GroupName')
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] =  np.where(mask, 'Guild_Level', xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'])
    # IsActive
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'] == 'IsActive')
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] =  np.where(mask, 'Active', xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'])


    # Calculated fields
    calculated_fields = [
        'IsGuild'
        ,'Comment'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['BirdGroups']['xwalk']['source']) 
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['BirdGroups']['xwalk']['calculation'])
    # IsGuild
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'] == 'IsGuild')
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] =  np.where(mask, "xwalk_dict['ncrn']['BirdGroups']['tbl_load']['IsGuild']=np.where(xwalk_dict['ncrn']['BirdGroups']['source']['GuildCategory'].isna(),0,1)", xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['note'] =  np.where(mask, 'tbd', xwalk_dict['ncrn']['BirdGroups']['xwalk']['note'])
    # Comment
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'] == 'Comment')
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] =  np.where(mask, "xwalk_dict['ncrn']['BirdGroups']['tbl_load']['Comment'] = 'Description:'+xwalk_dict['ncrn']['BirdGroups']['source']['Guild_Description'].astype(str)+';Code:'+xwalk_dict['ncrn']['BirdGroups']['source']['GuildCode'].astype(str)+';Integrity_Element:'+xwalk_dict['ncrn']['BirdGroups']['source']['Integrity_Element'].astype(str)+';GuildCategory:'+xwalk_dict['ncrn']['BirdGroups']['source']['GuildCategory'].astype(str)", xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['note'] =  np.where(mask, 'VARCHAR(MAX); a concatenation of all the leftover attributes from dbo_tlu_Guild_Assignment LEFT JOIN dbo_tbl_Guilds ON dbo_tlu_Guild_Assignment.Guild_ID = dbo_tbl_Guilds.Guild_ID', xwalk_dict['ncrn']['BirdGroups']['xwalk']['note'])

    # Blanks
    blank_fields = [
        'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['BirdGroups']['xwalk']['calculation'])
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn']['BirdGroups']['xwalk']['note'])

    return xwalk_dict

def _ncrn_BirdSpecies(xwalk_dict:dict) -> dict:
    """Crosswalk source.tlu_Species to destination.ncrn.BirdSpecies

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'Code'
        ,'CommonName'
        ,'ScientificName'
        ,'Family'
        ,'Order'
        ,'TaxonomicOrder'
        ,'IsActive'
        ,'IsTarget'
        ,'SynonymID'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['BirdSpecies']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['BirdSpecies']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'] =  np.where(mask, 'AOU_Code', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'])
    # Code
    mask = (xwalk_dict['ncrn']['BirdSpecies']['xwalk']['destination'] == 'Code')
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'] =  np.where(mask, 'AOU_Code', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'])
    # CommonName
    mask = (xwalk_dict['ncrn']['BirdSpecies']['xwalk']['destination'] == 'CommonName')
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'] =  np.where(mask, 'Common_Name', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'])
    # ScientificName
    mask = (xwalk_dict['ncrn']['BirdSpecies']['xwalk']['destination'] == 'ScientificName')
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'] =  np.where(mask, 'Scientific_Name', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'])
    # Family
    mask = (xwalk_dict['ncrn']['BirdSpecies']['xwalk']['destination'] == 'Family')
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'] =  np.where(mask, 'Family', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'])
    # Order
    mask = (xwalk_dict['ncrn']['BirdSpecies']['xwalk']['destination'] == 'Order')
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'] =  np.where(mask, 'Order', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'])
    # TaxonomicOrder
    mask = (xwalk_dict['ncrn']['BirdSpecies']['xwalk']['destination'] == 'TaxonomicOrder')
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'] =  np.where(mask, 'Taxonomic_Order', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'])
    # IsActive
    mask = (xwalk_dict['ncrn']['BirdSpecies']['xwalk']['destination'] == 'IsActive')
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'] =  np.where(mask, 'IsActive', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'])
    # IsTarget
    mask = (xwalk_dict['ncrn']['BirdSpecies']['xwalk']['destination'] == 'IsTarget')
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'] =  np.where(mask, 'IsTarget', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'])
    # SynonymID
    mask = (xwalk_dict['ncrn']['BirdSpecies']['xwalk']['destination'] == 'SynonymID')
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'] =  np.where(mask, 'SynonymID', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
        'Comments'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['BirdSpecies']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source']) 
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['calculation'])
    # Comments
    mask = (xwalk_dict['ncrn']['BirdSpecies']['xwalk']['destination'] == 'Comments')
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'] =  np.where(mask, "xwalk_dict['ncrn']['BirdSpecies']['tbl_load']['Comments']=np.NaN", xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['note'] =  np.where(mask, 'VARCHAR (1000) NULL; leaving blank because all other attributes are being cast to ncrn.BirdGroups and ncrn.BirdSpeciesPark', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['note'])

    # Blanks
    blank_fields = [
        'ModifiedDate'
        ,'ModifiedBy'
        ,'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['BirdSpecies']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['calculation'])
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['note'])

    return xwalk_dict

def _lu_NoiseLevel(xwalk_dict:dict) -> dict:
    """Crosswalk source.tlu_Disturbance to destination.lu.NoiseLevel

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'Code'
        ,'Label'
        ,'Description'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['lu']['NoiseLevel']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['lu']['NoiseLevel']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['lu']['NoiseLevel']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['lu']['NoiseLevel']['xwalk']['destination'] == 'ID')
    xwalk_dict['lu']['NoiseLevel']['xwalk']['source'] =  np.where(mask, 'Disturbance_Code', xwalk_dict['lu']['NoiseLevel']['xwalk']['source'])
    # Code
    mask = (xwalk_dict['lu']['NoiseLevel']['xwalk']['destination'] == 'Code')
    xwalk_dict['lu']['NoiseLevel']['xwalk']['source'] =  np.where(mask, 'Disturbance_Code', xwalk_dict['lu']['NoiseLevel']['xwalk']['source'])
    # Label
    mask = (xwalk_dict['lu']['NoiseLevel']['xwalk']['destination'] == 'Label')
    xwalk_dict['lu']['NoiseLevel']['xwalk']['source'] =  np.where(mask, 'Disturbance_Description', xwalk_dict['lu']['NoiseLevel']['xwalk']['source'])
    # Description
    mask = (xwalk_dict['lu']['NoiseLevel']['xwalk']['destination'] == 'Description')
    xwalk_dict['lu']['NoiseLevel']['xwalk']['source'] =  np.where(mask, 'Disturbance_Description', xwalk_dict['lu']['NoiseLevel']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['lu']['NoiseLevel']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['lu']['NoiseLevel']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['NoiseLevel']['xwalk']['source']) 
    xwalk_dict['lu']['NoiseLevel']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['lu']['NoiseLevel']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
        'SortOrder'
        ,'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['lu']['NoiseLevel']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['lu']['NoiseLevel']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['NoiseLevel']['xwalk']['calculation'])
    xwalk_dict['lu']['NoiseLevel']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['NoiseLevel']['xwalk']['source'])
    xwalk_dict['lu']['NoiseLevel']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['lu']['NoiseLevel']['xwalk']['note'])

    return xwalk_dict

def _ncrn_AuditLog(xwalk_dict:dict) -> dict:
    """Crosswalk source.tbl_History to destination.ncrn.AuditLog

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'username'
        ,'DetectionEventID'
        ,'Description'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['AuditLog']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['AuditLog']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['AuditLog']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['AuditLog']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['AuditLog']['xwalk']['source'] =  np.where(mask, 'History_ID', xwalk_dict['ncrn']['AuditLog']['xwalk']['source'])
    # username
    mask = (xwalk_dict['ncrn']['AuditLog']['xwalk']['destination'] == 'username')
    xwalk_dict['ncrn']['AuditLog']['xwalk']['source'] =  np.where(mask, 'Contact_ID', xwalk_dict['ncrn']['AuditLog']['xwalk']['source'])
    # Description
    mask = (xwalk_dict['ncrn']['AuditLog']['xwalk']['destination'] == 'Description')
    xwalk_dict['ncrn']['AuditLog']['xwalk']['source'] =  np.where(mask, 'Value_History_Notes', xwalk_dict['ncrn']['AuditLog']['xwalk']['source'])
    # DetectionEventID
    mask = (xwalk_dict['ncrn']['AuditLog']['xwalk']['destination'] == 'DetectionEventID')
    xwalk_dict['ncrn']['AuditLog']['xwalk']['source'] =  np.where(mask, 'Record_ID', xwalk_dict['ncrn']['AuditLog']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
        'LogDate'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['AuditLog']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['AuditLog']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['AuditLog']['xwalk']['source']) 
    xwalk_dict['ncrn']['AuditLog']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['AuditLog']['xwalk']['calculation'])
    # LogDate
    mask = (xwalk_dict['ncrn']['AuditLog']['xwalk']['destination'] == 'LogDate')
    xwalk_dict['ncrn']['AuditLog']['xwalk']['source'] =  np.where(mask, "xwalk_dict['ncrn']['AuditLog']['tbl_load']['LogDate']=np.where(xwalk_dict['ncrn']['AuditLog']['source']['Change_Date'].isna(),np.datetime64('1900-01-01T00:00:00.000000000'),xwalk_dict['ncrn']['AuditLog']['source']['Change_Date'])", xwalk_dict['ncrn']['AuditLog']['xwalk']['source'])
    xwalk_dict['ncrn']['AuditLog']['xwalk']['note'] = np.where(mask, "DATETIME NOT NULL; History_ID (tbl_load.ID) 20210525074241-398932278.156281, Record_ID (tbl_load.DetectionEventID) {C15DFDCF-428A-4980-B6AC-682C9D08054E} recorded null Change_Date; required field so fill in 1900-01-01", xwalk_dict['ncrn']['AuditLog']['xwalk']['note'])

    # Blanks
    blank_fields = [
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['AuditLog']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['AuditLog']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['AuditLog']['xwalk']['calculation'])
    xwalk_dict['ncrn']['AuditLog']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['AuditLog']['xwalk']['source'])
    xwalk_dict['ncrn']['AuditLog']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn']['AuditLog']['xwalk']['note'])

    return xwalk_dict

def _ncrn_AuditLogDetail(xwalk_dict:dict) -> dict:
    """Crosswalk source.tbl_History to destination.ncrn.AuditLogDetail

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'AuditLogID'
        ,'OldValue'
        ,'NewValue'
        ,'EntityAffected'
        ,'FieldName'
        ,'ProtocolID'
        ,'Description'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'] =  np.where(mask, 'History_ID', xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'])
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['note'] = np.where(mask, "NCRN only kept one history table (tbl_History), so ncrn.AuditLogDetail and ncrn.AuditLog were historically 1:1", xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['note'])
    # AuditLogID
    mask = (xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['destination'] == 'AuditLogID')
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'] =  np.where(mask, 'History_ID', xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'])
    # NewValue
    mask = (xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['destination'] == 'NewValue')
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'] =  np.where(mask, 'Value_New', xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'])
    # OldValue
    mask = (xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['destination'] == 'OldValue')
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'] =  np.where(mask, 'Value_Old', xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'])
    # EntityAffected
    mask = (xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['destination'] == 'EntityAffected')
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'] =  np.where(mask, 'Table_Name', xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'])
    # FieldName
    mask = (xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['destination'] == 'FieldName')
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'] =  np.where(mask, 'Record_ID_Field_Name', xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'])
    # Description
    mask = (xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['destination'] == 'Description')
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'] =  np.where(mask, 'Field_Name', xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'])
    # ProtocolID join protocol ID to this via exception
    mask = (xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['destination'] == 'ProtocolID')
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'] =  np.where(mask, "protocol_id", xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'])
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['note'] = np.where(mask, "tbl_History left join tbl_Events to add protocol id and name", xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['note'])

    # Calculated fields
    calculated_fields = [
        'RecordLocatorPath'
        ,'Action'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source']) 
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['calculation'])
    # RecordLocatorPath VARCHAR (200) matching format '<Protocol = Region 1 landbird monitoring protocol><DetectionEvent = Jul  9 2020  8:50AM>'
    mask = (xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['destination'] == 'RecordLocatorPath')
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'] = np.where(mask, "xwalk_dict['ncrn']['AuditLogDetail']['tbl_load']['RecordLocatorPath']='<Protocol='+xwalk_dict['ncrn']['AuditLogDetail']['source']['protocol_name'].astype(str)+'><DetectionEvent='+xwalk_dict['ncrn']['AuditLogDetail']['source']['Date'].astype(str)+'>'", xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'])
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['note'] = np.where(mask, "VARCHAR (200) matching format '<Protocol = Region 1 landbird monitoring protocol><DetectionEvent = Jul  9 2020  8:50AM>'", xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['note'])
    # Action VARCHAR (10) NOT NULL, not constrained; one-word description of what changed in the record
    mask = (xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['destination'] == 'Action')
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'] =  np.where(mask, "xwalk_dict['ncrn']['AuditLogDetail']['tbl_load']['Action']=np.where(xwalk_dict['ncrn']['AuditLogDetail']['source'].Value_Old.isna(),'Add','Update')", xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'])
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['note'] = np.where(mask, "VARCHAR (10) NOT NULL, not constrained; one-word description of what changed in the record", xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['note'])

    # Blanks
    blank_fields = [
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['calculation'])
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'])
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['note'])

    return xwalk_dict

def _lu_SamplingMethod(xwalk_dict:dict) -> dict:
    """Crosswalk source.tbl_Protocol to destination.lu.SamplingMethod

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    
    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'Code'
        ,'Label'
        ,'Summary'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['lu']['SamplingMethod']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['lu']['SamplingMethod']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['lu']['SamplingMethod']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['lu']['SamplingMethod']['xwalk']['destination'] == 'ID')
    xwalk_dict['lu']['SamplingMethod']['xwalk']['source'] =  np.where(mask, 'Protocol_Name', xwalk_dict['lu']['SamplingMethod']['xwalk']['source'])
    # Code
    mask = (xwalk_dict['lu']['SamplingMethod']['xwalk']['destination'] == 'Code')
    xwalk_dict['lu']['SamplingMethod']['xwalk']['source'] =  np.where(mask, 'Protocol_ID', xwalk_dict['lu']['SamplingMethod']['xwalk']['source'])
    # Label
    mask = (xwalk_dict['lu']['SamplingMethod']['xwalk']['destination'] == 'Label')
    xwalk_dict['lu']['SamplingMethod']['xwalk']['source'] =  np.where(mask, 'Protocol_Name', xwalk_dict['lu']['SamplingMethod']['xwalk']['source'])
    # Summary
    mask = (xwalk_dict['lu']['SamplingMethod']['xwalk']['destination'] == 'Summary')
    xwalk_dict['lu']['SamplingMethod']['xwalk']['source'] =  np.where(mask, 'Protocol_Name', xwalk_dict['lu']['SamplingMethod']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['lu']['SamplingMethod']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['lu']['SamplingMethod']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['SamplingMethod']['xwalk']['source']) 
    xwalk_dict['lu']['SamplingMethod']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['lu']['SamplingMethod']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['lu']['SamplingMethod']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['lu']['SamplingMethod']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['SamplingMethod']['xwalk']['calculation'])
    xwalk_dict['lu']['SamplingMethod']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['SamplingMethod']['xwalk']['source'])
    xwalk_dict['lu']['SamplingMethod']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['lu']['SamplingMethod']['xwalk']['note'])

    return xwalk_dict

def _lu_ExperienceLevel(xwalk_dict:dict) -> dict:
    """A table NCRN doesn't maintain but values are loosely based on tbl_Contacts.Position_Title

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    
    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'Code'
        ,'Label'
        ,'Description'
        ,'SortOrder'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['lu']['ExperienceLevel']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['lu']['ExperienceLevel']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['lu']['ExperienceLevel']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['lu']['ExperienceLevel']['xwalk']['destination'] == 'ID')
    xwalk_dict['lu']['ExperienceLevel']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['lu']['ExperienceLevel']['xwalk']['source'])
    # Code
    mask = (xwalk_dict['lu']['ExperienceLevel']['xwalk']['destination'] == 'Code')
    xwalk_dict['lu']['ExperienceLevel']['xwalk']['source'] =  np.where(mask, 'Code', xwalk_dict['lu']['ExperienceLevel']['xwalk']['source'])
    # Label
    mask = (xwalk_dict['lu']['ExperienceLevel']['xwalk']['destination'] == 'Label')
    xwalk_dict['lu']['ExperienceLevel']['xwalk']['source'] =  np.where(mask, 'Label', xwalk_dict['lu']['ExperienceLevel']['xwalk']['source'])
    # Description
    mask = (xwalk_dict['lu']['ExperienceLevel']['xwalk']['destination'] == 'Description')
    xwalk_dict['lu']['ExperienceLevel']['xwalk']['source'] =  np.where(mask, 'Description', xwalk_dict['lu']['ExperienceLevel']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['lu']['ExperienceLevel']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['lu']['ExperienceLevel']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['ExperienceLevel']['xwalk']['source'])
    xwalk_dict['lu']['ExperienceLevel']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['lu']['ExperienceLevel']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
        'Rowversion'
        ,'SortOrder'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['lu']['ExperienceLevel']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['lu']['ExperienceLevel']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['ExperienceLevel']['xwalk']['calculation'])
    xwalk_dict['lu']['ExperienceLevel']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['ExperienceLevel']['xwalk']['source'])
    xwalk_dict['lu']['ExperienceLevel']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['lu']['ExperienceLevel']['xwalk']['note'])

    return xwalk_dict

def _lu_Habitat(xwalk_dict:dict) -> dict:
    """Crosswalk source.tbl_Protocol to destination.lu.Habitat

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    # in the app, "habitat" seems to be a 1:many attribute of a sampling location
    # NCRN hasn't historically subdivided beyond "Forest" and "Grassland" so we just use tbl_Protocol 

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'Code'
        ,'Label'
        ,'Description'
        ,'Group'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['lu']['Habitat']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['lu']['Habitat']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['lu']['Habitat']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['lu']['Habitat']['xwalk']['destination'] == 'ID')
    xwalk_dict['lu']['Habitat']['xwalk']['source'] = np.where(mask, 'ID', xwalk_dict['lu']['Habitat']['xwalk']['source'])
    xwalk_dict['lu']['Habitat']['xwalk']['note'] = np.where(mask, "NCRN doesn't keep this table so borrow from NETNMIDN", xwalk_dict['lu']['Habitat']['xwalk']['note'])
    # Code
    mask = (xwalk_dict['lu']['Habitat']['xwalk']['destination'] == 'Code')
    xwalk_dict['lu']['Habitat']['xwalk']['source'] = np.where(mask, 'Code', xwalk_dict['lu']['Habitat']['xwalk']['source'])
    # Label
    mask = (xwalk_dict['lu']['Habitat']['xwalk']['destination'] == 'Label')
    xwalk_dict['lu']['Habitat']['xwalk']['source'] = np.where(mask, 'Label', xwalk_dict['lu']['Habitat']['xwalk']['source'])
    # Description
    mask = (xwalk_dict['lu']['Habitat']['xwalk']['destination'] == 'Description')
    xwalk_dict['lu']['Habitat']['xwalk']['source'] = np.where(mask, 'Description', xwalk_dict['lu']['Habitat']['xwalk']['source'])
    # Group
    mask = (xwalk_dict['lu']['Habitat']['xwalk']['destination'] == 'Group')
    xwalk_dict['lu']['Habitat']['xwalk']['source'] = np.where(mask, 'Group', xwalk_dict['lu']['Habitat']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['lu']['Habitat']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['lu']['Habitat']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['Habitat']['xwalk']['source']) 
    xwalk_dict['lu']['Habitat']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['lu']['Habitat']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
        'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['lu']['Habitat']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['lu']['Habitat']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['Habitat']['xwalk']['calculation'])
    xwalk_dict['lu']['Habitat']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['Habitat']['xwalk']['source'])
    xwalk_dict['lu']['Habitat']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['lu']['Habitat']['xwalk']['note'])

    return xwalk_dict


def _exception_ncrn_DetectionEvent(xwalk_dict:dict, deletes:list) -> dict:
    """Exceptions associated with the generation of destination table ncrn.DetectionEvent"""

    #  EXCEPTION 1: exceptions from storing observers/recorders in long-format instead of wide-format
    con = dbc._db_connect('access')
    with open(r'src\qry\qry_long_event_contacts.sql', 'r') as query:
        df = pd.read_sql_query(query.read(),con)
    con.close()

    mysorts = df.groupby(['Event_ID']).size().reset_index(name='count').sort_values(['count'], ascending=True)
    double_events = mysorts[mysorts['count']>1].Event_ID.unique()
    single_events = mysorts[mysorts['count']==1].Event_ID.unique()
    zero_events = mysorts[mysorts['count']==0].Event_ID.unique()
    # len(df) == len(singles) + len(doubles) # sanity check

    # edge case: >1 person was associated with the event
    mask = (df['Event_ID'].isin(double_events)) & (df['Contact_Role']!='Observer')
    df['Contact_Role'] = np.where(mask, 'Recorder', df['Contact_Role'])
    mask = (df['Event_ID'].isin(double_events))
    lookup = df[mask].copy()
    lookup['observer'] = lookup['Contact_ID']
    lookup['recorder'] = lookup['Contact_ID']
    lookup = lookup.drop_duplicates('Event_ID').reset_index()[['Event_ID', 'observer', 'recorder']]
    df = df.merge(lookup, on='Event_ID', how='left')

    # edge case: 0 people were associated with the event
    mask = (df['Event_ID'].isin(zero_events))
    df['observer'] = np.where(mask, np.NaN, df['observer'])
    df['recorder'] = np.where(mask, np.NaN, df['recorder'])

    # base case: one-and-only-one person was associated with the event
    mask = (df['Event_ID'].isin(single_events))
    df['observer'] = np.where(mask, df['Contact_ID'], df['observer'])
    df['recorder'] = np.where(mask, df['Contact_ID'], df['recorder'])

    # cleanup
    df = df.drop_duplicates('Event_ID')
    df = df[['Event_ID', 'observer', 'recorder', 'Position_Title']]
    df.rename(columns={'Event_ID':'event_id'}, inplace=True)

    xwalk_dict['ncrn']['DetectionEvent']['source'] = xwalk_dict['ncrn']['DetectionEvent']['source'].merge(df, on='event_id', how='left')

    # EXCEPTION 2: exceptions from storing date and time separately instead of as datetime
    xwalk_dict['ncrn']['DetectionEvent']['source']['Date'] = xwalk_dict['ncrn']['DetectionEvent']['source']['Date'].dt.date
    xwalk_dict['ncrn']['DetectionEvent']['source']['start_time'] = xwalk_dict['ncrn']['DetectionEvent']['source']['start_time'].dt.time
    xwalk_dict['ncrn']['DetectionEvent']['source']['start_time'] = np.where((xwalk_dict['ncrn']['DetectionEvent']['source']['start_time'].isna()), datetime.time(0, 0),xwalk_dict['ncrn']['DetectionEvent']['source']['start_time'] )
    xwalk_dict['ncrn']['DetectionEvent']['source']['Date'] = np.where((xwalk_dict['ncrn']['DetectionEvent']['source']['Date'].isna()), datetime.date(1900, 1, 1),xwalk_dict['ncrn']['DetectionEvent']['source']['Date'] )
    xwalk_dict['ncrn']['DetectionEvent']['source'].loc[:,'activity_start_datetime'] = pd.to_datetime(xwalk_dict['ncrn']['DetectionEvent']['source'].Date.astype(str)+' '+xwalk_dict['ncrn']['DetectionEvent']['source'].start_time.astype(str))

    # EXCEPTION 3: add additional rows from assets.C_DB to xwalk_dict['ncrn']['DetectionEvent']['source']
    con = dbc._db_connect('c')
    tbl = 'tbl_Events'
    df = dbc._exec_qry(con=con, qry=f'get_c_{tbl}')
    con.close()
    xwalk_dict['ncrn']['DetectionEvent']['source'] = pd.concat([xwalk_dict['ncrn']['DetectionEvent']['source'], df])
    mask = (xwalk_dict['ncrn']['DetectionEvent']['source']['Date'].isna()) & (xwalk_dict['ncrn']['DetectionEvent']['source'].activity_start_datetime.isna()==False)
    xwalk_dict['ncrn']['DetectionEvent']['source']['Date'] = np.where(mask, xwalk_dict['ncrn']['DetectionEvent']['source'].activity_start_datetime.dt.date, xwalk_dict['ncrn']['DetectionEvent']['source']['Date'])
    
    # EXCEPTION 4: make lookup table for `entered_by`
    # ncrn.DetectionEvent.EnteredBy is VARCHAR (100), not a pk-fk relationship, so we need to look the names up from source.tbl_Contacts and replace their guids
    lookup = xwalk_dict['ncrn']['Contact']['source'][['Contact_ID','Last_Name','First_Name']].copy()
    lookup['person_name'] = lookup['First_Name'] + ' ' + lookup['Last_Name']
    lookup = lookup[['Contact_ID','person_name']]
    xwalk_dict['ncrn']['DetectionEvent']['source'] = xwalk_dict['ncrn']['DetectionEvent']['source'].merge(lookup, left_on='entered_by', right_on='Contact_ID', how='left')
    xwalk_dict['ncrn']['DetectionEvent']['source']['entered_by'] = xwalk_dict['ncrn']['DetectionEvent']['source']['person_name']
    del xwalk_dict['ncrn']['DetectionEvent']['source']['person_name']
    del xwalk_dict['ncrn']['DetectionEvent']['source']['Contact_ID']

    # EXCEPTION 5: ncrn.DetectionEvent.EnteredBy, ncrn.DetectionEvent.Observer_ContactID, AND ncrn.DetectionEvent.Recorder_ContactID are NOT NULL, so we need to fill something in for None or np.NaN values
    # xwalk_dict['ncrn']['DetectionEvent']['source'] = xwalk_dict['ncrn']['DetectionEvent']['source'].copy(deep=True)
    # ncrn.DetectionEvent.EnteredBy
    mask = (xwalk_dict['ncrn']['DetectionEvent']['source']['entered_by'].isna()==True) & (xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'].isna()==False)
    xwalk_dict['ncrn']['DetectionEvent']['source']['entered_by'] = np.where(mask, xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'], xwalk_dict['ncrn']['DetectionEvent']['source']['entered_by'])
    mask = (xwalk_dict['ncrn']['DetectionEvent']['source']['entered_by'].isna()==True) & (xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'].isna()==True) & (xwalk_dict['ncrn']['DetectionEvent']['source']['observer'].isna()==False)
    xwalk_dict['ncrn']['DetectionEvent']['source']['entered_by'] = np.where(mask, xwalk_dict['ncrn']['DetectionEvent']['source']['observer'], xwalk_dict['ncrn']['DetectionEvent']['source']['entered_by'])
    mask = (xwalk_dict['ncrn']['DetectionEvent']['source']['entered_by'].isna()==True) & (xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'].isna()==True) & (xwalk_dict['ncrn']['DetectionEvent']['source']['observer'].isna()==True)
    xwalk_dict['ncrn']['DetectionEvent']['source']['entered_by'] = np.where(mask, '20230614154645-14017641.544342', xwalk_dict['ncrn']['DetectionEvent']['source']['entered_by'])
    # ncrn.DetectionEvent.Observer_ContactID
    mask = (xwalk_dict['ncrn']['DetectionEvent']['source']['observer'].isna()==True) & (xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'].isna()==False)
    xwalk_dict['ncrn']['DetectionEvent']['source']['observer'] = np.where(mask, xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'], xwalk_dict['ncrn']['DetectionEvent']['source']['observer'])
    mask = (xwalk_dict['ncrn']['DetectionEvent']['source']['observer'].isna()==True) & (xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'].isna()==True) & (xwalk_dict['ncrn']['DetectionEvent']['source']['observer'].isna()==False)
    xwalk_dict['ncrn']['DetectionEvent']['source']['observer'] = np.where(mask, xwalk_dict['ncrn']['DetectionEvent']['source']['observer'], xwalk_dict['ncrn']['DetectionEvent']['source']['observer'])
    mask = (xwalk_dict['ncrn']['DetectionEvent']['source']['observer'].isna()==True) & (xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'].isna()==True) & (xwalk_dict['ncrn']['DetectionEvent']['source']['observer'].isna()==True)
    xwalk_dict['ncrn']['DetectionEvent']['source']['observer'] = np.where(mask, '20230614154645-14017641.544342', xwalk_dict['ncrn']['DetectionEvent']['source']['observer'])
    # ncrn.DetectionEvent.Recorder_ContactID
    mask = (xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'].isna()==True) & (xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'].isna()==False)
    xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'] = np.where(mask, xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'], xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'])
    mask = (xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'].isna()==True) & (xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'].isna()==True) & (xwalk_dict['ncrn']['DetectionEvent']['source']['observer'].isna()==False)
    xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'] = np.where(mask, xwalk_dict['ncrn']['DetectionEvent']['source']['observer'], xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'])
    mask = (xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'].isna()==True) & (xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'].isna()==True) & (xwalk_dict['ncrn']['DetectionEvent']['source']['observer'].isna()==True)
    xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'] = np.where(mask, '20230614154645-14017641.544342', xwalk_dict['ncrn']['DetectionEvent']['source']['recorder'])
    # the lookup table adds NaNs, so we overwrite here
    entrants = list(xwalk_dict['ncrn']['DetectionEvent']['source'].entered_by.unique())
    entrants = [x for x in entrants if '2023' in x or '{' in x or '-' in x]
    lookup = xwalk_dict['ncrn']['Contact']['source'][['Contact_ID','Last_Name','First_Name']].copy()
    lookup['person_name'] = lookup['First_Name'] + ' ' + lookup['Last_Name']
    lookup = lookup[['Contact_ID','person_name']]
    lookup = lookup[lookup['Contact_ID'].isin(entrants)]
    xwalk_dict['ncrn']['DetectionEvent']['source'] = xwalk_dict['ncrn']['DetectionEvent']['source'].merge(lookup, left_on='entered_by', right_on='Contact_ID', how='left')
    xwalk_dict['ncrn']['DetectionEvent']['source']['entered_by'] = np.where(xwalk_dict['ncrn']['DetectionEvent']['source']['person_name'].isna()==False, xwalk_dict['ncrn']['DetectionEvent']['source']['person_name'],xwalk_dict['ncrn']['DetectionEvent']['source']['entered_by'])
    del xwalk_dict['ncrn']['DetectionEvent']['source']['person_name']
    del xwalk_dict['ncrn']['DetectionEvent']['source']['Contact_ID']

    # EXCEPTION 6: testdict['ncrn']['DetectionEvent']['source'].Position_Title has values that don't match lu.ExperienceLevel.ID
    pos_titles = xwalk_dict['ncrn']['DetectionEvent']['source'].Position_Title.unique()
    lookup = {
        'source_val':pos_titles
    }
    lookup = pd.DataFrame(lookup)
    lookup['target_val'] = np.NaN
    lookup['target_val'] = np.where(lookup['source_val']=='Field Technician', 2, lookup['target_val'])
    lookup['target_val'] = np.where(lookup['source_val'].isna(), 1, lookup['target_val'])
    lookup['target_val'] = np.where(lookup['source_val']=='None', 1, lookup['target_val'])
    lookup['target_val'] = np.where(lookup['source_val']=='Crew Leader', 3, lookup['target_val'])
    lookup['target_val'] = np.where(lookup['source_val']=='Top Dog', 3, lookup['target_val'])
    assert len(lookup[lookup['target_val'].isna()])==0, print('Exception 6, _exception_ncrn_DetectionEvent failed; revise the lookup table')
    xwalk_dict['ncrn']['DetectionEvent']['source'] = xwalk_dict['ncrn']['DetectionEvent']['source'].merge(lookup, left_on='Position_Title', right_on='source_val')
    xwalk_dict['ncrn']['DetectionEvent']['source']['Position_Title'] = xwalk_dict['ncrn']['DetectionEvent']['source']['target_val']
    del xwalk_dict['ncrn']['DetectionEvent']['source']['source_val']
    del xwalk_dict['ncrn']['DetectionEvent']['source']['target_val']

    # EXCEPTION 7: remove "ghost" events that field crews tried to delete and duplicate events
    xwalk_dict['ncrn']['DetectionEvent']['source'] = xwalk_dict['ncrn']['DetectionEvent']['source'][xwalk_dict['ncrn']['DetectionEvent']['source']['event_id'].isin(deletes)==False]
    xwalk_dict['ncrn']['DetectionEvent']['source'] = xwalk_dict['ncrn']['DetectionEvent']['source'].drop_duplicates('event_id')
    xwalk_dict['ncrn']['DetectionEvent']['source'].reset_index(drop=True, inplace=True)

    # EXCEPTION 8: `ncrn.DetectionEvent.EnteredDateTime` cannot be NULL
    # Event_ID == '{31ACD17E-AE57-4DD0-BEDB-C2D9C343418F}' has NULL as its entered date
    mask = (xwalk_dict['ncrn']['DetectionEvent']['source']['entered_date'].isna()) & (xwalk_dict['ncrn']['DetectionEvent']['source']['activity_start_datetime'].isna()==False)
    xwalk_dict['ncrn']['DetectionEvent']['source']['entered_date'] = np.where(mask, xwalk_dict['ncrn']['DetectionEvent']['source']['activity_start_datetime'], xwalk_dict['ncrn']['DetectionEvent']['source']['entered_date'])

    # EXCEPTION 9: cannot be NULL:
    # birds['ncrn']['DetectionEvent']['tbl_load']['ProtocolNoiseLevelID']: 146 NULLs
    mask = (xwalk_dict['ncrn']['DetectionEvent']['source']['disturbance_level'].isna())
    xwalk_dict['ncrn']['DetectionEvent']['source']['disturbance_level'] = np.where(mask, 0, xwalk_dict['ncrn']['DetectionEvent']['source']['disturbance_level'])
    # birds['ncrn']['DetectionEvent']['tbl_load']['ProtocolWindCodeID']: 142 NULLs
    mask = (xwalk_dict['ncrn']['DetectionEvent']['source']['wind_speed'].isna())
    xwalk_dict['ncrn']['DetectionEvent']['source']['wind_speed'] = np.where(mask, 0, xwalk_dict['ncrn']['DetectionEvent']['source']['wind_speed'])
    # birds['ncrn']['DetectionEvent']['tbl_load']['ProtocolPrecipitationTypeID']: 138 NULLs
    mask = (xwalk_dict['ncrn']['DetectionEvent']['source']['sky_condition'].isna())
    xwalk_dict['ncrn']['DetectionEvent']['source']['sky_condition'] = np.where(mask, 0, xwalk_dict['ncrn']['DetectionEvent']['source']['sky_condition'])
    
    # EXCEPTION 10: update global lookups to by-protocol lookups
    # `ncrn.DetectionEvent.ProtocolNoiseLevelID` -> `ncrn.ProtocolNoiseLevel.ID` -> ncrn.ProtocolNoiseLevel.NoiseLevelID` -> `lu.NoiseLevel.ID`
    # `ncrn.DetectionEvent.ProtocolWindCodeID` -> `ncrn.ProtocolWindCode.ID` -> ncrn.ProtocolPrecipitationType.WindCodeID` -> `lu.WindCode.ID`
    # `ncrn.DetectionEvent.ProtocolPrecipitationTypeID` -> `ncrn.ProtocolPrecipitationType.ID` -> ncrn.ProtocolPrecipitationType.PrecipitationTypeID` -> `lu.Precipitation.ID`
    # i.e., NCRN used the same integer to indicate match pk-fk regardless of protocol
    # the new data model requires different keys by-protocol
    before_colnames = xwalk_dict['ncrn']['DetectionEvent']['source'].columns

    # `ncrn.DetectionEvent.ProtocolNoiseLevelID`
    xwalk_dict['ncrn']['DetectionEvent']['source']['dummy'] = xwalk_dict['ncrn']['DetectionEvent']['source']['disturbance_level'].astype(int).astype(str) + '_' + xwalk_dict['ncrn']['DetectionEvent']['source']['protocol_id'].astype(str)
    # with open(r'assets/ProtocolWindCode.pkl', 'wb') as f:
    #     pickle.dump(birds['ncrn']['ProtocolWindCode']['source'], f)
    lookup = pd.read_pickle(r'assets/ProtocolNoiseLevel.pkl') # since this is a bridge table, its creation happens after the lookup; my order-of-operations didn't accomodate that so I have to reroute the dataframe
    # step 6, make a dummy variable in the lookup:
    lookup['dummy'] = lookup['Disturbance_Code'].astype(str)  + '_' +  lookup['ProtocolID'].astype(str)
    # step 7: keep only 2 cols: lookup = lookup[['dummy','ID']]
    lookup = lookup[['dummy','ID']]
    lookup.rename(columns={'ID':'dummyid'}, inplace=True)
    # step 8: left-join lookup to `ncrn.BirdDetection.source`
    xwalk_dict['ncrn']['DetectionEvent']['source'] = xwalk_dict['ncrn']['DetectionEvent']['source'].merge(lookup, on='dummy', how='left')
    # step 9: replace the existing `ncrn.BirdDetection.source.Distance_id` values with the corresponding value from the lookup
    xwalk_dict['ncrn']['DetectionEvent']['source']['disturbance_level'] = xwalk_dict['ncrn']['DetectionEvent']['source']['dummyid']
    # step 9: get rid of lookup values
    xwalk_dict['ncrn']['DetectionEvent']['source'] = xwalk_dict['ncrn']['DetectionEvent']['source'][before_colnames]
    
    # `ncrn.DetectionEvent.ProtocolWindCodeID`
    xwalk_dict['ncrn']['DetectionEvent']['source']['dummy'] = xwalk_dict['ncrn']['DetectionEvent']['source']['wind_speed'].astype(int).astype(str) + '_' + xwalk_dict['ncrn']['DetectionEvent']['source']['protocol_id'].astype(str)
    # with open(r'assets/ProtocolWindCode.pkl', 'wb') as f:
    #     pickle.dump(birds['ncrn']['ProtocolWindCode']['source'], f)
    lookup = pd.read_pickle(r'assets/ProtocolWindCode.pkl') # since this is a bridge table, its creation happens after the lookup; my order-of-operations didn't accomodate that so I have to reroute the dataframe
    # step 6, make a dummy variable in the lookup:
    lookup['dummy'] = lookup['Wind_Code'].astype(str)  + '_' +  lookup['ProtocolID'].astype(str)
    # step 7: keep only 2 cols: lookup = lookup[['dummy','ID']]
    lookup = lookup[['dummy','ID']]
    lookup.rename(columns={'ID':'dummyid'}, inplace=True)
    # step 8: left-join lookup to `ncrn.BirdDetection.source`
    xwalk_dict['ncrn']['DetectionEvent']['source'] = xwalk_dict['ncrn']['DetectionEvent']['source'].merge(lookup, on='dummy', how='left')
    # step 9: replace the existing `ncrn.BirdDetection.source.Distance_id` values with the corresponding value from the lookup
    xwalk_dict['ncrn']['DetectionEvent']['source']['wind_speed'] = xwalk_dict['ncrn']['DetectionEvent']['source']['dummyid']
    # step 9: get rid of lookup values
    xwalk_dict['ncrn']['DetectionEvent']['source'] = xwalk_dict['ncrn']['DetectionEvent']['source'][before_colnames]
    
    # `ncrn.DetectionEvent.ProtocolPrecipitationTypeID`
    xwalk_dict['ncrn']['DetectionEvent']['source']['dummy'] = xwalk_dict['ncrn']['DetectionEvent']['source']['sky_condition'].astype(int).astype(str) + '_' + xwalk_dict['ncrn']['DetectionEvent']['source']['protocol_id'].astype(str)
    # with open(r'assets/ProtocolWindCode.pkl', 'wb') as f:
    #     pickle.dump(birds['ncrn']['ProtocolWindCode']['source'], f)
    lookup = pd.read_pickle(r'assets/ProtocolPrecipitationType.pkl') # since this is a bridge table, its creation happens after the lookup; my order-of-operations didn't accomodate that so I have to reroute the dataframe
    lookup['PrecipitationTypeID'] = lookup['PrecipitationTypeID']-1 # netnmidn uses 1-index IDs, NCRN used 0-index IDs so we need to accomodate for the presence of 0-index IDs in the `source` data
    # step 6, make a dummy variable in the lookup:
    lookup['dummy'] = lookup['PrecipitationTypeID'].astype(str)  + '_' +  lookup['ProtocolID'].astype(str)
    # step 7: keep only 2 cols: lookup = lookup[['dummy','ID']]
    lookup = lookup[['dummy','ID']]
    lookup.rename(columns={'ID':'dummyid'}, inplace=True)
    # step 8: left-join lookup to `ncrn.BirdDetection.source`
    xwalk_dict['ncrn']['DetectionEvent']['source'] = xwalk_dict['ncrn']['DetectionEvent']['source'].merge(lookup, on='dummy', how='left')
    # step 9: replace the existing `ncrn.BirdDetection.source.Distance_id` values with the corresponding value from the lookup
    xwalk_dict['ncrn']['DetectionEvent']['source']['sky_condition'] = xwalk_dict['ncrn']['DetectionEvent']['source']['dummyid']
    # step 9: get rid of lookup values
    xwalk_dict['ncrn']['DetectionEvent']['source'] = xwalk_dict['ncrn']['DetectionEvent']['source'][before_colnames]

    return xwalk_dict

def _exception_ncrn_BirdSpecies(xwalk_dict:dict) -> dict:
    """The source table tlu_Species is missing some required attributes so add them"""

    # If there's a value in xwalk_dict['ncrn']['BirdSpecies']['source'] for an attribute but not in csv for that attribute, keep the one from source
    
    # get the best-available taxonomic info
    csv = pd.read_csv(r'assets\db\official_BirdSpecies.csv')
    df = xwalk_dict['ncrn']['BirdSpecies']['source'].copy()
    df = df[[x for x in df.columns if x == 'AOU_Code' or x not in csv.columns]]
    df = csv.merge(df, on='AOU_Code', how='left')

    # get the best-available secondary attributes
    csv = pd.read_csv(r'assets\db\bird_species.csv') # 'integration' [netnmidn].[BirdSpecies]
    csv = csv[['Code', 'IsActive', 'IsTarget', 'SynonymID']]
    csv.rename(columns={'Code':'AOU_Code'}, inplace=True)
    df = df.merge(csv, on='AOU_Code', how='left')

    # EXCEPTION 1: order, family, active, target cannot be NULL
    mask = (df['Order'].isna()) & (df['Family'].isna())
    df['Family'] = np.where(mask, df['Scientific_Name'], df['Family'])
    df['Order'] = np.where(mask, df['Scientific_Name'], df['Order'])
    mask = (df['IsActive'].isna())
    df['IsActive']  = np.where(mask, 1, df['IsActive'])
    mask = (df['IsTarget'].isna())
    df['IsTarget']  = np.where(mask, 1, df['IsTarget'])

    xwalk_dict['ncrn']['BirdSpecies']['source'] = df

    # add rows for ncrn-specific unidentified bird codes
    df = pd.read_excel(r'assets\birds_questions_20240215.xlsx', sheet_name='species_missing_attribute')
    newrows = df[df['RESOLUTION'].isna()].reset_index(drop=True)
    newrows.rename(columns={
        'scientific_name':'Scientific_Name'
        ,'common_name':'Common_Name'
    }
    ,inplace=True)
    newrows = newrows[['AOU_Code', 'Scientific_Name', 'Common_Name']]
    newrows['Family'] = newrows['Scientific_Name']
    newrows['Order'] = newrows['Scientific_Name']
    newrows['IsActive'] = 1
    newrows['IsTarget'] = 0
    newrows['Comments'] = 'NCRN-specific code for an unidentified bird'
    newtaxonomic_orders = [x for x in range((99999-len(newrows)),99999,1)]
    newrows['Taxonomic_Order'] = newtaxonomic_orders

    xwalk_dict['ncrn']['BirdSpecies']['source'] = pd.concat([xwalk_dict['ncrn']['BirdSpecies']['source'], newrows])
    xwalk_dict['ncrn']['BirdSpecies']['source'].reset_index(drop=True, inplace=True)
    

    return xwalk_dict

def _add_row_id(xwalk_dict:dict) -> dict:
    # 1-index row-id for all tables
    rowid_colname = 'rowid'

    for schema in xwalk_dict.keys():
        for tbl in xwalk_dict[schema].keys():
            if len(xwalk_dict[schema][tbl]['tbl_load'])>0:
                xwalk_dict[schema][tbl]['tbl_load'][rowid_colname] = xwalk_dict[schema][tbl]['tbl_load'].index+1
            else:
                orig_cols = list(xwalk_dict[schema][tbl]['tbl_load'].columns)
                orig_cols.append(rowid_colname)
                xwalk_dict[schema][tbl]['tbl_load'] = pd.DataFrame(columns=orig_cols)

    return xwalk_dict

def _exception_ncrn_AuditLogDetail(xwalk_dict:dict) -> dict:
    """The source table tbl_History does not include protocol so add it"""

    history = xwalk_dict['ncrn']['AuditLogDetail']['source'].copy()
    events = xwalk_dict['ncrn']['DetectionEvent']['source'].copy()

    df = history.merge(events, left_on='Record_ID',  right_on='event_id', how='left')
    additions = ['protocol_id', 'protocol_name', 'Date']
    df = df[[x for x in df.columns if x in additions or x in history.columns]]

    xwalk_dict['ncrn']['AuditLogDetail']['source'] = df

    return xwalk_dict

def _exception_lu_Habitat(xwalk_dict:dict) -> dict:
    """NCRN doesn't keep this table so borrow from NETNMIDN"""

    filename = r'assets\db\lu_habitat.csv'
    habitat = pd.read_csv(filename)

    xwalk_dict['lu']['Habitat']['source'] = habitat
    xwalk_dict['lu']['Habitat']['source_name'] = 'NETNMIDN_Landbirds.lu.Habitat'

    return xwalk_dict

def _exception_ncrn_BirdSpeciesGroups(xwalk_dict:dict) -> dict:
    """This is a bridge table between ncrn.BirdSpecies and ncrn.BirdGroups that lets you assign species to zero, one, or more than one group"""

    # If you cast it long-ways on the attributes
    # So a unique combination of AOU_Code and attribute is a row, you'll match the schema for ncrn.BirdSpeciesGroups

    # just make sure that you manage referential integrity between ncrn.BirdSpecies and ncrn.BirdGroups

    # e.g., 
    # ID |  BirdSpeciesID |    BirdGroupID                         | Rowversion
    # 1  |       465      |    20080421161312-627642035.484314     | NaN
    # 2  |       465      |    20080421161058-412766814.231873     | NaN

    species = xwalk_dict['ncrn']['BirdSpecies']['source'].copy()
    groups = xwalk_dict['ncrn']['BirdGroups']['source'].copy()

    targets = [
        'AOU_Code'
        ,'Trophic_Level'
        ,'Foraging_Behavior'
        ,'Nesting_Placement'
        ,'Nest_Height'
        ,'Mass_Quartiles'
        ,'PIF_Avg_Groups'
        ,'Primary_Habitat'
        ,'Number_Broods'
    ]
    species = species[targets]
    species = species.melt(id_vars=['AOU_Code'], var_name='Guild_Name')

    myvals = species[species['value'].isna()==False].copy()
    myvals.loc[:, 'dummy'] = myvals['Guild_Name'].str.upper() + myvals['value'].str.upper()
    myvals = myvals[['AOU_Code', 'dummy']]
    myvals['dummy'] = myvals['dummy'].str.strip()
    myvals['dummy'] = np.where(myvals['dummy']=='NESTING_PLACEMENTGN', 'NESTING_PLACEMENTOGN', myvals['dummy'])
    myvals['dummy'] = np.where(myvals['dummy']=='FORAGING_BEHAVIOR', np.NaN, myvals['dummy'])
    myvals['dummy'] = np.where(myvals['dummy']=='TROPHIC_LEVELTROPHIC LEVEL', np.NaN, myvals['dummy'])

    lookup = {guild:guild.replace(' ', '_') for guild in groups.Guild_Name.unique()}
    seen = []
    for k,v in lookup.items():
        xkeys = k.split(' ')
        for key in xkeys:
            for g in species.Guild_Name.unique():
                if key in g and key not in seen:
                    seen.append(g)
                    lookup[k] = g
    lookup['Nest Predator/Brood Parasite'] = 'Nest Predator/Brood Parasite'.replace(' ','_').replace('/','_')

    myxwalk = {
        'Guild_Name':[]
        ,'newguild':[]
    }
    for k,v in lookup.items():
        myxwalk['Guild_Name'].append(k)
        myxwalk['newguild'].append(v)
    myxwalk = pd.DataFrame(myxwalk)
    groups = groups.merge(myxwalk, on='Guild_Name', how='left')
    groups.loc[:, 'dummy'] = groups['newguild'].str.upper() + groups['Guild_Assignment_Code'].str.upper()

    df = myvals.merge(groups[['dummy', 'Guild_Assignment_ID']], on='dummy', how='left')
    df[df['Guild_Assignment_ID'].isna()].dummy.unique()
    df = df[['AOU_Code', 'Guild_Assignment_ID']]
    df.rename(columns={
        'AOU_Code':'BirdSpeciesID'
        ,'Guild_Assignment_ID':'BirdGroupID'
    }, inplace=True)
    df['ID'] = df.index+1
    df['Rowversion'] = np.NaN
    df = df[df['BirdGroupID'].isna()==False] # `ncrn.BirdSpeciesGroups.BirdGroupID` cannot be NULL but NCRN did not always assign birds to groups

    xwalk_dict['ncrn']['BirdSpeciesGroups']['source'] = df.copy()

    return xwalk_dict

def _ncrn_BirdSpeciesGroups(xwalk_dict:dict) -> dict:
    """This is a bridge table between ncrn.BirdSpecies and ncrn.Park"""

        # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'BirdSpeciesID'
        ,'BirdGroupID'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['source'])
    # BirdSpeciesID
    mask = (xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['destination'] == 'BirdSpeciesID')
    xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['source'] =  np.where(mask, 'BirdSpeciesID', xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['source'])
    # BirdGroupID
    mask = (xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['destination'] == 'BirdGroupID')
    xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['source'] =  np.where(mask, 'BirdGroupID', xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
        'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['calculation'])
    xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn']['BirdSpeciesGroups']['xwalk']['note'])

    return xwalk_dict

def _exception_ncrn_BirdSpeciesPark(xwalk_dict:dict) -> dict:
    """This is a bridge table between ncrn.BirdSpecies and ncrn.Park"""
    # ncrn.BirdSpeciesPark is a park-level grouping variable for species in ncrn.BirdGroups
    # ncrn.BirdSpeciesPark is a bridge table so you can assign a "protected status" to bird species by-park

    # to match NETNMIDN's format, you create one row per unique combination of ncrn.BirdSpecies.ID and ncrn.Park.ID and lu.ProtectedStatus.ID
    # i.e., if all species are unprotected:
    # e.g., if you have two species, two parks, and one protected status, the table should look like this:
    # ID | BirdSpeciesID | ParkID | ProtectedStatusID | Comment
    # 1  | 1             | 1      | 1                 | Null
    # 2  | 2             | 1      | 1                 | Null
    # 3  | 1             | 2      | 1                 | Null
    # 4  | 2             | 2      | 1                 | Null

    species = xwalk_dict['ncrn']['BirdSpecies']['source'].copy()
    parks = xwalk_dict['ncrn']['Park']['source'].copy()

    df = pd.DataFrame()
    for park in parks.PARKCODE.unique():
        tmp = pd.DataFrame()
        tmp['BirdSpeciesID'] = species[['AOU_Code']].copy()
        tmp['ParkID'] = park
        tmp['ProtectedStatusID'] = 3
        df = pd.concat([df, tmp])
    df = df.reset_index()
    df['ID'] = df['BirdSpeciesID'] + '_' + df['ParkID']
    df['Comment'] = np.NaN
    df = df[xwalk_dict['ncrn']['BirdSpeciesPark']['source'].columns]

    xwalk_dict['ncrn']['BirdSpeciesPark']['source'] = df.copy()
    
    return xwalk_dict

def _exception_ncrn_ScannedFile(xwalk_dict:dict) -> dict:
    """ncrn.ScannedFile is a table that does not exist in source"""
    # this is an empty table for the initial database load because we have no scanned files
    return xwalk_dict

def _ncrn_ScannedFile(xwalk_dict:dict) -> dict:
    """This is an empty table that holds scanned files"""

        # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'DetectionEventID'
        ,'Description'
        ,'FileName'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['ScannedFile']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['ScannedFile']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['ScannedFile']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['ScannedFile']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['ScannedFile']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['ncrn']['ScannedFile']['xwalk']['source'])
    # DetectionEventID
    mask = (xwalk_dict['ncrn']['ScannedFile']['xwalk']['destination'] == 'DetectionEventID')
    xwalk_dict['ncrn']['ScannedFile']['xwalk']['source'] =  np.where(mask, 'DetectionEventID', xwalk_dict['ncrn']['ScannedFile']['xwalk']['source'])
    # Description
    mask = (xwalk_dict['ncrn']['ScannedFile']['xwalk']['destination'] == 'Description')
    xwalk_dict['ncrn']['ScannedFile']['xwalk']['source'] =  np.where(mask, 'Description', xwalk_dict['ncrn']['ScannedFile']['xwalk']['source'])
    # FileName
    mask = (xwalk_dict['ncrn']['ScannedFile']['xwalk']['destination'] == 'FileName')
    xwalk_dict['ncrn']['ScannedFile']['xwalk']['source'] =  np.where(mask, 'FileName', xwalk_dict['ncrn']['ScannedFile']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['ScannedFile']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['ScannedFile']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['ScannedFile']['xwalk']['source'])
    xwalk_dict['ncrn']['ScannedFile']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['ScannedFile']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
        'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['ScannedFile']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['ScannedFile']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['ScannedFile']['xwalk']['calculation'])
    xwalk_dict['ncrn']['ScannedFile']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['ScannedFile']['xwalk']['source'])
    xwalk_dict['ncrn']['ScannedFile']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn']['ScannedFile']['xwalk']['note'])

    return xwalk_dict

def _exception_dbo_ParkUser(xwalk_dict:dict) -> dict:
    """dbo.ParkUser is a table that does not exist in source"""
    # this is an empty table for the initial database load because we have no scanned files
    return xwalk_dict

def _dbo_ParkUser(xwalk_dict:dict) -> dict:
    """This is a bridge table between dbo.BirdSpecies and dbo.Park"""

        # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'ParkID'
        ,'UserID'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['dbo']['ParkUser']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['dbo']['ParkUser']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['dbo']['ParkUser']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['dbo']['ParkUser']['xwalk']['destination'] == 'ID')
    xwalk_dict['dbo']['ParkUser']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['dbo']['ParkUser']['xwalk']['source'])
    # ParkID
    mask = (xwalk_dict['dbo']['ParkUser']['xwalk']['destination'] == 'ParkID')
    xwalk_dict['dbo']['ParkUser']['xwalk']['source'] =  np.where(mask, 'ParkID', xwalk_dict['dbo']['ParkUser']['xwalk']['source'])
    # UserID
    mask = (xwalk_dict['dbo']['ParkUser']['xwalk']['destination'] == 'UserID')
    xwalk_dict['dbo']['ParkUser']['xwalk']['source'] =  np.where(mask, 'UserID', xwalk_dict['dbo']['ParkUser']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['dbo']['ParkUser']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['dbo']['ParkUser']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['dbo']['ParkUser']['xwalk']['source'])
    xwalk_dict['dbo']['ParkUser']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['dbo']['ParkUser']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['dbo']['ParkUser']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['dbo']['ParkUser']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['dbo']['ParkUser']['xwalk']['calculation'])
    xwalk_dict['dbo']['ParkUser']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['dbo']['ParkUser']['xwalk']['source'])
    xwalk_dict['dbo']['ParkUser']['xwalk']['note'] =  np.where(mask, 'this field was not collected by dbo and has no dbo equivalent', xwalk_dict['dbo']['ParkUser']['xwalk']['note'])

    return xwalk_dict

def _exception_dbo_User(xwalk_dict:dict) -> dict:
    """dbo.User is a table that does not exist in source"""
    # this is an empty table for the initial database load because we have no scanned files

    df = pd.read_csv(r'assets\db\dbo_user.csv')
    xwalk_dict['dbo']['User']['source'] = df.copy()

    return xwalk_dict

def _dbo_User(xwalk_dict:dict) -> dict:
    """dbo.User is a table that does not exist in source"""

        # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'UserCode'
        ,'Issuer'
        ,'DateCreated'
        ,'LastAccessed'
        ,'AccessCount'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['dbo']['User']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['dbo']['User']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['dbo']['User']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['dbo']['User']['xwalk']['destination'] == 'ID')
    xwalk_dict['dbo']['User']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['dbo']['User']['xwalk']['source'])
    # UserCode
    mask = (xwalk_dict['dbo']['User']['xwalk']['destination'] == 'UserCode')
    xwalk_dict['dbo']['User']['xwalk']['source'] =  np.where(mask, 'UserCode', xwalk_dict['dbo']['User']['xwalk']['source'])
    # Issuer
    mask = (xwalk_dict['dbo']['User']['xwalk']['destination'] == 'Issuer')
    xwalk_dict['dbo']['User']['xwalk']['source'] =  np.where(mask, 'Issuer', xwalk_dict['dbo']['User']['xwalk']['source'])
    # DateCreated
    mask = (xwalk_dict['dbo']['User']['xwalk']['destination'] == 'DateCreated')
    xwalk_dict['dbo']['User']['xwalk']['source'] =  np.where(mask, 'DateCreated', xwalk_dict['dbo']['User']['xwalk']['source'])
    # LastAccessed
    mask = (xwalk_dict['dbo']['User']['xwalk']['destination'] == 'LastAccessed')
    xwalk_dict['dbo']['User']['xwalk']['source'] =  np.where(mask, 'LastAccessed', xwalk_dict['dbo']['User']['xwalk']['source'])
    # AccessCount
    mask = (xwalk_dict['dbo']['User']['xwalk']['destination'] == 'AccessCount')
    xwalk_dict['dbo']['User']['xwalk']['source'] =  np.where(mask, 'AccessCount', xwalk_dict['dbo']['User']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['dbo']['User']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['dbo']['User']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['dbo']['User']['xwalk']['source'])
    xwalk_dict['dbo']['User']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['dbo']['User']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['dbo']['User']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['dbo']['User']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['dbo']['User']['xwalk']['calculation'])
    xwalk_dict['dbo']['User']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['dbo']['User']['xwalk']['source'])
    xwalk_dict['dbo']['User']['xwalk']['note'] =  np.where(mask, 'this field was not collected by dbo and has no dbo equivalent', xwalk_dict['dbo']['User']['xwalk']['note'])

    return xwalk_dict

def _exception_dbo_UserRole(xwalk_dict:dict) -> dict:
    """dbo.User is a table that does not exist in source"""
    df = pd.read_csv(r'assets\db\dbo_userrole.csv')
    xwalk_dict['dbo']['UserRole']['source'] = df.copy()
    return xwalk_dict

def _dbo_UserRole(xwalk_dict:dict) -> dict:
    """dbo.User is a table that does not exist in source"""

        # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'UserID'
        ,'RoleID'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['dbo']['UserRole']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['dbo']['UserRole']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['dbo']['UserRole']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['dbo']['UserRole']['xwalk']['destination'] == 'ID')
    xwalk_dict['dbo']['UserRole']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['dbo']['UserRole']['xwalk']['source'])
    # RoleID
    mask = (xwalk_dict['dbo']['UserRole']['xwalk']['destination'] == 'RoleID')
    xwalk_dict['dbo']['UserRole']['xwalk']['source'] =  np.where(mask, 'RoleID', xwalk_dict['dbo']['UserRole']['xwalk']['source'])
    # UserID
    mask = (xwalk_dict['dbo']['UserRole']['xwalk']['destination'] == 'UserID')
    xwalk_dict['dbo']['UserRole']['xwalk']['source'] =  np.where(mask, 'UserID', xwalk_dict['dbo']['UserRole']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['dbo']['UserRole']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['dbo']['UserRole']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['dbo']['UserRole']['xwalk']['source'])
    xwalk_dict['dbo']['UserRole']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['dbo']['UserRole']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['dbo']['UserRole']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['dbo']['UserRole']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['dbo']['UserRole']['xwalk']['calculation'])
    xwalk_dict['dbo']['UserRole']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['dbo']['UserRole']['xwalk']['source'])
    xwalk_dict['dbo']['UserRole']['xwalk']['note'] =  np.where(mask, 'this field was not collected by dbo and has no dbo equivalent', xwalk_dict['dbo']['UserRole']['xwalk']['note'])

    return xwalk_dict

def _exception_ncrn_ProtocolWindCode(xwalk_dict:dict) -> dict:
    """ncrn.ProtocolWindCode is a bridge table between ncrn.Protocol and lu.WindCode that does not exist in source"""

    protocols = xwalk_dict['ncrn']['Protocol']['source']
    windcodes = xwalk_dict['lu']['WindCode']['source']

    df = pd.DataFrame()
    for protocol in protocols.Protocol_ID.unique():
        windcodes2 = windcodes.copy()
        windcodes2['ProtocolID'] = protocol
        df = pd.concat([df, windcodes2])
    df = df[['Wind_Code', 'ProtocolID']]
    df = df.reset_index()
    del df['index']
    df['ID'] = df.index+1
    xwalk_dict['ncrn']['ProtocolWindCode']['source'] = df.copy()

    return xwalk_dict

def _ncrn_ProtocolWindCode(xwalk_dict:dict) -> dict:
    """ncrn.ProtocolWindCode is a bridge table between ncrn.Protocol and lu.WindCode that does not exist in source"""

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'ProtocolID'
        # ,'WindCodeID'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['source'])
    # ProtocolID
    mask = (xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['destination'] == 'ProtocolID')
    xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['source'] =  np.where(mask, 'ProtocolID', xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['source'])
    # WindCodeID
    # mask = (xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['destination'] == 'WindCodeID')
    # xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['source'] =  np.where(mask, 'Wind_Code', xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
        'WindCodeID'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['source'])
    xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['calculation'])
    # `WindCodeID` update from zero-index to 1-index
    mask = (xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['destination'] == 'WindCodeID')
    xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['source'] =  np.where(mask, "xwalk_dict['ncrn']['ProtocolWindCode']['tbl_load']['WindCodeID'] = xwalk_dict['ncrn']['ProtocolWindCode']['source']['Wind_Code']+1", xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['source'])
    xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['note'] =  np.where(mask, "source.Wind_Code is zero-index and we need 1-index in destination", xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['note'])

    # Blanks
    blank_fields = [
        'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['calculation'])
    xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['source'])
    xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['note'] =  np.where(mask, 'this field was not collected by ncrn and has no ncrn equivalent', xwalk_dict['ncrn']['ProtocolWindCode']['xwalk']['note'])

    return xwalk_dict

def _exception_ncrn_ProtocolPrecipitationType(xwalk_dict:dict) -> dict:
    """ncrn.ProtocolPrecipitationType is a bridge table between ncrn.Protocol and lu.PrecipitationType that does not exist in source"""

    protocols = xwalk_dict['ncrn']['Protocol']['source']
    PrecipitationTypes = xwalk_dict['lu']['PrecipitationType']['source']

    df = pd.DataFrame()
    for protocol in protocols.Protocol_ID.unique():
        PrecipitationTypes2 = PrecipitationTypes.copy()
        PrecipitationTypes2['ProtocolID'] = protocol
        PrecipitationTypes2['PrecipitationTypeID'] = PrecipitationTypes2['ID']
        df = pd.concat([df, PrecipitationTypes2])
    df = df[['ID', 'ProtocolID', 'PrecipitationTypeID']]
    df.reset_index(drop=True, inplace=True)
    df['ID'] = df.index+1
    xwalk_dict['ncrn']['ProtocolPrecipitationType']['source'] = df.copy()

    return xwalk_dict

def _ncrn_ProtocolPrecipitationType(xwalk_dict:dict) -> dict:
    """ncrn.ProtocolPrecipitationType is a bridge table between ncrn.Protocol and lu.PrecipitationType that does not exist in source"""

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'ProtocolID'
        ,'PrecipitationTypeID'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['source'])
    # ProtocolID
    mask = (xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['destination'] == 'ProtocolID')
    xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['source'] =  np.where(mask, 'ProtocolID', xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['source'])
    # PrecipitationTypeID
    mask = (xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['destination'] == 'PrecipitationTypeID')
    xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['source'] =  np.where(mask, 'PrecipitationTypeID', xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['source'])
    xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
        'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['calculation'])
    xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['source'])
    xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['note'] =  np.where(mask, 'this field was not collected by ncrn and has no ncrn equivalent', xwalk_dict['ncrn']['ProtocolPrecipitationType']['xwalk']['note'])

    return xwalk_dict

def _exception_ncrn_ProtocolNoiseLevel(xwalk_dict:dict) -> dict:
    """ncrn.ProtocolNoiseLevel is a bridge table between ncrn.Protocol and lu.NoiseLevel that does not exist in source"""

    protocols = xwalk_dict['ncrn']['Protocol']['source']
    NoiseLevels = xwalk_dict['lu']['NoiseLevel']['source']

    df = pd.DataFrame()
    for protocol in protocols.Protocol_ID.unique():
        NoiseLevels2 = NoiseLevels.copy()
        NoiseLevels2['ProtocolID'] = protocol
        df = pd.concat([df, NoiseLevels2])
    df = df[['Disturbance_Code', 'ProtocolID']]
    df = df.reset_index()
    del df['index']
    df['ID'] = df.index+1
    xwalk_dict['ncrn']['ProtocolNoiseLevel']['source'] = df.copy()

    return xwalk_dict

def _ncrn_ProtocolNoiseLevel(xwalk_dict:dict) -> dict:
    """ncrn.ProtocolNoiseLevel is a bridge table between ncrn.Protocol and lu.NoiseLevel that does not exist in source"""

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'ProtocolID'
        # ,'NoiseLevelID'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['source'])
    # ProtocolID
    mask = (xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['destination'] == 'ProtocolID')
    xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['source'] =  np.where(mask, 'ProtocolID', xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['source'])
    # NoiseLevelID
    # mask = (xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['destination'] == 'NoiseLevelID')
    # xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['source'] =  np.where(mask, 'Disturbance_Code', xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
        'NoiseLevelID'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['source'])
    xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['calculation'])
    # `NoiseLevelID` update from zero-index to 1-index
    mask = (xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['destination'] == 'NoiseLevelID')
    xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['source'] =  np.where(mask, "xwalk_dict['ncrn']['ProtocolNoiseLevel']['tbl_load']['NoiseLevelID'] = xwalk_dict['ncrn']['ProtocolNoiseLevel']['source']['Disturbance_Code']+1", xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['source'])
    xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['note'] =  np.where(mask, "source.Disturbance_Code is zero-index and we need 1-index in destination", xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['note'])

    # Blanks
    blank_fields = [
        'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['calculation'])
    xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['source'])
    xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['note'] =  np.where(mask, 'this field was not collected by ncrn and has no ncrn equivalent', xwalk_dict['ncrn']['ProtocolNoiseLevel']['xwalk']['note'])

    return xwalk_dict

def _exception_ncrn_ProtocolTimeInterval(xwalk_dict:dict) -> dict:
    """ncrn.ProtocolTimeInterval is a bridge table between ncrn.Protocol and lu.TimeInterval that does not exist in source"""

    protocols = xwalk_dict['ncrn']['Protocol']['source']
    TimeIntervals = xwalk_dict['lu']['TimeInterval']['source']

    df = pd.DataFrame()
    for protocol in protocols.Protocol_ID.unique():
        TimeIntervals2 = TimeIntervals.copy()
        TimeIntervals2['ProtocolID'] = protocol
        df = pd.concat([df, TimeIntervals2])
    df = df[['Interval', 'ProtocolID']]
    df = df.reset_index()
    del df['index']
    df['ID'] = df.index+1
    xwalk_dict['ncrn']['ProtocolTimeInterval']['source'] = df.copy()

    return xwalk_dict

def _ncrn_ProtocolTimeInterval(xwalk_dict:dict) -> dict:
    """ncrn.ProtocolTimeInterval is a bridge table between ncrn.Protocol and lu.TimeInterval that does not exist in source"""

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'ProtocolID'
        ,'TimeIntervalID'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['source'])
    # ProtocolID
    mask = (xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['destination'] == 'ProtocolID')
    xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['source'] =  np.where(mask, 'ProtocolID', xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['source'])
    # TimeIntervalID
    mask = (xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['destination'] == 'TimeIntervalID')
    xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['source'] =  np.where(mask, 'Interval', xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['source'])
    xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['calculation'])
    xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['source'])
    xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['note'] =  np.where(mask, 'this field was not collected by ncrn and has no ncrn equivalent', xwalk_dict['ncrn']['ProtocolTimeInterval']['xwalk']['note'])

    return xwalk_dict

def _exception_ncrn_ProtocolDetectionType(xwalk_dict:dict) -> dict:
    """ncrn.ProtocolDetectionType is a bridge table between ncrn.Protocol and lu.DetectionType that does not exist in source"""

    protocols = xwalk_dict['ncrn']['Protocol']['source']
    DetectionTypes = xwalk_dict['lu']['DetectionType']['source']

    df = pd.DataFrame()
    for protocol in protocols.Protocol_ID.unique():
        DetectionTypes2 = DetectionTypes.copy()
        DetectionTypes2['ProtocolID'] = protocol
        df = pd.concat([df, DetectionTypes2])
    df = df[['ID_Code', 'ProtocolID']]
    df = df.reset_index()
    del df['index']
    df['ID'] = df.index+1
    xwalk_dict['ncrn']['ProtocolDetectionType']['source'] = df.copy()

    lookup = {}
    lookup['C'] = 1
    lookup['S'] = 2
    lookup['V'] = 3
    for k,v in lookup.items():
        mask = (xwalk_dict['ncrn']['ProtocolDetectionType']['source']['ID_Code']==k)
        xwalk_dict['ncrn']['ProtocolDetectionType']['source']['ID_Code'] = np.where(mask, v, xwalk_dict['ncrn']['ProtocolDetectionType']['source']['ID_Code'])

    return xwalk_dict

def _ncrn_ProtocolDetectionType(xwalk_dict:dict) -> dict:
    """ncrn.ProtocolDetectionType is a bridge table between ncrn.Protocol and lu.DetectionType that does not exist in source"""

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'ProtocolID'
        ,'DetectionTypeID'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['source'])
    # ProtocolID
    mask = (xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['destination'] == 'ProtocolID')
    xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['source'] =  np.where(mask, 'ProtocolID', xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['source'])
    # DetectionTypeID
    mask = (xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['destination'] == 'DetectionTypeID')
    xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['source'] =  np.where(mask, 'ID_Code', xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['source'])
    xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['calculation'])
    xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['source'])
    xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['note'] =  np.where(mask, 'this field was not collected by ncrn and has no ncrn equivalent', xwalk_dict['ncrn']['ProtocolDetectionType']['xwalk']['note'])

    return xwalk_dict

def _exception_ncrn_ProtocolDistanceClass(xwalk_dict:dict) -> dict:
    """ncrn.ProtocolDistanceClass is a bridge table between ncrn.Protocol and lu.DistanceClass that does not exist in source"""

    protocols = xwalk_dict['ncrn']['Protocol']['source']
    DistanceClasss = xwalk_dict['lu']['DistanceClass']['source']

    df = pd.DataFrame()
    for protocol in protocols.Protocol_ID.unique():
        DistanceClasss2 = DistanceClasss.copy()
        DistanceClasss2['ProtocolID'] = protocol
        df = pd.concat([df, DistanceClasss2])
    df = df[['Distance_id', 'ProtocolID']]
    df = df.reset_index()
    del df['index']
    df['ID'] = df.index+1
    xwalk_dict['ncrn']['ProtocolDistanceClass']['source'] = df.copy()

    return xwalk_dict

def _ncrn_ProtocolDistanceClass(xwalk_dict:dict) -> dict:
    """ncrn.ProtocolDistanceClass is a bridge table between ncrn.Protocol and lu.DistanceClass that does not exist in source"""

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'ProtocolID'
        ,'DistanceClassID'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['source'])
    # ProtocolID
    mask = (xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['destination'] == 'ProtocolID')
    xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['source'] =  np.where(mask, 'ProtocolID', xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['source'])
    # DistanceClassID
    mask = (xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['destination'] == 'DistanceClassID')
    xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['source'] =  np.where(mask, 'Distance_id', xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['source'])
    xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
        'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['calculation'])
    xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['source'])
    xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['note'] =  np.where(mask, 'this field was not collected by ncrn and has no ncrn equivalent', xwalk_dict['ncrn']['ProtocolDistanceClass']['xwalk']['note'])

    return xwalk_dict

def _exception_lu_TemperatureUnit(xwalk_dict:dict) -> dict:
    """ncrn.TemperatureUnit is a table that does not exist in source so create"""

    df = pd.DataFrame({
        'Code':[
            'F'
            ,'C'
        ]
        ,'Label':[
            'Farenheit'
            ,'Celsius'
        ]
        ,'Description':[
            'Degrees Farenheit'
            ,'Degrees Celsius'
        ]
        ,'ValidMinimumValue':[
            -40
            ,-40
        ]
        ,'ValidMaximumValue':[
            130
            ,55
        ]
        ,'Rowversion':[
            np.NaN
            ,np.NaN
        ]
    })
    xwalk_dict['lu']['TemperatureUnit']['source'] = df

    return xwalk_dict

def _lu_ProtectedStatus(xwalk_dict:dict) -> dict:
    """This is a bridge table between ncrn.BirdSpecies and ncrn.Park"""

        # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'Code'
        ,'Label'
        ,'Summary'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['lu']['ProtectedStatus']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['lu']['ProtectedStatus']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['lu']['ProtectedStatus']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['lu']['ProtectedStatus']['xwalk']['destination'] == 'ID')
    xwalk_dict['lu']['ProtectedStatus']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['lu']['ProtectedStatus']['xwalk']['source'])
    # Code
    mask = (xwalk_dict['lu']['ProtectedStatus']['xwalk']['destination'] == 'Code')
    xwalk_dict['lu']['ProtectedStatus']['xwalk']['source'] =  np.where(mask, 'Code', xwalk_dict['lu']['ProtectedStatus']['xwalk']['source'])
    # Label
    mask = (xwalk_dict['lu']['ProtectedStatus']['xwalk']['destination'] == 'Label')
    xwalk_dict['lu']['ProtectedStatus']['xwalk']['source'] =  np.where(mask, 'Label', xwalk_dict['lu']['ProtectedStatus']['xwalk']['source'])
    # Summary
    mask = (xwalk_dict['lu']['ProtectedStatus']['xwalk']['destination'] == 'Summary')
    xwalk_dict['lu']['ProtectedStatus']['xwalk']['source'] =  np.where(mask, 'Summary', xwalk_dict['lu']['ProtectedStatus']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['lu']['ProtectedStatus']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['lu']['ProtectedStatus']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['ProtectedStatus']['xwalk']['source'])
    xwalk_dict['lu']['ProtectedStatus']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['lu']['ProtectedStatus']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['lu']['ProtectedStatus']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['lu']['ProtectedStatus']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['ProtectedStatus']['xwalk']['calculation'])
    xwalk_dict['lu']['ProtectedStatus']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['ProtectedStatus']['xwalk']['source'])
    xwalk_dict['lu']['ProtectedStatus']['xwalk']['note'] =  np.where(mask, 'this field was not collected by lu and has no lu equivalent', xwalk_dict['lu']['ProtectedStatus']['xwalk']['note'])

    return xwalk_dict

def _lu_TemperatureUnit(xwalk_dict:dict) -> dict:
    """This is a bridge table between ncrn.BirdSpecies and ncrn.Park"""

        # 1:1 fields
    one_to_one_fields = [
        'Code'
        ,'Label'
        ,'Description'
        ,'ValidMinimumValue'
        ,'ValidMaximumValue'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['lu']['TemperatureUnit']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['lu']['TemperatureUnit']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['lu']['TemperatureUnit']['xwalk']['calculation'])
    # Code
    mask = (xwalk_dict['lu']['TemperatureUnit']['xwalk']['destination'] == 'Code')
    xwalk_dict['lu']['TemperatureUnit']['xwalk']['source'] =  np.where(mask, 'Code', xwalk_dict['lu']['TemperatureUnit']['xwalk']['source'])
    # Label
    mask = (xwalk_dict['lu']['TemperatureUnit']['xwalk']['destination'] == 'Label')
    xwalk_dict['lu']['TemperatureUnit']['xwalk']['source'] =  np.where(mask, 'Label', xwalk_dict['lu']['TemperatureUnit']['xwalk']['source'])
    # Description
    mask = (xwalk_dict['lu']['TemperatureUnit']['xwalk']['destination'] == 'Description')
    xwalk_dict['lu']['TemperatureUnit']['xwalk']['source'] =  np.where(mask, 'Description', xwalk_dict['lu']['TemperatureUnit']['xwalk']['source'])
    # ValidMinimumValue
    mask = (xwalk_dict['lu']['TemperatureUnit']['xwalk']['destination'] == 'ValidMinimumValue')
    xwalk_dict['lu']['TemperatureUnit']['xwalk']['source'] =  np.where(mask, 'ValidMinimumValue', xwalk_dict['lu']['TemperatureUnit']['xwalk']['source'])
    # ValidMaximumValue
    mask = (xwalk_dict['lu']['TemperatureUnit']['xwalk']['destination'] == 'ValidMaximumValue')
    xwalk_dict['lu']['TemperatureUnit']['xwalk']['source'] =  np.where(mask, 'ValidMaximumValue', xwalk_dict['lu']['TemperatureUnit']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['lu']['TemperatureUnit']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['lu']['TemperatureUnit']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['TemperatureUnit']['xwalk']['source'])
    xwalk_dict['lu']['TemperatureUnit']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['lu']['TemperatureUnit']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
        'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['lu']['TemperatureUnit']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['lu']['TemperatureUnit']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['TemperatureUnit']['xwalk']['calculation'])
    xwalk_dict['lu']['TemperatureUnit']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['TemperatureUnit']['xwalk']['source'])
    xwalk_dict['lu']['TemperatureUnit']['xwalk']['note'] =  np.where(mask, 'this field was not collected by lu and has no lu equivalent', xwalk_dict['lu']['TemperatureUnit']['xwalk']['note'])

    return xwalk_dict

def _exception_lu_ExperienceLevel(xwalk_dict:dict) -> dict:
    """A table NCRN doesn't maintain but values are loosely based on tbl_Contacts.Position_Title"""

    exp_lev = {
        'ID':[1,2,3,4]
        ,'Code':['unk', 'tech', 'lead', 'exp']
        ,'Label':['Unknown', 'Field technician', 'Field lead', 'Expert']
        ,'Description':['Unknown level of experience', 'Cursory training and basic experience in mid-Atlantic grassland/forestland bird identification', 'Considerable training and solid experience in mid-Atlantic grassland/forestland bird identification', 'An expert in mid-Atlantic grassland/forestland bird identification']
        ,'SortOrder':[np.NaN, np.NaN, np.NaN, np.NaN]
        ,'Rowversion':[np.NaN, np.NaN, np.NaN, np.NaN]
    }

    exp_lev = pd.DataFrame(exp_lev)

    xwalk_dict['lu']['ExperienceLevel']['source'] = exp_lev.copy()

    return xwalk_dict

def _exception_dbo_Role(xwalk_dict:dict) -> dict:
    """A table NCRN doesn't maintain but vadboes are loosely based on tbl_Contacts.Position_Title"""

    df = {
        'ID':[1,2,3,4]
        ,'Label':['Administrator', 'DataReader', 'ParkDataEntry', 'ProtocolLead']
    }

    df = pd.DataFrame(df)

    xwalk_dict['dbo']['Role']['source'] = df.copy()

    return xwalk_dict

def _dbo_Role(xwalk_dict:dict) -> dict:
    """Table of db roles, not a NCRN table"""

        # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'Label'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['dbo']['Role']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['dbo']['Role']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['dbo']['Role']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['dbo']['Role']['xwalk']['destination'] == 'ID')
    xwalk_dict['dbo']['Role']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['dbo']['Role']['xwalk']['source'])
    # ID
    mask = (xwalk_dict['dbo']['Role']['xwalk']['destination'] == 'Label')
    xwalk_dict['dbo']['Role']['xwalk']['source'] =  np.where(mask, 'Label', xwalk_dict['dbo']['Role']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['dbo']['Role']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['dbo']['Role']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['dbo']['Role']['xwalk']['source'])
    xwalk_dict['dbo']['Role']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['dbo']['Role']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['dbo']['Role']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['dbo']['Role']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['dbo']['Role']['xwalk']['calculation'])
    xwalk_dict['dbo']['Role']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['dbo']['Role']['xwalk']['source'])
    xwalk_dict['dbo']['Role']['xwalk']['note'] =  np.where(mask, 'this field was not collected by dbo and has no dbo equivalent', xwalk_dict['dbo']['Role']['xwalk']['note'])

    return xwalk_dict

def _exception_lu_ProtectedStatus(xwalk_dict:dict) -> dict:
    """A table NCRN doesn't maintain but values are loosely based on tbl_Contacts.Position_Title"""

    protections = {
        'ID':[1,2,3,4]
        ,'Code':['LP', 'OP', 'NP', 'NA']
        ,'Label':['Legally Protected', 'Operationally Protected', 'Not Protected', 'Not Available']
        ,'Summary':[
            'Covered under existing statue as being exempt from public dissemination'
            ,'Data withheld from release to protect resources or site fidelity but may be subject to release upon request'
            ,'Data is neither legally nor operationally sensitive and therefore does not have any distribution restrictions conditional on protected status'
            ,np.NaN
            ]
    }

    protections = pd.DataFrame(protections)

    xwalk_dict['lu']['ProtectedStatus']['source'] = protections.copy()

    return xwalk_dict

def _exception_ncrn_Contact(xwalk_dict:dict) -> dict:
    """map experience level to ncrn.Contact.source.ExperienceLevelID"""

    # EXCEPTION 1: make erroneous position titles into sensible experience levels consistent with NETNMIDN
    mymap = {}
    mymap['Crew Leader'] = 3
    mymap['None'] = 1
    mymap['Field Technician'] = 2
    mymap['Data Manager'] = 1
    mymap['NCRN Data Manager'] = 1
    mymap['Undergrad'] = 2
    mymap['Top Dog'] = 3
    xwalk_dict['ncrn']['Contact']['source']['ExperienceLevelID'] = np.NaN
    xwalk_dict['ncrn']['Contact']['source']['Position_Title'] = xwalk_dict['ncrn']['Contact']['source']['Position_Title'].astype(str)
    for k,v in mymap.items():
        mask = (xwalk_dict['ncrn']['Contact']['source'].Position_Title == k)
        xwalk_dict['ncrn']['Contact']['source']['ExperienceLevelID'] = np.where(mask, v, xwalk_dict['ncrn']['Contact']['source']['ExperienceLevelID'])

    # EXCEPTION 2: fill in blanks and replace erroneous `organization` values
    xwalk_dict['ncrn']['Contact']['source']['Organization'] = xwalk_dict['ncrn']['Contact']['source']['Organization'].str.upper() # make org case-insensitive
    mask = (xwalk_dict['ncrn']['Contact']['source']['Organization']=='UNKNOWN')
    xwalk_dict['ncrn']['Contact']['source']['Organization'] = np.where(mask, 'UDEL', xwalk_dict['ncrn']['Contact']['source']['Organization'])

    # EXCEPTION 3: filter out erroneous contact IDs
    # exclusions
    # these are IDs that have no related records, are erroneous, and should be deleted from the dataset
    exclude_names = [
        '{BD1C25D7-5D8A-4998-8031-21421D0C9767}' # First: 'TEST', Last: 'TEST', related records (n): 0
        ,'{FC4440FD-820F-436C-8833-71641A801664}' # First: 'josh', Last: '.', related records (n): 0
    ]
    xwalk_dict['ncrn']['Contact']['source'] = xwalk_dict['ncrn']['Contact']['source'][xwalk_dict['ncrn']['Contact']['source']['Contact_ID'].isin(exclude_names)==False]
    xwalk_dict['ncrn']['Contact']['source'].reset_index(drop=True, inplace=True)

    # EXCEPTION 4: deduplicate contact IDs, cascade changes to `ncrn.DetectionEvent.source.observer` and `ncrn.DetectionEvent.source.recorder`
    # TODO

    return xwalk_dict

def _exception_ncrn_Location(xwalk_dict:dict) -> dict:
    """
    Clean up source.tbl_Locations

    1. Add additional rows from second source.tbl_Field_Data to ncrn.BirdDetection
    
    2. Filter source.tbl_Locations
    source.tbl_Locations contains >4k sampling locations (i.e. unique `tbl_Locations.Location_ID`s.
    ~80% of those `Location_ID`s have zero site visits.
    Here, we filter out the locations that have zero site visits because, if we've never been there since monitoring started, they're not real.
    It's not clear why there are so many erroneous sampling locations in `source.tbl_Locations`.

    3. Add missing lat/lon for real `Location_ID`s
    Six `Location_ID`s that we have visited have no lat/lon but do have UTMs.
    Here, we manually look up the missing lat/lon values and map them to `Location_ID`s.

    4. concat all of the remaining attributes into a `ncrn.Location.Notes` field
    """
    # 1. query additional rows from second source db, append to existing source

    con = dbc._db_connect('c')
    tbl = 'tbl_Locations'
    df = dbc._exec_qry(con=con, qry=f'get_c_{tbl}')
    con.close()
    xwalk_dict['ncrn']['Location']['source'] = pd.concat([xwalk_dict['ncrn']['Location']['source'], df])

    # 2. filter
    xwalk_dict['ncrn']['Location']['source'] = xwalk_dict['ncrn']['Location']['source'][xwalk_dict['ncrn']['Location']['source']['Location_ID'].isin(xwalk_dict['ncrn']['DetectionEvent']['source'].location_id.unique())]
    
    # 3. add lat/lon decimal degrees from UTM when dec degrees are missing
    mysites = xwalk_dict['ncrn']['Location']['source'][xwalk_dict['ncrn']['Location']['source']['Long_WGS84'].isna()].Location_ID.unique()
    mymap = {}
    for site in mysites:
        mymap[site] = {
            'Lat_WGS84':np.NaN
            ,'Long_WGS84':np.NaN
        }
    # hard-code values are from https://tagis.dep.wv.gov/convert/convertClassic.html
    # convert the Zone 18N UTM values to decimal degrees
    # e.g., 265250 E, 4371750 N, Zone 18, Datum NAD83 == -77.728683, 39.463336
    mymap['20070118125204-200309872.627258']['Lat_WGS84'] = -77.780567
    mymap['20070118125204-200309872.627258']['Long_WGS84'] = 39.306708
    mymap['20070118125204-21898925.3044128']['Lat_WGS84'] = -77.780922
    mymap['20070118125204-21898925.3044128']['Long_WGS84'] = 39.315708
    mymap['20070118125204-472600340.843201']['Lat_WGS84'] = -77.778206
    mymap['20070118125204-472600340.843201']['Long_WGS84'] = 39.320278
    mymap['20070118125204-560659170.150757']['Lat_WGS84'] = -77.734403
    mymap['20070118125204-560659170.150757']['Long_WGS84'] = 39.460950
    mymap['20070118125204-842249035.835266']['Lat_WGS84'] = -77.752528
    mymap['20070118125204-842249035.835266']['Long_WGS84'] = 39.478539
    mymap['20070118125204-896642208.099365']['Lat_WGS84'] = -77.728683
    mymap['20070118125204-896642208.099365']['Long_WGS84'] = 39.463336

    c_locs = assets.C_L
    mymap.update(c_locs)
    
    for k,v in mymap.items():
        mask = (xwalk_dict['ncrn']['Location']['source']['Location_ID']==k)
        xwalk_dict['ncrn']['Location']['source'].loc[mask, 'Lat_WGS84'] = v['Lat_WGS84']
        xwalk_dict['ncrn']['Location']['source'].loc[mask, 'Long_WGS84'] = v['Long_WGS84']

    # 4. add notes
    # concatenate all of the remaining field names and values into a string and assign to 'Notes'
    used_cols = [
        'Location_ID'
        ,'Site_ID'
        ,'Plot_Name'
        ,'Long_WGS84'
        ,'Lat_WGS84'
        ,'Datum'
        ,'Establish_Date'
        ,'Plot_Name'
        ,'Location_Type'
        ,'Active'
        ,'GRTS_Order'
    ]
    remaining_cols = [x for x in xwalk_dict['ncrn']['Location']['source'].columns if x not in used_cols]

    mymap = {}
    mymap['Location_ID'] = []
    mymap['Notes'] = []
    for loc in xwalk_dict['ncrn']['Location']['source'].Location_ID.unique():
        mymap['Location_ID'].append(loc)
        mystrings = []
        mask = (xwalk_dict['ncrn']['Location']['source']['Location_ID']==loc)
        for col in remaining_cols:
            mystrings.append(f'{col}:' + str(xwalk_dict['ncrn']['Location']['source'][mask][col].values[0]))
        mymap['Notes'].append(';'.join(mystrings))

    mymap = pd.DataFrame(mymap)
    xwalk_dict['ncrn']['Location']['source'] = xwalk_dict['ncrn']['Location']['source'].merge(mymap, on='Location_ID', how='left')
    xwalk_dict['ncrn']['Location']['source']['GRTS_Order'] = xwalk_dict['ncrn']['Location']['source']['GRTS_Order'].astype(int).astype(str)

    # `ncrn.Location.EnteredDate` cannot be NULL
    # in 539 of 663 cases, NCRN did not record an `Establish_Date`
    # we know that all sites were established in 2006, so we assign
    mask = (xwalk_dict['ncrn']['Location']['source']['Establish_Date'].isna())
    maxdate = max(xwalk_dict['ncrn']['Location']['source'][~mask].Establish_Date.values)
    xwalk_dict['ncrn']['Location']['source']['Establish_Date'] = np.where(mask, maxdate, xwalk_dict['ncrn']['Location']['source']['Establish_Date'])

    # `ncrn.Location.SiteID` cannot be NULL
    # NCRN did not assign every `LocationID` to a `SiteID`, though
    # to solve this problem, we add 'Unknown' rows to `ncrn.Site` and assign blank `ncrn.Location.SiteID`s to that `ncrn.Site.ID`
    for loctype in xwalk_dict['ncrn']['Location']['source']['Location_Type'].unique():
        for unitcode in xwalk_dict['ncrn']['Location']['source']['Unit_Code'].unique():
            mask = (xwalk_dict['ncrn']['Location']['source']['Site_ID'].isna()) & (xwalk_dict['ncrn']['Location']['source']['Location_Type']==loctype) & (xwalk_dict['ncrn']['Location']['source']['Unit_Code']==unitcode)
            xwalk_dict['ncrn']['Location']['source']['Site_ID'] = np.where(mask, 'Grassland_unknown_site_'+unitcode, xwalk_dict['ncrn']['Location']['source']['Site_ID'])

    return xwalk_dict

def _exception_ncrn_BirdDetection(xwalk_dict:dict, deletes:list) -> dict:
    """Clean up source.tbl_Field_Data

    1. Add additional rows from second source.tbl_Field_Data to ncrn.BirdDetection
    """
    # EXCEPTION 1: Add additional rows from second source.tbl_Field_Data to ncrn.BirdDetection
    con = dbc._db_connect('c')
    tbl = 'tbl_Field_Data'
    df = dbc._exec_qry(con=con, qry=f'get_c_{tbl}')
    con.close()
    xwalk_dict['ncrn']['BirdDetection']['source'] = pd.concat([xwalk_dict['ncrn']['BirdDetection']['source'], df])

    # EXCPETION 2: cascade delete dupliate site visits
    xwalk_dict['ncrn']['BirdDetection']['source'] = xwalk_dict['ncrn']['BirdDetection']['source'][xwalk_dict['ncrn']['BirdDetection']['source']['Event_ID'].isin(deletes)==False]

    # EXCEPTION 3: update `ncrn.BirdDetection.source.Distance_id` to a by-protocol value
    # i.e., NCRN used the same 1-5 integer to indicate distance regardless of protocol
    # the new data model allows you to use different distance increments by-protocol
    # to accomodate that, you need to use different IDs for each distance increment-protocol combination
    # step 1, make a lookup table with two columns: `ncrn.DetectionEvent.source.event_id` and `ncrn.DetectionEvent.source.protocol`
    lookup = xwalk_dict['ncrn']['DetectionEvent']['source'][['event_id','protocol_id']]
    # step 2, left-join the lookup to `ncrn.BirdDetection.source` to add the protocol column to `BirdDetection`
    xwalk_dict['ncrn']['BirdDetection']['source'] = xwalk_dict['ncrn']['BirdDetection']['source'].merge(lookup, left_on='Event_ID', right_on='event_id', how='left')
    # step 3, replace NaNs in `ncrn.BirdDetection.source.Distance_id` it's erroneous to exclude this at data-entry...
    mask = (xwalk_dict['ncrn']['BirdDetection']['source']['Distance_id'].isna())
    xwalk_dict['ncrn']['BirdDetection']['source']['Distance_id'] = np.where(mask, 2, xwalk_dict['ncrn']['BirdDetection']['source']['Distance_id']) # `ncrn.BirdDetection.source.Distance_id` cannot be blank
    # step 4: make a dummy variable in `ncrn.BirdDetection.source`
    xwalk_dict['ncrn']['BirdDetection']['source']['dummy'] = xwalk_dict['ncrn']['BirdDetection']['source']['Distance_id'].astype(int).astype(str) + '_' + xwalk_dict['ncrn']['BirdDetection']['source']['protocol_id'].astype(str)
    # step 5, make a lookup of three columns `ncrn.ProtocolDistanceClass.ID`, `ncrn.ProtocolDistanceClass.ProtocolID`, and `ncrn.ProtocolDistanceClass.Distance_id`
    lookup = pd.read_pickle(r'assets/ProtocolDistanceClass.pkl') # since this is a bridge table, its creation happens after the lookup; my order-of-operations didn't accomodate that so I have to reroute the dataframe
    # step 6, make a dummy variable in the lookup:
    lookup['dummy'] = lookup['Distance_id'].astype(str)  + '_' +  lookup['ProtocolID'].astype(str)
    # step 7: keep only 2 cols: lookup = lookup[['dummy','ID']]
    lookup = lookup[['dummy','ID']]
    lookup.rename(columns={'ID':'dummyid'}, inplace=True)
    # step 8: left-join lookup to `ncrn.BirdDetection.source`
    xwalk_dict['ncrn']['BirdDetection']['source'] = xwalk_dict['ncrn']['BirdDetection']['source'].merge(lookup, on='dummy', how='left')
    # step 9: replace the existing `ncrn.BirdDetection.source.Distance_id` values with the corresponding value from the lookup
    xwalk_dict['ncrn']['BirdDetection']['source']['Distance_id'] = xwalk_dict['ncrn']['BirdDetection']['source']['dummyid']
    # step 9: get rid of lookup values
    del xwalk_dict['ncrn']['BirdDetection']['source']['dummy']
    del xwalk_dict['ncrn']['BirdDetection']['source']['protocol_id']
    del xwalk_dict['ncrn']['BirdDetection']['source']['event_id']
    del xwalk_dict['ncrn']['BirdDetection']['source']['dummyid']

    # EXCEPTION 4: recode SexID== 0 to 1
    # In 79571 cases between 2017 and 2023, NCRN recorded a SexID of 0
    # 0 is not in the lookup table for sex so the db should never have allowed it to be entered
    # from reviewing paper datasheets, 'Undetermined' (SexID==1) is the most common sex choice
    # from spot-checking datasheets, 'Undetermined' birds were recorded as SexID==0
    # it looks like the database somehow assigned a 0 when it should have assigned a 1...
    mask = (xwalk_dict['ncrn']['BirdDetection']['source']['Sex_ID']==0)
    xwalk_dict['ncrn']['BirdDetection']['source']['Sex_ID'] = np.where(mask, 1, xwalk_dict['ncrn']['BirdDetection']['source']['Sex_ID'])

    # EXCEPTION 5: birds['ncrn']['BirdDetection']['tbl_load']['ProtocolDetectionTypeID'] cannot be NULL
    # scope: 383 site visits, 6365 individual birds
    mask = (xwalk_dict['ncrn']['BirdDetection']['source']['ID_Method_Code'].isna())
    xwalk_dict['ncrn']['BirdDetection']['source']['ID_Method_Code'] = np.where(mask, 'S', xwalk_dict['ncrn']['BirdDetection']['source']['ID_Method_Code'])
    
    # EXCEPTION 6: recode ncrn.BirdDetection.ProtocolDetectionTypeID from str to int present in ncrn.ProtocolDetectionType.ID
    # step 1, change lowercase to uppercase
    xwalk_dict['ncrn']['BirdDetection']['source']['ID_Method_Code'] = xwalk_dict['ncrn']['BirdDetection']['source']['ID_Method_Code'].str.upper()
    # step 2: replace unacceptable codes with acceptable codes
    # there are only three acceptable codes: ['C', 'S', 'V']
    # codes present in source data after capitalizing:
    #                    ID_Code   count
    # 2                        5       1
    # 3                        <       1
    # 8                        M       1
    # 10                       U       1
    # 12                       W       1
    # 13                       X       1
    # 0                        1       3
    # 4                        A       3
    # 7                        F       5
    # 6                        D       8
    # 1                        2      14
    # 11                       V   33929
    # 5                        C   77565
    # 9                        S  118138
    acceptable = ['C', 'S', 'V']
    present = list(xwalk_dict['ncrn']['BirdDetection']['source'].ID_Method_Code.unique())
    replacements = [x for x in present if x not in acceptable]
    lookup = {}
    for x in replacements:
        lookup[x] = 'S'
    for k,v in lookup.items():
        mask = (xwalk_dict['ncrn']['BirdDetection']['source']['ID_Method_Code']==k)
        xwalk_dict['ncrn']['BirdDetection']['source']['ID_Method_Code'] = np.where(mask, v, xwalk_dict['ncrn']['BirdDetection']['source']['ID_Method_Code'])
    # step 3: recode str to int
    before_colnames = xwalk_dict['ncrn']['BirdDetection']['source'].columns
    birddetection = xwalk_dict['ncrn']['BirdDetection']['source'].copy()
    detectionevent = xwalk_dict['ncrn']['DetectionEvent']['source'].copy()
    df = birddetection.merge(detectionevent[['event_id','protocol_id']], left_on='Event_ID', right_on='event_id', how='left')
    df['dummy'] = df['ID_Method_Code'].astype(str) + '_' + df['protocol_id'].astype(str)
    lookup = pd.read_pickle(r'assets/ProtocolDetectionType.pkl') # since this is a bridge table, its creation happens after the lookup; my order-of-operations didn't accomodate that so I have to reroute the dataframe
    # step 3: recode int to str
    rev_lookup = {}
    rev_lookup[1] = 'C'
    rev_lookup[2] = 'S'
    rev_lookup[3] = 'V'
    for k,v in rev_lookup.items():
        mask = (lookup['ID_Code']==k)
        lookup['ID_Code'] = np.where(mask, v, lookup['ID_Code'])
    lookup['dummy'] = lookup['ID_Code'].astype(str)  + '_' +  lookup['ProtocolID'].astype(str)
    lookup = lookup[['dummy','ID']]
    lookup.rename(columns={'ID':'dummyid'}, inplace=True)
    df = df.merge(lookup, on='dummy', how='left')
    df['ID_Method_Code'] = df['dummyid']
    xwalk_dict['ncrn']['BirdDetection']['source'] = df[before_colnames]

    # EXCEPTION 7:
    # species codes were entered wrong or have been updated since NCRN made tbl_species
    # step 1: update species codes per SME instruction: need to read in responses from `species_missing_attribute` and correct ncrn.BirdDetection.source.AOU_Code accordingly
    starters = [x for x in xwalk_dict['ncrn']['BirdDetection']['source']['AOU_Code'].unique() if x not in xwalk_dict['ncrn']['BirdSpecies']['source']['AOU_Code'].unique()]
    df = pd.read_excel(r'assets\birds_questions_20240215.xlsx', sheet_name='species_missing_attribute')
    df['corrected_AOU'] = None
    mask = (df['RESOLUTION'].str.contains('should be changed'))
    df['corrected_AOU'] = np.where(mask, df['RESOLUTION'].str[-4:], df['corrected_AOU'])
    df[df['AOU_Code'].isin(starters)==False]
    lookup = df[df['corrected_AOU'].isna()==False][['AOU_Code', 'corrected_AOU']]
    for i in range(len(lookup)):
        aou:str = lookup['AOU_Code'].values[i]
        corrected_aou:str = lookup['corrected_AOU'].values[i]
        mask = (xwalk_dict['ncrn']['BirdDetection']['source']['AOU_Code'] == aou)
        xwalk_dict['ncrn']['BirdDetection']['source']['AOU_Code'] = np.where(mask, corrected_aou, xwalk_dict['ncrn']['BirdDetection']['source']['AOU_Code'])
    # Step 2: add rows to ncrn.BirdSpecies.source per SME instruction to accomodate 'unidentified' birds present in NCRN data but absent from official species list
    # done in `_exception_ncrn_BirdSpecies()`

    # step 3: update ncrn.BirdDetection.BirdSpeciesParkID to a fk that's relative to park (to match bridge table primary key ncrn.BirdSpeciesPark.ID)
    before_colnames = xwalk_dict['ncrn']['BirdDetection']['source'].columns
    birddetection = xwalk_dict['ncrn']['BirdDetection']['source'].copy()
    detectionevent = xwalk_dict['ncrn']['DetectionEvent']['source'].copy()
    location = pd.read_pickle(r'assets/Location.pkl')
    detectionevent = detectionevent.merge(location[['Location_ID', 'Unit_Code']], left_on='location_id', right_on='Location_ID', how='left')
    df = birddetection.merge(detectionevent[['event_id','Unit_Code']], left_on='Event_ID', right_on='event_id', how='left')
    df['AOU_Code'] = df['AOU_Code'] + '_' + df['Unit_Code']
    xwalk_dict['ncrn']['BirdDetection']['source'] = df[before_colnames]

    xwalk_dict['ncrn']['BirdDetection']['source'].reset_index(drop=True, inplace=True)
    return xwalk_dict

def _exception_ncrn_Site(xwalk_dict:dict) -> dict:
    """Clean up source.tbl_Sites

    1. Add additional rows from second source.tbl_site to ncrn.Site
    """

    df = xwalk_dict['ncrn']['Location']['source'][['Site_ID', 'Active']]
    df = df[df['Site_ID'].isna()==False]
    df = df.drop_duplicates('Site_ID')
    if len(df) != len(df.Site_ID.unique()):
        print('Bug in `_exception_ncrn_Site()`. Review')
    xwalk_dict['ncrn']['Site']['source'] = xwalk_dict['ncrn']['Site']['source'].merge(df, on='Site_ID', how='left')

    # `ncrn.Location.SiteID` cannot be NULL
    # NCRN did not assign every `LocationID` to a `SiteID`, though
    # to solve this problem, we add 'Unknown' rows to `ncrn.Site` and assign blank `ncrn.Location.SiteID`s to that `ncrn.Site.ID`
    mydfs = {
        'grass': pd.DataFrame(columns=xwalk_dict['ncrn']['Site']['source'].columns)
        ,'forest': pd.DataFrame(columns=xwalk_dict['ncrn']['Site']['source'].columns)
    }
    parkunits = xwalk_dict['ncrn']['Site']['source'][xwalk_dict['ncrn']['Site']['source']['Unit_Code'].isna()==False].Unit_Code.unique()
    final_df = pd.DataFrame()
    for df in mydfs.keys():
        mydfs[df]['Unit_Code'] = parkunits
        if df == 'grass':
            mydfs[df]['Site_ID'] = 'Grassland_unknown_site'
            mydfs[df]['Site_Name'] = 'Grassland_unknown_site'
            mydfs[df]['Route_Type'] = 'Grassland'
        elif df == 'forest':
            mydfs[df]['Site_ID'] = 'Forest_unknown_site'
            mydfs[df]['Site_Name'] = 'Forest_unknown_site'
            mydfs[df]['Route_Type'] = 'Forest'
        else:
            print(f'error `exception_ncrn_Site` {df=}')
        mydfs[df]['Active'] = True
        mydfs[df]['Site_ID'] = mydfs[df]['Site_ID'] + '_' + mydfs[df]['Unit_Code']
        mydfs[df]['Site_Name'] = mydfs[df]['Site_Name'] + '_' + mydfs[df]['Unit_Code']
        final_df = pd.concat([final_df,mydfs[df]])
    xwalk_dict['ncrn']['Site']['source'] = pd.concat([xwalk_dict['ncrn']['Site']['source'],final_df])
    xwalk_dict['ncrn']['Site']['source'].reset_index(inplace=True, drop=True)

    # `Site_ID` '{54DDD5AD-5F8A-49D2-B636-C886D4ACCB8D}' is marked `Active` == False but it should be True
    mask = (xwalk_dict['ncrn']['Site']['source']['Site_ID']=='{54DDD5AD-5F8A-49D2-B636-C886D4ACCB8D}')
    xwalk_dict['ncrn']['Site']['source']['Active'] = np.where(mask, True, xwalk_dict['ncrn']['Site']['source']['Active'])

    return xwalk_dict

def _make_pk_fk_lookup(xwalk_dict:dict) -> dict:
    for schema in xwalk_dict.keys():
        for tbl in xwalk_dict[schema].keys():
            key_field = xwalk_dict[schema][tbl]['xwalk'][xwalk_dict[schema][tbl]['xwalk']['pk']==True].destination.values[0]
            try:
                xwalk_dict[schema][tbl]['pk_fk_lookup'] = xwalk_dict[schema][tbl]['tbl_load'][[key_field,'rowid']]
            except:
                print(f"FAIL: assign pk-fk relationships: birds['{schema}']['{tbl}']['pk_fk_lookup']")
                print(key_field)

    return xwalk_dict

def _add_sql_constraints(xwalk_dict:dict) -> dict:
    """Extract constraints and params from the CREATE TABLE SQL and add to xwalk dataframe for each table"""

    constraints = _preprocess_sql()
    constraints = _field_sql_constraints(constraints)
    constraints,xwalk_dict = _table_sql_constraints(constraints, xwalk_dict)

    for schema in constraints.keys():
        for tbl in constraints[schema].keys():
            try:
                xwalk_dict[schema][tbl]['xwalk'] = xwalk_dict[schema][tbl]['xwalk'].merge(constraints[schema][tbl]['constraint_df'], on='destination', how='left')
            except:
                print(f"FAIL CONSTRAINT MERGE: birds['{schema}']['{tbl}']")

    return xwalk_dict

def _preprocess_sql() -> dict:
    """Read SQL file, break into lines, clean lines, assign lines into dictionary of tables for further processing"""

    f = open(assets.CREATE_SQL, "r")
    lines:str = f.read()
    f.close()

    tbls:list = lines.split('CREATE TABLE')
    find:re.Pattern = re.compile(r"^([^;]*).*")
    clean_tbls:list = []
    for tbl in tbls:
        clean_tbl:str = re.search(find, tbl).group(0)
        clean_tbl:str = clean_tbl.strip().split(';', 1)[0]
        if 'IF CREATING NEW DATABASE, START HERE' not in clean_tbl:
            clean_tbls.append(clean_tbl)
        
    constraints:dict = {}
    colnames:list = ['destination','fieldtype','can_be_null','maxlen','default','pk','fk','references','maxval','minval']
    for schema in assets.TBL_XWALK.keys():
        constraints[schema] = {}
        for tbl in assets.TBL_XWALK[schema].keys():
            constraints[schema][tbl] = {
                'all_constraints':''
                ,'fieldwise':''
                ,'tablewise':''
                ,'constraint_df':pd.DataFrame(columns=colnames)
            }
    for schema in assets.TBL_ADDITIONS.keys():
        if schema not in constraints.keys():
            constraints[schema] = {}
        for tbl in assets.TBL_ADDITIONS[schema]:
            constraints[schema][tbl] = {
                'all_constraints':''
                ,'fieldwise':''
                ,'tablewise':''
                ,'constraint_df':pd.DataFrame(columns=colnames)
            }

    for tbl in clean_tbls:
        splits = tbl.split('.',1)
        schema = splits[0].replace('[','').replace(']','').strip()
        remaining = splits[1].split(' ',1)
        tbl_name = remaining[0].replace('[','').replace(']','').strip()
        all_constraints = remaining[1]
        try:
            constraints[schema][tbl_name]['all_constraints'] = all_constraints
        except:
            print(f"FAIL: constraints['{schema}']['{tbl_name}']")

    for schema in constraints.keys():
        for tbl in constraints[schema].keys():
            lines = constraints[schema][tbl]['all_constraints'].split('\n')
            lines = [x.strip() for x in lines if x.startswith('(')==False and x.startswith(')')==False]
            lines = [x for x in lines if x.startswith('(')==False and x.startswith(')')==False]
            field_lines = [x for x in lines if x.startswith('CONSTRAINT')==False]
            table_lines = [x for x in lines if x.startswith('CONSTRAINT')==True]
            constraints[schema][tbl]['fieldwise'] = field_lines
            constraints[schema][tbl]['tablewise'] = table_lines

    return constraints

def _field_sql_constraints(constraints:dict) -> dict:
    """Parse CREATE TABLE SQL into a dataframe of constraints for each field
    
    Field-constraints include:
    field type
    nullable
    maximum length
    default value
    """
            
    for schema in constraints.keys():
        for tbl in constraints[schema].keys():
            field_lines = constraints[schema][tbl]['fieldwise']
            fieldnames = []
            fieldtypes = []
            maxlens = []
            nullbools = []
            defaults = []
            for field_line in field_lines:
                fieldname = field_line.split(' ',1)[0].strip().replace('[','').replace(']','').strip()
                fieldtype = field_line.split(' ',1)[1].strip().split(' ',1)[0].strip()
                if 'NOT NULL' in field_line:
                    nullbool=False
                else:
                    nullbool=True
                if fieldtype == 'VARCHAR' or fieldtype == 'CHAR':
                    try:
                        maxlen = re.findall(r'\(.*?\)', field_line.split(' ',1)[1].strip().split(' ',1)[1].strip())[0].replace('(','').replace(')','')
                        if maxlen == 'MAX':
                            maxlen = np.NaN
                        else:
                            maxlen = int(maxlen)
                    except:
                        print(field_line)
                elif fieldtype == 'DECIMAL':
                    tmp_chars = re.findall(r'\(.*?\)', field_line)[0].strip()
                    maxlen = tmp_chars
                else:
                    maxlen = np.NaN
                if 'DEFAULT' in field_line:
                    try:
                        default = re.findall(r'DEFAULT.*?,', field_line)[0].strip().rsplit(',',1)[0]
                    except:
                        print(f'ERROR: {field_line}')
                else:
                    default = np.NaN
                fieldnames.append(fieldname)
                nullbools.append(nullbool)
                fieldtypes.append(fieldtype)
                maxlens.append(maxlen)
                defaults.append(default)
                
            constraints[schema][tbl]['constraint_df']['destination']=fieldnames
            constraints[schema][tbl]['constraint_df']['fieldtype']=fieldtypes
            constraints[schema][tbl]['constraint_df']['maxlen']=maxlens
            constraints[schema][tbl]['constraint_df']['can_be_null']=nullbools
            constraints[schema][tbl]['constraint_df']['default']=defaults

    return constraints

def _table_sql_constraints(constraints:dict, xwalk_dict:dict) -> tuple:
    """Parse CREATE TABLE SQL into a dataframe of constraints for each table
    
    Table constraints include:
    primary-key/foreign-key relationships
    unique values within and among fields
    logical keys that are unique combinations of fields (based on business logic)
    """

    for schema in constraints.keys():
        for tbl in constraints[schema].keys():
            table_lines = constraints[schema][tbl]['tablewise']
            # base cases
            constraints[schema][tbl]['constraint_df']['pk'] = False
            constraints[schema][tbl]['constraint_df']['fk'] = False
            constraints[schema][tbl]['constraint_df']['references'] = None
            constraints[schema][tbl]['constraint_df']['maxval'] = np.NaN
            constraints[schema][tbl]['constraint_df']['minval'] = np.NaN
            # exceptions to base cases
            for table_line in table_lines:
                if 'PRIMARY KEY' in table_line:
                    pk = True
                    try:
                        fieldname = re.findall(r'\(.*?\)',re.findall(r'PRIMARY KEY.*?$', table_line)[0].strip())[0].split(' ',1)[0].replace('[','').replace(']','').replace('(','').replace(')','')
                        constraints[schema][tbl]['constraint_df']['pk']=np.where((constraints[schema][tbl]['constraint_df']['destination']==fieldname),pk,constraints[schema][tbl]['constraint_df']['pk'])
                    except:
                        print(f"FAIL PK EXTRACT: constraints['{schema}']['{tbl}']")
                        print(table_line)
                if 'FOREIGN KEY' in table_line:
                    fk = True
                    try:
                        fieldname = re.findall(r'FOREIGN KEY.*?REFERENCES', table_line)[0].replace('FOREIGN KEY','').replace('REFERENCES','').replace('(','').replace(')','').replace('[','').replace(']','').strip()
                    except:
                        print(f"FAIL FK EXTRACT: constraints['{schema}']['{tbl}']")
                        print(table_line)
                    try:
                        constraints[schema][tbl]['constraint_df']['fk']=np.where((constraints[schema][tbl]['constraint_df']['destination']==fieldname),fk,constraints[schema][tbl]['constraint_df']['fk'])
                    except:
                        print(f"FAIL FK assign: constraints['{schema}']['{tbl}']")
                        print(table_line)
                    try:
                        step1 = re.findall(r'REFERENCES.*?\)', table_line)[0]
                        if step1.endswith(','):
                            step1 = step1.rsplit(',',1)[0]
                        step1 = step1.split('REFERENCES',1)[-1].strip().replace('[','').replace(']','').replace(' ','')
                        step2 = step1.split('.')
                        if len(step2) <3:
                            for i in range(len(step2)):
                                if '(' in step2[i]:
                                    w = step2[i].split('(')
                                    step2.pop(i)
                                    for x in w:
                                        step2.append(x.replace(')',''))
                        if len(step2) == 2 and step2[0]=='Role' and step2[1]=='ID':
                            tmp = ['dbo']
                            for x in step2:
                                tmp.append(x)
                            step2 = tmp
                            refers_to = '.'.join(step2)
                        elif len(step2) == 2 and step2[0]=='User' and step2[1]=='ID':
                            tmp = ['dbo']
                            for x in step2:
                                tmp.append(x)
                            step2 = tmp
                            refers_to = '.'.join(step2)
                        elif len(step2) <3:
                            print(f"FAIL REFERENCES EXTRACT: constraints['{schema}']['{tbl}']")
                        else:
                            refers_to = '.'.join(step2)
                    except:
                        print(f"FAIL REFERENCES EXTRACT: constraints['{schema}']['{tbl}']")
                        print(table_line)
                    try:
                        constraints[schema][tbl]['constraint_df']['references']=np.where((constraints[schema][tbl]['constraint_df']['destination']==fieldname),refers_to,constraints[schema][tbl]['constraint_df']['references'])
                    except:
                        print(f"FAIL REFERENCES assign: constraints['{schema}']['{tbl}']")
                        print(table_line)
                if 'UNIQUE' in table_line:
                    try:
                        step1 = re.findall(r'UNIQUE.*?$', table_line)[0]
                    except:
                        print(f"FAIL UNIQUE parse step1: constraints['{schema}']['{tbl}']")
                        print(table_line)
                    try:
                        step2 = re.findall(r'\(.*?\)',step1)[0]
                        if step2.endswith(','):
                            step2 = step2.rsplit(',',1)[0]
                    except:
                        print(f"FAIL UNIQUE parse step2: constraints['{schema}']['{tbl}']")
                        print(table_line)
                    try:
                        step3 = step2.strip().replace('(','').replace(')','').strip().replace(' ','').replace('ASC','').replace('[','').replace(']','')
                    except:
                        print(f"FAIL UNIQUE parse step3: constraints['{schema}']['{tbl}']")
                        print(table_line)
                    try:
                        xwalk_dict[schema][tbl]['unique_vals'].append(step3)
                    except:
                        print(f"FAIL UNIQUE append step3: constraints['{schema}']['{tbl}']")
                        print(table_line)

    return constraints,xwalk_dict

def _find_dupe_site_visits(xwalk_dict:dict) -> dict:
    """
    Find duplicate site visits
    
    Duplicate site visits occur when there are >1 ncrn.DetectionEvent.ID for a unique combination of ncrn.DetectionEvent.LocationID + ncrn.DetectionEvent.StartDateTime + ncrn.DetectionEvent.ProtocolID
    This aligns with sql:
    CONSTRAINT [UniqueLocationDate] UNIQUE NONCLUSTERED ([LocationID] ASC, [StartDateTime] ASC, [ProtocolID] ASC)
    """

    DetectionEvent = xwalk_dict['ncrn']['DetectionEvent']['source'].copy()
    BirdDetection = xwalk_dict['ncrn']['BirdDetection']['source'].copy()
    DetectionEvent['dummy'] = DetectionEvent['location_id'].astype(str)+DetectionEvent['Date'].astype(str)+DetectionEvent['protocol_id'].astype(str)
    # DetectionEvent['dummy'] = DetectionEvent['location_id'].astype(str)+DetectionEvent['activity_start_datetime'].dt.date.astype(str)+DetectionEvent['protocol_id'].astype(str)
    DetectionEvent = DetectionEvent[['event_id','dummy']]
    BirdDetection = BirdDetection[['Event_ID']]

    tmp =DetectionEvent.groupby(['dummy']).size().reset_index(name='count').sort_values(['count'], ascending=True)
    tmp = tmp[tmp['count']>1]
    ghosts = ['NoneNaT2','NoneNaT1']
    tmp = tmp[tmp['dummy'].isin(ghosts)==False]
    lookup = DetectionEvent[DetectionEvent['dummy'].isin(tmp.dummy.unique())]
    BirdDetection = BirdDetection[BirdDetection['Event_ID'].isin(lookup.event_id.unique())]
    BirdDetection = BirdDetection.merge(lookup, left_on='Event_ID', right_on='event_id')[['Event_ID','dummy']]

    counter=0
    outcomes = {
        'delete':[]
        ,'review':{}
    }
    # CASE 1: one `DetectionEvent.dummy` has >1 `DetectionEvent.Event_ID` but we CAN determine which `DetectionEvent.Event_ID` to keep based on whether the `DetectionEvent.Event_ID` was present as a `BirdDetection.Event_ID`
    for d in BirdDetection.dummy.unique():
        mysub = BirdDetection[BirdDetection['dummy']==d]
        visits = list(mysub.Event_ID.unique())
        n_visits = len(mysub.Event_ID.unique())
        if n_visits == 0: # if a dummy has >1 Event_ID and none of the Event_IDs recorded any bird detections, it doesn't matter which Event_ID we keep, so delete all but the [0]th
            del_this = list(lookup[lookup['dummy']==d].event_id.unique())
            del_this = del_this[1:]
            for y in del_this:
                outcomes['delete'].append(y)
        elif n_visits == 1: # if a dummy has >1 Event_ID and only one of the Event_IDs recorded any bird detections, keep the Event_ID that logged birds and delete all others
            del_this = list(lookup[lookup['dummy']==d].event_id.unique())
            del_this = [x for x in del_this if x not in visits]
            for y in del_this:
                outcomes['delete'].append(y)
        elif n_visits >1: # if a dummy has >1 Event_ID and each of the Event_IDs recorded bird detections, store for processing in CASE 2
            counter+=1
            outcomes['review'][d] = {
                'counter': n_visits
                ,'DetectionEventID':visits
            }

    DetectionEvent = xwalk_dict['ncrn']['DetectionEvent']['source'].copy()
    BirdDetection = xwalk_dict['ncrn']['BirdDetection']['source'].copy()
    DetectionEvent = DetectionEvent[DetectionEvent['event_id'].isin(outcomes['delete'])==False] # update original dataset to "look like" we already deleted the `delete`s identified in step 1, so we are reviewing the dataset at the correct "stage"
    BirdDetection = BirdDetection[BirdDetection['Event_ID'].isin(outcomes['delete'])==False] # update original dataset to "look like" we already deleted the `delete`s identified in step 1, so we are reviewing the dataset at the correct "stage"
    DetectionEvent['dummy'] = DetectionEvent['location_id'].astype(str)+DetectionEvent['Date'].astype(str)+DetectionEvent['protocol_id'].astype(str)
    # DetectionEvent['dummy'] = DetectionEvent['location_id'].astype(str)+DetectionEvent['activity_start_datetime'].dt.date.astype(str)+DetectionEvent['protocol_id'].astype(str)
    DetectionEvent = DetectionEvent[['event_id','dummy']]
    BirdDetection = BirdDetection[['Event_ID','AOU_Code']]

    tmp =DetectionEvent.groupby(['dummy']).size().reset_index(name='count').sort_values(['count'], ascending=True)
    tmp = tmp[tmp['count']>1]
    tmp = tmp[tmp['dummy'].isin(ghosts)==False]
    lookup = DetectionEvent[DetectionEvent['dummy'].isin(tmp.dummy.unique())]
    BirdDetection = BirdDetection[BirdDetection['Event_ID'].isin(lookup.event_id.unique())]
    BirdDetection = BirdDetection.merge(lookup, left_on='Event_ID', right_on='event_id')[['Event_ID','AOU_Code','dummy']]

    lookup = lookup[lookup['event_id'].isin(outcomes['delete'])==False]
    BirdDetection = BirdDetection[BirdDetection['Event_ID'].isin(outcomes['delete'])==False]
    DetectionEvent = DetectionEvent[(DetectionEvent['event_id'].isin(outcomes['delete'])==False)&(DetectionEvent['dummy'].isin(lookup['dummy']))]

    # CASE 2: one `DetectionEvent.dummy` has >1 `DetectionEvent.Event_ID` but we CANNOT determine which `DetectionEvent.Event_ID` to keep based on whether the `DetectionEvent.Event_ID` was present as a `BirdDetection.Event_ID`
    # If we can't determine which `DetectionEvent.Event_ID` to keep based on its presence/absence as a `BirdDetection.Event_ID`, subset `BirdDetection` by each `Event_ID` and compare
    # If the subsets are identical, it doesn't matter which `Event_ID` we keep, so just keep the [0]th
    # if the subsets are different, we have no choice but review the paper datasheet
    for dummy in BirdDetection.dummy.unique():
        n_records = []
        n_unique_sp = []
        species = []
        dummy_subset = BirdDetection[BirdDetection['dummy']==dummy]
        for visit in dummy_subset.Event_ID.unique():
            visit_subset = dummy_subset[dummy_subset['Event_ID']==visit]
            n_row = len(visit_subset) # how many rows are in the `visit`?
            n_sp = len(visit_subset.AOU_Code.unique()) # how many unique species are in the `visit`?
            sp = visit_subset.AOU_Code.values.tolist() # how many of each species were observed in the `visit`?
            sp.sort()
            n_records.append(n_row)
            n_unique_sp.append(n_sp)
            species.append(sp)
        n_records = set(n_records)
        n_unique_sp = set(n_unique_sp)
        species = set(tuple(i) for i in species)
        if len(n_records) == 1 and len(n_unique_sp) == 1 and len(species) == 1: # if these are all == 1, we know the subsets are identical so we just keep the [0]th `Event_ID`
            delete = outcomes['review'][dummy]['DetectionEventID'][1:]
            for d in delete:
                outcomes['delete'].append(d)
            outcomes['review'].pop(dummy)
        else: # if any of the above != 1, we know the subsets are different, so we leave the `dummy` and its associated `Event_ID`s in the 'review' category
            pass

    # CASE 3: review the paper datasheet for each dupe identified in CASE 2 and update the dataset accordingly
    outcomedf = pd.read_excel(assets.BIRDS_RESEARCH, sheet_name='research')
    deletes = outcomedf[outcomedf['resolution']=='delete']

    findkeys = [] # need to backtrace the key since I used a slightly different key format...
    for d in outcomes['review'].keys():
        for e in outcomes['review'][d]['DetectionEventID']:
            if e in deletes.event_id.unique():
                findkeys.append(d)
    findkeys = set(findkeys)

    for dummy in findkeys: # remove the key from the `review`s
        try: # try-except because in cases where a `dummy` has >1 event to pop, pop will only work for the first attempt
            outcomes['review'].pop(dummy)
        except:
            # print(dummy)
            continue
    for e in deletes.event_id.unique(): # add the event to `delete`s
        outcomes['delete'].append(e)
    
    return outcomes

def _concat_deletes(xwalk_dict:dict, ghosts:list=assets.DELETES) -> list:
    dupes = _find_dupe_site_visits(xwalk_dict)
    deletes = ghosts.copy()
    for x in dupes['delete']:
        deletes.append(x)

    return deletes
