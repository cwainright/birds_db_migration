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

TBL_XWALK = assets.TBL_XWALK

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
        ,'SamplingMethodID'
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
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'SamplingMethodID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'protocol_name', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `Observer_ContactID`
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'Observer_ContactID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'observer', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `Recorder_ContactID`
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'Recorder_ContactID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'recorder', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `ProtocolNoiseLevelID`: [lu.NoiseLevel]([ID]) # does NCRN capture an equivalent to this?
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'ProtocolNoiseLevelID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'protocol_id', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `ProtocolWindCodeID`: [lu.WindCode]([ID]) # does NCRN capture an equivalent to this?
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'ProtocolWindCodeID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'protocol_id', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `ProtocolPrecipitationTypeID`: [lu.PrecipitationType]([ID]) # does NCRN capture an equivalent to this?
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'ProtocolPrecipitationTypeID')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, 'protocol_id', xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
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
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, "xwalk_dict['ncrn']['DetectionEvent']['tbl_load']['TemperatureUnitCode'] = 1", xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['note'] =  np.where(mask, "temperature collected in celsius", xwalk_dict['ncrn']['DetectionEvent']['xwalk']['note'])
    # ExcludeNote
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'ExcludeNote')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, "xwalk_dict['ncrn']['DetectionEvent']['tbl_load']['ExcludeNote']=xwalk_dict['ncrn']['DetectionEvent']['source']['flaggroup']+': '+xwalk_dict['ncrn']['DetectionEvent']['source']['label']", xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])
    # `ExcludeEvent`: this is a 1 or 0 depending on `ExcludeNote`
    mask = (xwalk_dict['ncrn']['DetectionEvent']['xwalk']['destination'] == 'ExcludeEvent')
    xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'] =  np.where(mask, "xwalk_dict['ncrn']['DetectionEvent']['tbl_load']['ExcludeEvent'] = np.where((xwalk_dict['ncrn']['DetectionEvent']['source']['label'].isna()), 0, 1)", xwalk_dict['ncrn']['DetectionEvent']['xwalk']['source'])

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
    xwalk_dict['ncrn']['BirdDetection']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['BirdDetection']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
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
        ,'Version'
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
    mask = (xwalk_dict['ncrn']['Protocol']['xwalk']['destination'] == 'Version')
    xwalk_dict['ncrn']['Protocol']['xwalk']['source'] =  np.where(mask, 'Protocol_Version', xwalk_dict['ncrn']['Protocol']['xwalk']['source'])
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

    # do we really collect data from >4k sites?!

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'SiteID'
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

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['Location']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['Location']['xwalk']['source'])
    xwalk_dict['ncrn']['Location']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['Location']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['Location']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['Location']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['Location']['xwalk']['calculation'])
    xwalk_dict['ncrn']['Location']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['Location']['xwalk']['source'])
    xwalk_dict['ncrn']['Location']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn']['Location']['xwalk']['note'])

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
    xwalk_dict['ncrn']['Park']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['Park']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
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
    xwalk_dict['lu']['TimeInterval']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['TimeInterval']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
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
    xwalk_dict['lu']['WindCode']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['WindCode']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
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
    xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['DataProcessingLevel']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
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
        'ID'
        ,'Code'
        ,'Label'
        ,'Description'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['lu']['DetectionType']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['lu']['DetectionType']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['lu']['DetectionType']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['lu']['DetectionType']['xwalk']['destination'] == 'ID')
    xwalk_dict['lu']['DetectionType']['xwalk']['source'] =  np.where(mask, 'ID_Code', xwalk_dict['lu']['DetectionType']['xwalk']['source'])
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
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['lu']['DetectionType']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['lu']['DetectionType']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['DetectionType']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
    xwalk_dict['lu']['DetectionType']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['lu']['DetectionType']['xwalk']['calculation'])

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
    xwalk_dict['lu']['DistanceClass']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['DistanceClass']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
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
    xwalk_dict['lu']['GeodeticDatum']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['GeodeticDatum']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
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
    xwalk_dict['lu']['Sex']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['Sex']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
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

def _ncrn_Contact(xwalk_dict:dict) -> dict: ##TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO##
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
    xwalk_dict['ncrn']['Contact']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['Contact']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
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
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['lu']['PrecipitationType']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['lu']['PrecipitationType']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['lu']['PrecipitationType']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['lu']['PrecipitationType']['xwalk']['destination'] == 'ID')
    xwalk_dict['lu']['PrecipitationType']['xwalk']['source'] =  np.where(mask, 'Sky_Code', xwalk_dict['lu']['PrecipitationType']['xwalk']['source'])
    # Code
    mask = (xwalk_dict['lu']['PrecipitationType']['xwalk']['destination'] == 'Code')
    xwalk_dict['lu']['PrecipitationType']['xwalk']['source'] =  np.where(mask, 'Sky_Code', xwalk_dict['lu']['PrecipitationType']['xwalk']['source'])
    # Label
    mask = (xwalk_dict['lu']['PrecipitationType']['xwalk']['destination'] == 'Label')
    xwalk_dict['lu']['PrecipitationType']['xwalk']['source'] =  np.where(mask, 'Code_Description', xwalk_dict['lu']['PrecipitationType']['xwalk']['source'])
    # Description
    mask = (xwalk_dict['lu']['PrecipitationType']['xwalk']['destination'] == 'Description')
    xwalk_dict['lu']['PrecipitationType']['xwalk']['source'] =  np.where(mask, 'Code_Description', xwalk_dict['lu']['PrecipitationType']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['lu']['PrecipitationType']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['lu']['PrecipitationType']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['PrecipitationType']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
    xwalk_dict['lu']['PrecipitationType']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['lu']['PrecipitationType']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
        'SortOrder'
        ,'Rowversion'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['lu']['PrecipitationType']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['lu']['PrecipitationType']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['PrecipitationType']['xwalk']['calculation'])
    xwalk_dict['lu']['PrecipitationType']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['lu']['PrecipitationType']['xwalk']['source'])
    xwalk_dict['lu']['PrecipitationType']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['lu']['PrecipitationType']['xwalk']['note'])

    return xwalk_dict

def _ncrn_BirdSpeciesPark(xwalk_dict:dict) -> dict: ##TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO##
    """Crosswalk source.tlu_Species to destination.ncrn.BirdSpeciesPark

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    # ncrn.BirdSpeciesPark is a park-level grouping variable for species in ncrn.BirdGroups
    # NCRN doesn't maintain attributes like this so it's probably a blank table
        
    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'BirdSpeciesID'
        ,'ParkID'
        ,'ProtectedStatusID'
        ,'Comment'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['BirdGroups']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'])
    # BirdSpeciesID
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'] == 'BirdSpeciesID')
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] =  np.where(mask, 'BirdSpeciesID', xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'])
    # BirdGroupID
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'] == 'BirdGroupID')
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] =  np.where(mask, 'BirdGroupID', xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'])


    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['BirdGroups']['xwalk']['calculation'])

    # Blanks
    blank_fields = [
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['BirdGroups']['xwalk']['calculation'])
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn']['BirdGroups']['xwalk']['note'])

    return xwalk_dict

def _ncrn_BirdSpeciesGroup(xwalk_dict:dict) -> dict:
    """Crosswalk source.tlu_Species to destination.ncrn.BirdSpeciesPark

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    # ncrn.BirdSpeciesGroups is a species-level grouping variable for species in ncrn.BirdGroups
    # NCRN doesn't maintains these attributes for a handful of species

    # 1:1 fields
    one_to_one_fields = [
        'ID'
        ,'BirdSpeciesID'
        ,'BirdGroupID'
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn']['BirdGroups']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] =  np.where(mask, 'ID', xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'])
    # BirdSpeciesID
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'] == 'BirdSpeciesID')
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] =  np.where(mask, 'BirdSpeciesID', xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'])
    # BirdGroupID
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'] == 'BirdGroupID')
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] =  np.where(mask, 'BirdGroupID', xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'])


    # Calculated fields
    calculated_fields = [
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn']['BirdGroups']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'])
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn']['BirdGroups']['xwalk']['calculation'])

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
    xwalk_dict['ncrn']['BirdGroups']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['BirdGroups']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
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

def _ncrn_BirdSpecies(xwalk_dict:dict) -> dict: ##TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO##
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
    xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['BirdSpecies']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
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
    xwalk_dict['lu']['NoiseLevel']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['NoiseLevel']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
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
    xwalk_dict['ncrn']['AuditLog']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['AuditLog']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
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
    xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['ncrn']['AuditLogDetail']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
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

def _lu_SamplingMethod(xwalk_dict:dict) -> dict: ##TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO##
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
    xwalk_dict['lu']['SamplingMethod']['xwalk']['source'] =  np.where(mask, 'Protocol_ID', xwalk_dict['lu']['SamplingMethod']['xwalk']['source'])
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
    xwalk_dict['lu']['SamplingMethod']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['SamplingMethod']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
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

def _lu_ExperienceLevel(xwalk_dict:dict) -> dict: ##TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO##
    """Crosswalk lu.ExperienceLevel

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
    # Description
    mask = (xwalk_dict['lu']['ExperienceLevel']['xwalk']['destination'] == 'Description')
    xwalk_dict['lu']['ExperienceLevel']['xwalk']['source'] =  np.where(mask, 'Description', xwalk_dict['lu']['ExperienceLevel']['xwalk']['source'])
    # SortOrder
    mask = (xwalk_dict['lu']['ExperienceLevel']['xwalk']['destination'] == 'SortOrder')
    xwalk_dict['lu']['ExperienceLevel']['xwalk']['source'] =  np.where(mask, 'SortOrder', xwalk_dict['lu']['ExperienceLevel']['xwalk']['source'])

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
    xwalk_dict['lu']['Habitat']['xwalk']['source'] = np.where(mask, 'placeholder', xwalk_dict['lu']['Habitat']['xwalk']['source']) # TODO: DELETE THIS LINE, FOR TESTING ONLY# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO# TODO
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


def _exception_ncrn_DetectionEvent(xwalk_dict:dict) -> dict:
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

    return xwalk_dict

def _exception_ncrn_BirdSpecies(xwalk_dict:dict) -> dict:
    """The source table tlu_Species is missing some required attributes so add them"""

    # TODO: only overwrite when there's a blank
    # If there's a value in xwalk_dict['ncrn']['BirdSpecies']['source'] for an attribute but not in csv for that attribute, keep the one from source
    
    # get the best-available taxonomic info
    csv = pd.read_csv(r'assets\db\official_BirdSpecies.csv')
    df = xwalk_dict['ncrn']['BirdSpecies']['source'].copy()
    df = df[[x for x in df.columns if x == 'AOU_Code' or x not in csv.columns]]
    df = df.merge(csv, on='AOU_Code', how='left')

    # get the best-available secondary attributes
    csv = pd.read_csv(r'assets\db\bird_species.csv') # 'integration' [netnmidn].[BirdSpecies]
    csv = csv[['Code', 'IsActive', 'IsTarget', 'SynonymID']]
    csv.rename(columns={'Code':'AOU_Code'}, inplace=True)
    df = df.merge(csv, on='AOU_Code', how='left')

    xwalk_dict['ncrn']['BirdSpecies']['source'] = df

    return xwalk_dict

def _add_row_id(xwalk_dict:dict) -> dict:
    # 1-index row-id for all tables

    for schema in xwalk_dict.keys():
        for tbl in xwalk_dict[schema].keys():
            if len(xwalk_dict[schema][tbl]['tbl_load'])>0:
                xwalk_dict[schema][tbl]['tbl_load']['rowid'] = xwalk_dict[schema][tbl]['tbl_load'].index+1

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

def _exception_ncrn_BirdSpeciesGroups(xwalk_dict:dict) -> dict: ##TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO##
    """This table is empty in NETNMIDN but it's a bridge table between ncrn.BirdSpecies and ncrn.BirdGroups"""
    # TODO: create `source` here
    # TODO: create `source_name` here

    # The dataframe below is wide:
    # testdict['ncrn']['BirdSpecies']['source'][testdict['ncrn']['BirdSpecies']['source']['Primary_Habitat'].isna()==False]

    # If you cast it long-ways on the attributes
    # So a unique combination of AOU_Code and attribute is a row, you'll match the schema for ncrn.BirdSpeciesGroups

    # just make sure that you manage referential integrity between ncrn.BirdSpecies and ncrn.BirdGroups

    # e.g., 
    # ID |  BirdSpeciesID |    BirdGroupID                         | Rowversion
    # 1  |       465      |    20080421161312-627642035.484314     | NaN
    # 2  |       465      |    20080421161058-412766814.231873     | NaN

    xwalk_dict['ncrn']['BirdSpeciesGroups']['source_name'] = xwalk_dict['ncrn']['BirdSpecies']['source_name']

    return xwalk_dict

def _exception_ncrn_ProtocolDetectionType(xwalk_dict:dict) -> dict:
    """This table is empty in NETNMIDN but it's a bridge table between ncrn.BirdSpecies and ncrn.BirdGroups"""
    # TODO: create `source` here
    # TODO: create `source_name` here

    xwalk_dict['ncrn']['ProtocolDetectionType']['source_name'] = 'tbd'
    # xwalk_dict['ncrn']['ProtocolDetectionType']['source_name'] = pd.DataFrame()

    return xwalk_dict

def _exception_ncrn_BirdSpeciesPark(xwalk_dict:dict) -> dict: ##TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO##
    """This is a bridge table between ncrn.BirdSpecies and ncrn.Park"""
    # TODO: create `source` here
    # TODO: create `source_name` here

    return xwalk_dict

def _exception_lu_ExperienceLevel(xwalk_dict:dict) -> dict:
    """NCRN doesn't keep this table and NETNMIDN's tbl NETNMIDN_Landbirds.lu.ExperienceLevel is empty so we add a dataframe here"""

    exp_lev = {
        'ID':[1,2,3,4]
        ,'Code':['unk', 'tech', 'lead', 'pro']
        ,'Description':['Unknown level of experience', 'Cursory training and basic experience in mid-Atlantic grassland/forestland bird identification', 'Considerable training and solid experience in mid-Atlantic grassland/forestland bird identification', 'An expert in mid-Atlantic grassland/forestland bird identification']
        ,'SortOrder':[np.NaN, np.NaN, np.NaN, np.NaN]
    }

    exp_lev = pd.DataFrame(exp_lev)

    xwalk_dict['lu']['ExperienceLevel']['source_name'] = 'NETNMIDN_Landbirds.lu.ExperienceLevel'
    xwalk_dict['lu']['ExperienceLevel']['source'] = exp_lev.copy()

    return xwalk_dict

def _exception_ncrn_Contact(xwalk_dict:dict) -> dict:
    """map experience level to ncrn.Contact.source.ExperienceLevelID"""

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

    return xwalk_dict

def _exception_ncrn_Location(xwalk_dict:dict) -> dict:
    """map experience level to ncrn.Contact.source.ExperienceLevelID"""

    xwalk_dict['ncrn']['Location']['source'] = xwalk_dict['ncrn']['Location']['source'][xwalk_dict['ncrn']['Location']['source']['Location_ID'].isin(xwalk_dict['ncrn']['DetectionEvent']['source'].location_id.unique())]

    return xwalk_dict

