"""
Hard code the crosswalk for each table.

A crosswalks are dataframes that document where `destination` data come from in the `source` data.

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

Information documented in the crosswalk is executed downstream in this pipeline.

"""

import pandas as pd
import numpy as np

def _tbl_detection_event(xwalk_dict:dict) -> dict:
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
    ]
    # assign grouping variable `calculation` for the 1:1 fields
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'].isin(one_to_one_fields))
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['calculation'] =  np.where(mask, 'map_source_to_destination_1_to_1', xwalk_dict['ncrn.DetectionEvent']['xwalk']['calculation'])
    # ID
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'ID')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'event_id', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    # LocationID
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'LocationID')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'location_id', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    # ProtocolID
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'ProtocolID')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'protocol_id', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    # EnteredBy
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'EnteredBy')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'entered_by', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    # AirTemperature
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'AirTemperature')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'temperature', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    # Notes
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'Notes')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'event_notes', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    # EnteredDateTime
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'EnteredDateTime')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'entered_date', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['note'] =  np.where(mask, 'ncrn.entered_date is collected as a date, not a datetime, so the entered time is unknown', xwalk_dict['ncrn.DetectionEvent']['xwalk']['note'])
    # DataProcessingLevelID
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'DataProcessingLevelID')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'dataprocessinglevelid', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    # DataProcessingLevelDate
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'DataProcessingLevelDate')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'dataprocessingleveldate', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    # RelativeHumidity
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'RelativeHumidity')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'humidity', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])

    # Calculated fields
    calculated_fields = [
        'AirTemperatureRecorded'
        ,'StartDateTime'
        ,'IsSampled'
        ,'SamplingMethodID'
        ,'Observer_ContactID'
        ,'Recorder_ContactID'
        ,'Observer_ExperienceLevelID'
        ,'ProtocolNoiseLevelID'
        ,'ProtocolWindCodeID'
        ,'ProtocolPrecipitationTypeID'
        ,'TemperatureUnitCode'
    ]
    # assign grouping variable `calculation` for the calculated fields
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'].isin(calculated_fields))
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['calculation'] =  np.where(mask, 'calculate_dest_field_from_source_field', xwalk_dict['ncrn.DetectionEvent']['xwalk']['calculation'])
    # `AirTemperatureRecorded` BIT type (bool as 0 or 1); not a NCRN field. Should be 1 when the row has a value in `AirTemperature`
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'AirTemperatureRecorded')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] = np.where(mask, r"mask=xwalk_dict['ncrn.DetectionEvent']['source'][xwalk_dict['ncrn.DetectionEvent']['source'].temperature.isna()]\xwalk_dict['ncrn.DetectionEvent']['destination']=np.where(mask, 0, xwalk_dict['ncrn.DetectionEvent']['destination'])\xwalk_dict['ncrn.DetectionEvent']['destination']=np.where(~mask, 1, xwalk_dict['ncrn.DetectionEvent']['destination'])", xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['note'] =  np.where(mask, "BIT type (bool as 0 or 1); not a NCRN field. Should be 1 when the row has a value in `AirTemperature`", xwalk_dict['ncrn.DetectionEvent']['xwalk']['note'])
    # `StartDateTime` need to make datetime from src_tbl.Date and src_tbl.Start_Time
    # TODO: write logic here
    # `IsSampled` BIT type (bool as 0 or 1) I think this is the inverse of src_tbl.is_excluded, which is a boolean in Access with one unique value: [0]
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'] == 'IsSampled')
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, "xwalk_dict['ncrn.DetectionEvent']['tbl_load']['IsSampled'] = 1", xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['note'] =  np.where(mask, "BIT type (bool as 0 or 1) seems to be the inverse of ncrn.tbl_Events.is_excluded, which is a boolean in Access with one unique value: [0]", xwalk_dict['ncrn.DetectionEvent']['xwalk']['note'])
    # -- lookup-constrained calculations
    # TODO: figure out what to do with these fields
    # `SamplingMethodID`: [lu.SamplingMethod]([ID]) # does NCRN capture an equivalent to this?
    # `Observer_ContactID`: [ncrn.Contact]([ID]) # does NCRN capture an equivalent to this?
    # `Recorder_ContactID`: [ncrn.Contact]([ID]) # does NCRN capture an equivalent to this?
    # `Observer_ExperienceLevelID`: [lu.ExperienceLevel]([ID])
    # `ProtocolNoiseLevelID`: [lu.NoiseLevel]([ID]) # does NCRN capture an equivalent to this?
    # `ProtocolWindCodeID`: [lu.WindCode]([ID]) # does NCRN capture an equivalent to this?
    # `ProtocolPrecipitationTypeID`: [lu.PrecipitationType]([ID]) # does NCRN capture an equivalent to this?
    # `TemperatureUnitCode`: [lu.TemperatureUnit]([ID]) # does NCRN capture an equivalent to this?

    # Blanks
    blank_fields = [
        'DataProcessingLevelNote'
        ,'Rowversion'
        ,'UserCode'
    ]
    # assign grouping variable `calculation` for the blank fields
    mask = (xwalk_dict['ncrn.DetectionEvent']['xwalk']['destination'].isin(blank_fields))
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['calculation'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn.DetectionEvent']['xwalk']['calculation'])
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'] =  np.where(mask, 'blank_field', xwalk_dict['ncrn.DetectionEvent']['xwalk']['source'])
    xwalk_dict['ncrn.DetectionEvent']['xwalk']['note'] =  np.where(mask, 'this field was not collected by NCRN and has no NCRN equivalent', xwalk_dict['ncrn.DetectionEvent']['xwalk']['note'])

    return xwalk_dict

def _tbl_bird_detection(xwalk_dict:dict) -> dict: ##TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO##
    """Crosswalk source.tbl_Event to destination.ncrn.BirdDetection

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """
    # xwalk_dict['ncrn.BirdDetection']['source_name'] = 'tbl_Field_Data'
    # xwalk_dict['ncrn.DetectionEvent']['xwalk']

    return xwalk_dict

def _tbl_protocol(xwalk_dict:dict) -> dict: ##TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO##
    """Crosswalk source.tbl_Protocol to destination.ncrn.Protocol

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    return xwalk_dict

def _tbl_locations(xwalk_dict:dict) -> dict: ##TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO##
    """Crosswalk source.tbl_Locations to destination.ncrn.Location

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    # do we really collect data from >4k sites?!

    return xwalk_dict

def _tlu_park_code(xwalk_dict:dict) -> dict: ##TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO##
    """Crosswalk source.tlu_Park_Code to destination.ncrn.Park

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    return xwalk_dict

def _tlu_interval(xwalk_dict:dict) -> dict: ##TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO##
    """Crosswalk source.tlu_interval to destination.lu.TimeInterval

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    return xwalk_dict

def _tlu_wind_code(xwalk_dict:dict) -> dict: ##TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO####TODO##TODO##TODO##
    """Crosswalk source.tlu_Wind_Code to destination.lu.WindCode

    Args:
        xwalk_dict (dict): dictionary of column names crosswalked between source and destination tables

    Returns:
        dict: dictionary of column names crosswalked between source and destination tables with data updated for this table
    """

    return xwalk_dict







