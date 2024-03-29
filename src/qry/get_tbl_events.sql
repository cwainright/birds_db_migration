SELECT 
  tbl_events.event_id, 
  tbl_events.location_id, 
  tbl_events.event_group_id, 
  tbl_events.location_group_id, 
  tbl_events.protocol_id, 
  tbl_events.Date, 
  tbl_events.start_time, 
  tbl_events.end_time, 
  tbl_events.visit, 
  tbl_events.survey_type, 
  tbl_events.eventflag, 
  tbl_events.flagdescription, 
  tbl_events.dbsource, 
  tbl_events.is_excluded, 
  tbl_events.entered_by, 
  tbl_events.entered_date, 
  tbl_events.updated_by, 
  tbl_events.updated_date, 
  tbl_events.verified, 
  tbl_events.verified_by, 
  tbl_events.verified_date, 
  tbl_events.certified, 
  tbl_events.certified_by, 
  tbl_events.certified_date, 
  tbl_events.dataprocessinglevelid, 
  tbl_events.dataprocessinglevel_userid, 
  tbl_events.dataprocessingleveldate, 
  tbl_event_details.event_detail_id, 
  tbl_event_details.sky_condition, 
  tbl_event_details.wind_speed, 
  tbl_event_details.disturbance_level, 
  tbl_event_details.temperature, 
  tbl_event_details.humidity, 
  tbl_event_details.call_back_survey, 
  tbl_event_details.event_notes, 
  tbl_event_details.notsampledreasonflag, 
  flags.flaggroup, 
  flags.label, 
  tbl_protocol.protocol_name 
FROM 
  (
    tbl_events 
    LEFT JOIN (
      tbl_event_details 
      LEFT JOIN flags ON tbl_event_details.notsampledreasonflag = flags.id
    ) ON tbl_events.event_id = tbl_event_details.event_id
  ) 
  INNER JOIN tbl_protocol ON tbl_events.protocol_id = tbl_protocol.protocol_id;
