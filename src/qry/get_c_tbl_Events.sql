SELECT 
    tbl_Events.Event_ID as event_id
    ,tbl_Events.Location_ID as location_id
    ,tbl_Events.Event_Group_ID as event_group_id
    ,tbl_Events.Location_Group_ID as location_group_id
    ,tbl_Events.Protocol_ID as protocol_id
    ,tbl_Events.Start_Time as start_time
    ,tbl_Events.End_Time as end_time
    ,tbl_Events.Visit as visit
    ,tbl_Events.Survey_Type as survey_type
    ,tbl_Events.EventFlag as eventflag
    ,tbl_Events.FlagDescription as flagdescription
    ,tbl_Events.DbSource as dbSource
    ,tbl_Events.Is_Excluded as is_excluded
    ,tbl_Events.Entered_By as entered_by
    ,tbl_Events.Entered_Date as entered_date
    ,tbl_Events.Updated_By as updated_by
    ,tbl_Events.Updated_Date as updated_date
    ,tbl_Events.Verified as verified
    ,tbl_Events.Verified_By as verified_by
    ,tbl_Events.Verified_Date as verified_date
    ,tbl_Events.Certified as certified
    ,tbl_Events.Certified_By as certified_by
    ,tbl_Events.Certified_Date as certified_date
    ,tbl_Events.DataProcessingLevelID as dataprocessinglevelid
    ,tbl_Events.DataProcessingLevel_UserID as dataprocessinglevel_userid
    ,tbl_Events.DataProcessingLevelDate as dataprocessingleveldate
FROM tbl_Events;