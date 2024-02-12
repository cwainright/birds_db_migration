SELECT 
  tbl_Events.Event_ID, 
  xref_Event_Contacts.Contact_ID, 
  xref_Event_Contacts.Contact_Role,
  tlu_Contacts.Position_Title
FROM 
  tlu_Contacts 
  INNER JOIN (
    tbl_Events 
    INNER JOIN xref_Event_Contacts ON tbl_Events.Event_ID = xref_Event_Contacts.Event_ID
  ) ON tlu_Contacts.Contact_ID = xref_Event_Contacts.Contact_ID;
