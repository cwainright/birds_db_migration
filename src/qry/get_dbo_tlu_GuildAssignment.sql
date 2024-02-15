SELECT dbo_tlu_Guild_Assignment.*, dbo_tbl_Guilds.Integrity_Element, dbo_tbl_Guilds.Integrity_Element, dbo_tbl_Guilds.Guild_Name, dbo_tbl_Guilds.Guild_Desc, dbo_tbl_Guilds.GuildCategory, *
FROM dbo_tlu_Guild_Assignment LEFT JOIN dbo_tbl_Guilds ON dbo_tlu_Guild_Assignment.Guild_ID = dbo_tbl_Guilds.Guild_ID;
