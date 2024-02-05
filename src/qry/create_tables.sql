-- ################################################################ --
-- ############# IF CREATING NEW DATABASE, START HERE ############# --
-- ################################################################ --

-- STEP 1) Suggest manually creating a new database (https://learn.microsoft.com/en-us/sql/relational-databases/databases/create-a-database?view=sql-server-ver16)

-- STEP 2) Run large block of CREATE SQL below to create the Landbirds tables and establish contraints, et al.

CREATE TABLE [User] (
  [ID] int,
  [UserCode] varchar(50),
  [Issuer] varchar(50),
  [DateCreated] datetime2,
  [LastAccessed] datetime2,
  [AccessCount] int,
  PRIMARY KEY ([ID])
);

CREATE INDEX [AK] ON  [User] ([UserCode], [Issuer]);

CREATE TABLE [Role] (
  [ID] int,
  [Label] varchar(50),
  PRIMARY KEY ([ID])
);

CREATE INDEX [AK] ON  [Role] ([Label]);

CREATE TABLE [UserRole] (
  [ID] int,
  [UserID] int,
  [RoleID] int,
  PRIMARY KEY ([ID]),
  CONSTRAINT [FK_UserRole.UserID]
    FOREIGN KEY ([UserID])
      REFERENCES [User]([ID]),
  CONSTRAINT [FK_UserRole.RoleID]
    FOREIGN KEY ([RoleID])
      REFERENCES [Role]([ID])
);

CREATE INDEX [AK] ON  [UserRole] ([UserID], [RoleID]);

CREATE TABLE [lu.ExperienceLevel] (
  [ID] int,
  [Code] varchar(5),
  [Label] varchar(25),
  [Description] varchar(200),
  [SortOrder] int,
  [Rowversion] timestamp,
  PRIMARY KEY ([ID])
);

CREATE INDEX [AK] ON  [lu.ExperienceLevel] ([Code]);

CREATE TABLE [lu.GeodeticDatum] (
  [ID] int,
  [Code] varchar(5),
  [Label] varchar(35),
  [Summary] varchar(100),
  PRIMARY KEY ([ID])
);

CREATE TABLE [lu.Habitat] (
  [ID] int,
  [Code] int,
  [Label] varchar(40),
  [Description] varchar(500),
  [Group] varchar(500),
  [Rowversion] timestamp,
  PRIMARY KEY ([ID])
);

CREATE INDEX [AK] ON  [lu.Habitat] ([Code]);

CREATE TABLE [ncrn.Location] (
  [ID] int,
  [SiteID] int,
  [HabitatID] int,
  [EnteredBy] varchar(100),
  [Label] varchar(255),
  [Notes] varchar(1000),
  [X_Coord_DD_NAD83] float,
  [Y_Coord_DD_NAD83] float,
  [GeodeticDatumID] int,
  [EnteredDate] datetime,
  [IsActive] bit,
  [IsSensitive] bit,
  [Rowversion] timestamp,
  [Code] varchar(100),
  [OldCode] varchar(100),
  [LegacyCode] varchar(100),
  PRIMARY KEY ([ID]),
  CONSTRAINT [FK_ncrn.Location.GeodeticDatumID]
    FOREIGN KEY ([GeodeticDatumID])
      REFERENCES [lu.GeodeticDatum]([ID]),
  CONSTRAINT [FK_ncrn.Location.HabitatID]
    FOREIGN KEY ([HabitatID])
      REFERENCES [lu.Habitat]([ID])
);

CREATE TABLE [ncrn.Contact] (
  [ID] int,
  [IsActive] bit,
  [LastName] varchar(25),
  [FirstName] varchar(25),
  [Organization] varchar(50),
  [Notes] varchar(100),
  [ExperienceLevelID] int,
  [NetworkCode] varchar(4),
  [Rowversion] timestamp,
  PRIMARY KEY ([ID]),
  CONSTRAINT [FK_ncrn.Contact.ExperienceLevelID]
    FOREIGN KEY ([ExperienceLevelID])
      REFERENCES [lu.ExperienceLevel]([ID])
);

CREATE INDEX [AK] ON  [ncrn.Contact] ([LastName], [FirstName]);

CREATE TABLE [lu.SamplingMethod] (
  [ID] int,
  [Code] varchar(5),
  [Label] varchar(25),
  [Summary] varchar(100),
  PRIMARY KEY ([ID])
);

CREATE TABLE [lu.TemperatureUnit] (
  [Code] char(1),
  [Label] varchar(20),
  [Description] varchar(200),
  [ValidMinimumValue] float,
  [ValidMaximumValue] float,
  [Rowversion] timestamp,
  PRIMARY KEY ([Code])
);

CREATE TABLE [ncrn.Protocol] (
  [ID] int,
  [Title] varchar(100),
  [Version] varchar(10),
  [EffectiveBeginDate] date,
  [EffectiveEndDate] date,
  [IsActive] bit,
  [IsDefault] bit,
  [ObserverExperienceRequired] bit,
  [Duration] int,
  [Rowversion] timestamp,
  [Comments] varchar(1000),
  PRIMARY KEY ([ID])
);

CREATE INDEX [AK] ON  [ncrn.Protocol] ([Version]);

CREATE TABLE [lu.DataProcessingLevel] (
  [ID] int,
  [Code] varchar(5),
  [Label] varchar(25),
  [Summary] varchar(1000),
  [IsActive] bit,
  [ProcessOrder] tinyint,
  PRIMARY KEY ([ID])
);

CREATE TABLE [ncrn.DetectionEvent] (
  [ID] int,
  [LocationID] int,
  [ProtocolID] int,
  [SamplingMethodID] int,
  [EnteredBy] varchar(100),
  [Observer_ContactID] int,
  [Recorder_ContactID] int,
  [Observer_ExperienceLevelID] int,
  [ProtocolNoiseLevelID] int,
  [ProtocolWindCodeID] int,
  [ProtocolPrecipitationTypeID] int,
  [AirTemperatureRecorded] bit,
  [AirTemperature] float,
  [TemperatureUnitCode] char(1),
  [StartDateTime] datetime,
  [Notes] varchar(1000),
  [ExcludeEvent] bit,
  [ExcludeNote] varchar(4000),
  [IsSampled] bit,
  [EnteredDateTime] datetime,
  [DataProcessingLevelID] int,
  [DataProcessingLevelDate] datetime,
  [DataProcessingLevelNote] varchar(4000),
  [Rowversion] timestamp,
  [UserCode] varchar(50),
  [RelativeHumidity] decimal,
  PRIMARY KEY ([ID]),
  CONSTRAINT [FK_ncrn.DetectionEvent.Observer_ExperienceLevelID]
    FOREIGN KEY ([Observer_ExperienceLevelID])
      REFERENCES [lu.ExperienceLevel]([ID]),
  CONSTRAINT [FK_ncrn.DetectionEvent.LocationID]
    FOREIGN KEY ([LocationID])
      REFERENCES [ncrn.Location]([ID]),
  CONSTRAINT [FK_ncrn.DetectionEvent.Observer_ContactID]
    FOREIGN KEY ([Observer_ContactID])
      REFERENCES [ncrn.Contact]([ID]),
  CONSTRAINT [FK_ncrn.DetectionEvent.SamplingMethodID]
    FOREIGN KEY ([SamplingMethodID])
      REFERENCES [lu.SamplingMethod]([ID]),
  CONSTRAINT [FK_ncrn.DetectionEvent.TemperatureUnitCode]
    FOREIGN KEY ([TemperatureUnitCode])
      REFERENCES [lu.TemperatureUnit]([Code]),
  CONSTRAINT [FK_ncrn.DetectionEvent.ProtocolID]
    FOREIGN KEY ([ProtocolID])
      REFERENCES [ncrn.Protocol]([ID]),
  CONSTRAINT [FK_ncrn.DetectionEvent.Recorder_ContactID]
    FOREIGN KEY ([Recorder_ContactID])
      REFERENCES [ncrn.Contact]([ID]),
  CONSTRAINT [FK_ncrn.DetectionEvent.DataProcessingLevelID]
    FOREIGN KEY ([DataProcessingLevelID])
      REFERENCES [lu.DataProcessingLevel]([ID])
);

CREATE INDEX [AK] ON  [ncrn.DetectionEvent] ([LocationID], [ProtocolID], [StartDateTime]);

CREATE TABLE [ncrn.AuditLog] (
  [ID] int,
  [username] varchar(255),
  [LogDate] datetime,
  [Description] varchar(2000),
  [DetectionEventID] int,
  PRIMARY KEY ([ID])
);

CREATE TABLE [lu.NoiseLevel] (
  [ID] int,
  [Code] varchar(5),
  [Label] varchar(25),
  [Description] varchar(100),
  [SortOrder] int,
  [Rowversion] timestamp,
  PRIMARY KEY ([ID])
);

CREATE INDEX [AK] ON  [lu.NoiseLevel] ([Code]);

CREATE TABLE [lu.DistanceClass] (
  [ID] int,
  [Code] varchar(5),
  [Label] varchar(25),
  [Description] varchar(100),
  [SortOrder] int,
  [Rowversion] timestamp,
  PRIMARY KEY ([ID])
);

CREATE INDEX [AK] ON  [lu.DistanceClass] ([Code]);

CREATE TABLE [lu.PrecipitationType] (
  [ID] int,
  [Code] varchar(2),
  [Label] varchar(30),
  [Description] varchar(100),
  [SortOrder] int,
  [Rowversion] timestamp,
  PRIMARY KEY ([ID])
);

CREATE INDEX [AK] ON  [lu.PrecipitationType] ([Code]);

CREATE TABLE [Migration] (
  [Protocol] varchar,
  [Park] varchar,
  [Location] varchar,
  [Start Date] varchar,
  [Start Time] varchar,
  [Observer] varchar,
  [Recorder] varchar,
  [Sky] varchar,
  [Wind] varchar,
  [Background Noise] varchar,
  [AirTemp] varchar,
  [AirTempUnits] varchar,
  [DPL] varchar,
  [TimeInterval] varchar,
  [SpeciesCode] varchar,
  [SpeciesName] varchar,
  [NumIndividuals] varchar,
  [DetectionType] varchar,
  [DistanceClass] varchar,
  [EventNotes] varchar,
  [DetectionNotes] varchar,
  [ExcludeEvent] varchar,
  [ExcludeNote] varchar,
  [ExcludeDetection] varchar,
  [ExcludeDetectionNote] varchar
);

CREATE TABLE [ncrn.Park] (
  [ID] int,
  [NetworkCode] varchar(4),
  [ParkCode] varchar(15),
  [Label] varchar(100),
  [IsActive] bit,
  [Rowversion] timestamp,
  PRIMARY KEY ([ID])
);

CREATE TABLE [lu.ProtectedStatus] (
  [ID] int,
  [Code] varchar(5),
  [Label] varchar(25),
  [Summary] varchar(200),
  PRIMARY KEY ([ID])
);

CREATE TABLE [ncrn.BirdSpeciesPark] (
  [ID] int,
  [BirdSpeciesID] int,
  [ParkID] int,
  [ProtectedStatusID] int,
  [Comment] varchar(1000),
  PRIMARY KEY ([ID]),
  CONSTRAINT [FK_ncrn.BirdSpeciesPark.ParkID]
    FOREIGN KEY ([ParkID])
      REFERENCES [ncrn.Park]([ID]),
  CONSTRAINT [FK_ncrn.BirdSpeciesPark.ProtectedStatusID]
    FOREIGN KEY ([ProtectedStatusID])
      REFERENCES [lu.ProtectedStatus]([ID])
);

CREATE INDEX [AK] ON  [ncrn.BirdSpeciesPark] ([BirdSpeciesID], [ParkID]);

CREATE TABLE [lu.Sex] (
  [ID] int,
  [Code] char(1),
  [Label] varchar(20),
  [Description] varchar(100),
  [Rowversion] timestamp,
  PRIMARY KEY ([ID])
);

CREATE INDEX [AK] ON  [lu.Sex] ([Code]);

CREATE TABLE [lu.DetectionType] (
  [ID] int,
  [Code] varchar(5),
  [Label] varchar(25),
  [Description] varchar(100),
  [SortOrder] int,
  [Rowversion] timestamp,
  PRIMARY KEY ([ID])
);

CREATE INDEX [AK] ON  [lu.DetectionType] ([Code]);

CREATE TABLE [ncrn.ProtocolDetectionType] (
  [ID] int,
  [ProtocolID] int,
  [DetectionTypeID] int,
  PRIMARY KEY ([ID]),
  CONSTRAINT [FK_ncrn.ProtocolDetectionType.ProtocolID]
    FOREIGN KEY ([ProtocolID])
      REFERENCES [ncrn.Protocol]([ID]),
  CONSTRAINT [FK_ncrn.ProtocolDetectionType.DetectionTypeID]
    FOREIGN KEY ([DetectionTypeID])
      REFERENCES [lu.DetectionType]([ID])
);

CREATE TABLE [ncrn.BirdDetection] (
  [ID] int,
  [DetectionEventID] int,
  [BirdSpeciesParkID] int,
  [ProtocolDistanceClassID] int,
  [ProtocolDetectionTypeID] int,
  [SexID] int,
  [ProtocolTimeIntervalID] int,
  [NumberIndividuals] int,
  [ExcludeDetection] bit,
  [DetectionNotes] varchar(255),
  [ImportNotes] varchar(255),
  [Rowversion] timestamp,
  [UserCode] varchar(50),
  [ExcludeReason] varchar(255),
  PRIMARY KEY ([ID]),
  CONSTRAINT [FK_ncrn.BirdDetection.BirdSpeciesParkID]
    FOREIGN KEY ([BirdSpeciesParkID])
      REFERENCES [ncrn.BirdSpeciesPark]([ID]),
  CONSTRAINT [FK_ncrn.BirdDetection.SexID]
    FOREIGN KEY ([SexID])
      REFERENCES [lu.Sex]([ID]),
  CONSTRAINT [FK_ncrn.BirdDetection.ProtocolDetectionTypeID]
    FOREIGN KEY ([ProtocolDetectionTypeID])
      REFERENCES [ncrn.ProtocolDetectionType]([ID]),
  CONSTRAINT [FK_ncrn.BirdDetection.DetectionEventID]
    FOREIGN KEY ([DetectionEventID])
      REFERENCES [ncrn.DetectionEvent]([ID])
);

CREATE TABLE [ncrn.BirdGroups] (
  [ID] int,
  [GroupName] varchar(50),
  [IsGuild] bit,
  [IsActive] bit,
  [Comment] varchar,
  [Rowversion] timestamp,
  PRIMARY KEY ([ID])
);

CREATE INDEX [AK] ON  [ncrn.BirdGroups] ([GroupName]);

CREATE TABLE [ncrn.BirdSpeciesGroups] (
  [ID] int,
  [BirdSpeciesID] int,
  [BirdGroupID] int,
  [Rowversion] timestamp,
  PRIMARY KEY ([ID]),
  CONSTRAINT [FK_ncrn.BirdSpeciesGroups.BirdGroupID]
    FOREIGN KEY ([BirdGroupID])
      REFERENCES [ncrn.BirdGroups]([ID])
);

CREATE INDEX [AK] ON  [ncrn.BirdSpeciesGroups] ([BirdSpeciesID], [BirdGroupID]);

CREATE TABLE [ParkUser] (
  [ID] int,
  [ParkID] int,
  [UserID] int,
  PRIMARY KEY ([ID]),
  CONSTRAINT [FK_ParkUser.ParkID]
    FOREIGN KEY ([ParkID])
      REFERENCES [ncrn.Park]([ID]),
  CONSTRAINT [FK_ParkUser.UserID]
    FOREIGN KEY ([UserID])
      REFERENCES [User]([ID])
);

CREATE INDEX [AK] ON  [ParkUser] ([ParkID], [UserID]);

CREATE TABLE [lu.WindCode] (
  [ID] int,
  [Code] varchar(2),
  [Label] varchar(25),
  [Description] varchar(100),
  [WindSpeedLabel_mph] varchar(20),
  [SortOrder] int,
  [Rowversion] timestamp,
  PRIMARY KEY ([ID])
);

CREATE INDEX [AK] ON  [lu.WindCode] ([Code]);

CREATE TABLE [ncrn.AuditLogDetail] (
  [ID] int,
  [AuditLogID] int,
  [Action] varchar(10),
  [ProtocolID] int,
  [EntityAffected] varchar(200),
  [OldValue] varchar,
  [NewValue] varchar,
  [Description] varchar(1000),
  [FieldName] varchar(100),
  [RecordLocatorPath] varchar(200),
  PRIMARY KEY ([ID]),
  CONSTRAINT [FK_ncrn.AuditLogDetail.AuditLogID]
    FOREIGN KEY ([AuditLogID])
      REFERENCES [ncrn.AuditLog]([ID])
);

CREATE TABLE [lu.TimeInterval] (
  [ID] int,
  [Code] varchar(5),
  [Label] varchar(50),
  [Summary] varchar(150),
  [SortOrder] int,
  PRIMARY KEY ([ID])
);

CREATE INDEX [AK] ON  [lu.TimeInterval] ([Code], [Label]);

USE [NCRN_Landbirds]
GO

-- update commented-out because table is empty requires exist
-- UPDATE [NCRN_Landbirds].[dbo].[lu.ExperienceLevel] SET Code = 'EXP', Label = 'Expert', Description = 'An expert', SortOrder = 1 WHERE ID = 1;

INSERT INTO [NCRN_Landbirds].[dbo].[lu.ExperienceLevel]
           ([ID] -- ID required, and isn't autogenerating
	   ,[Code]
           ,[Label]
           ,[Description]
           ,[SortOrder])
     VALUES
			(1,'NOV','Novice','A beginner',1)
	
INSERT INTO [NCRN_Landbirds].[dbo].[lu.ExperienceLevel]
           ([ID] -- ID required, and isn't autogenerating
	   ,[Code]
           ,[Label]
           ,[Description]
           ,[SortOrder])
     VALUES
			(2,'EXP','Expert','An expert',2)

GO