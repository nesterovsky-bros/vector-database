USE [Vectors]
GO
/****** Object:  UserDefinedTableType [dbo].[PointType]    Script Date: 28/06/2023 17:06:20 ******/
CREATE TYPE [dbo].[PointType] AS TABLE(
	[ID] [bigint] NOT NULL,
	[Idx] [smallint] NOT NULL,
	[Value] [real] NULL,
	PRIMARY KEY CLUSTERED 
(
	[ID] ASC,
	[Idx] ASC
)WITH (IGNORE_DUP_KEY = OFF)
)
GO
/****** Object:  UserDefinedTableType [dbo].[RangeType]    Script Date: 28/06/2023 17:06:20 ******/
CREATE TYPE [dbo].[RangeType] AS TABLE(
	[RangeID] [bigint] NOT NULL,
	[Dimension] [smallint] NULL,
	[Mid] [real] NULL,
	[LowRangeID] [bigint] NULL,
	[HighRangeID] [bigint] NULL,
	[ID] [bigint] NULL,
	PRIMARY KEY CLUSTERED 
(
	[RangeID] ASC
)WITH (IGNORE_DUP_KEY = OFF)
)
GO
/****** Object:  UserDefinedFunction [dbo].[BuildIndex]    Script Date: 28/06/2023 17:06:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

USE [Vectors]
GO
/****** Object:  UserDefinedFunction [dbo].[BuildIndex]    Script Date: 04/07/2023 12:54:24 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Builds range index for points.
create function [dbo].[BuildIndex]
(
  -- a points table to build range index.
  @points dbo.PointType readonly
)
returns @index table 
(
	RangeID bigint not null primary key,
	Dimension smallint null,
	Mid real null,
	LowRangeID bigint null,
	HighRangeID bigint null,
	ID bigint null
)
as
begin
  declare @ranges table
	(
		ID bigint,
		RangeID bigint,
		primary key(RangeID, ID)
	);

  declare @stats table
  (
    RangeID bigint not null primary key,
    Idx smallint not null,
    Mean real not null,
    [Stdev] real,
    Count bigint not null,
    ID bigint not null
  );

--raiserror(N'Level 0.', 0, 0) with nowait;

  insert into @stats(RangeID, Idx, Mean, Stdev, Count, ID)
  select top 1
    0,
    Idx,
    avg(Value),
    isnull(stdev(Value), 0) Stdev,
    count_big(*),
    avg(ID)
  from
    @points
  group by
    Idx
  order by
    Stdev desc

  declare @next bit = @@rowcount;

  if (@next != 0)
  begin
    insert @ranges(RangeID, ID)
    select
      iif(S.Stdev = 0, iif(P.ID <= S.ID, 1, 2), iif(Value < Mean, 1, 2)),
      P.ID
    from
      @points P
      join
      @stats S
      on
        P.Idx = S.Idx and
        S.Count > 1;

    set @next = @@rowcount;
    declare @level bigint = 0;
    declare @i tinyint = 0;

    while(@next != 0)
    begin
--raiserror(N'Level %i.', 0, 0, @level) with nowait;

      insert into @stats(RangeID, Idx, Mean, Stdev, Count, ID)
      select
        S.RangeID * 2 + N.I, 
        R.Idx, 
        R.Mean, 
        R.Stdev, 
        R.Count, 
        R.ID
      from
        @stats S
        join
        (select 1 union all select 2) N(I)
        on
          S.RangeID >= @level and
          S.Count > 1
        cross apply
        (
          select top 1
            P.Idx,
            avg(P.Value) Mean,
            isnull(stdev(P.Value), 0) Stdev,
            count_big(*) Count,
            avg(P.ID) ID
          from
            @ranges R
            join
            @points P
            on
              P.ID = R.ID and
              R.RangeID = S.RangeID * 2 + N.I
          group by
            Idx
          order by
            iif(@level % 2 = 1, Stdev, -Stdev) desc
        ) R;

      set @level = @level * 2 + 1;
      set @i += 1;

      with R as
      (
        select
          R.*,
          R.RangeID * 2 + 
            case
              when Value < Mean then 1
              when Value > Mean then 2
              when R.ID <= S.ID then 1
              else 2
            end NewRangeID
        from
          @ranges R
          join
          @stats S
          on
            S.RangeID = R.RangeID and
            S.RangeID >= @level and
            S.Count > 1
          join
          @points P
          on
            P.ID = R.ID and
            P.Idx = S.Idx
      )
      update R
      set 
        RangeID = NewRangeID;

      set @next = @@rowcount;
    end;
  end;

  insert into @index(RangeID, Dimension, Mid, LowRangeID, HighRangeID, ID)
  select
    RangeID,
    iif(Stdev = 0, null, Idx) Dimension,
    iif(Stdev = 0, null, Mean) Mid,
    iif(Count = 1, null, RangeID * 2 + 1) LowRangeID,
    iif(Count = 1, null, RangeID * 2 + 2) HighRangeID,
    iif(Count = 1, ID, null) ID
  from
    @stats;

  return;
end
GO
/****** Object:  Table [dbo].[TextIndex]    Script Date: 28/06/2023 17:06:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TextIndex](
	[DocID] [bigint] NOT NULL,
	[RangeID] [bigint] NOT NULL,
	[Dimension] [smallint] NULL,
	[Mid] [real] NULL,
	[LowRangeID] [bigint] NULL,
	[HighRangeID] [bigint] NULL,
	[TextID] [bigint] NULL,
 CONSTRAINT [PK_TextIndex] PRIMARY KEY CLUSTERED 
(
	[RangeID] ASC,
	[DocID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY],
 CONSTRAINT [IX_TextIndex] UNIQUE NONCLUSTERED 
(
	[DocID] ASC,
	[RangeID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  UserDefinedFunction [dbo].[Search]    Script Date: 28/06/2023 17:06:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION [dbo].[Search]
(	
	-- json array of embedding vector
	@point nvarchar(max),
	-- a search domain.
	@domain real,
	-- Optional doc id.
	@docId bigint = null
)
returns table 
as
return 
with Vector as
(
  select 
	[key] Idx, 
	value - @domain MinValue, 
	value + @domain MaxValue 
  from 
	openjson(@point)
),
Node as
(
	select
	  *
	from
	  dbo.TextIndex
	where
	  RangeID = 0 and
	  (@docId is null or DocID = @docId)
	union all
	select
	  I.*
	from
	  dbo.TextIndex I
	  inner join
	  Node N
	  on
		  N.LowRangeID is not null and
	      I.DocID = N.DocID and
		  I.RangeID = N.LowRangeID and
		  (
			  N.Dimension is null or
			  N.Mid >= (select MinValue from Vector where Idx = N.Dimension)
		  )
	union all
	select
	  I.*
	from
	  dbo.TextIndex I
	  inner join
	  Node N
	  on
		  N.HighRangeID is not null and
	      I.DocID = N.DocID and
		  I.RangeID = N.HighRangeID and
		  (
			  N.Dimension is null or
			  N.Mid <= (select MaxValue from Vector where Idx = N.Dimension)
		  )
)
select DocID, TextID from Node where TextID is not null;
GO
/****** Object:  Table [dbo].[Document]    Script Date: 28/06/2023 17:06:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Document](
	[DocID] [bigint] NOT NULL,
	[Name] [nvarchar](256) NULL,
 CONSTRAINT [PK_Documents] PRIMARY KEY CLUSTERED 
(
	[DocID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Text]    Script Date: 28/06/2023 17:06:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Text](
	[DocID] [bigint] NOT NULL,
	[TextID] [bigint] NOT NULL,
	[Text] [nvarchar](max) NULL,
	[Vector] [nvarchar](max) NULL,
 CONSTRAINT [PK_Text] PRIMARY KEY CLUSTERED 
(
	[DocID] ASC,
	[TextID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE [dbo].[Document] ADD  CONSTRAINT [DF_Document_DocID]  DEFAULT (NEXT VALUE FOR [dbo].[DocumentID]) FOR [DocID]
GO
ALTER TABLE [dbo].[Text] ADD  CONSTRAINT [DF_Text_TextID_1]  DEFAULT (NEXT VALUE FOR [dbo].[TextID]) FOR [TextID]
GO
ALTER TABLE [dbo].[Text]  WITH CHECK ADD  CONSTRAINT [FK_Text_Document] FOREIGN KEY([DocID])
REFERENCES [dbo].[Document] ([DocID])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[Text] CHECK CONSTRAINT [FK_Text_Document]
GO
ALTER TABLE [dbo].[TextIndex]  WITH CHECK ADD  CONSTRAINT [FK_TextIndex_Document] FOREIGN KEY([DocID])
REFERENCES [dbo].[Document] ([DocID])
ON UPDATE CASCADE
ON DELETE CASCADE
GO
ALTER TABLE [dbo].[TextIndex] CHECK CONSTRAINT [FK_TextIndex_Document]
GO
/****** Object:  StoredProcedure [dbo].[IndexDocument]    Script Date: 28/06/2023 17:06:20 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE procedure [dbo].[IndexDocument]
  @docID bigint
as
begin
	set nocount on;

  declare @points dbo.PointType;
  declare @index dbo.RangeType;

--raiserror(N'Start loading points.', 0, 0, @timespan) with nowait;

--set @start = current_timestamp;

  insert into @points(ID, Idx, Value)
  select
	  TextID, [key], value
  from
	  dbo.Text
	  cross apply
	  openjson(Vector)
  where
	  DocID = @docID;

--set @end = current_timestamp;
--set @timespan = datediff(ms, @start, @end);

--raiserror(N'Points loaded in %i milliseconds.', 0, 0, @timespan) with nowait;

--raiserror(N'Start building index.', 0, 0, @timespan) with nowait;

--set @start = current_timestamp;

--raiserror(N'Start building index.', 0, 0, @timespan) with nowait;

--set @start = current_timestamp;

  insert into @index
  select * from dbo.BuildIndex(@points);

--set @end = current_timestamp;
--set @timespan = datediff(ms, @start, @end);

--raiserror(N'Index built in %i milliseconds.', 0, 0, @timespan) with nowait;

	-- Update index.
  delete from dbo.TextIndex where DocID = @docID;

  insert into dbo.TextIndex
  (
    DocID, 
    RangeID, 
    Dimension, 
    Mid, 
    LowRangeID, 
    HighRangeID, 
    TextID
  )
  select
    @docID,
    RangeID,
    Dimension,
    Mid,
    LowRangeID,
    HighRangeID,
    ID
  from
    @index
end
GO
