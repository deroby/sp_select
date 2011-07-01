USE master
GO
if OBJECT_ID('sp_select') is not null
  begin
    print 'Dropping procedure sp_select...'
    drop procedure sp_select
  end
 GO
/*
Created by: Filip De Vos
http://foxtricks.blogspot.com

Based on the post by Jonathan Kehayias
http://sqlblog.com/blogs/jonathan_kehayias/archive/2009/09/29/what-session-created-that-object-in-tempdb.aspx

Revised by: Roby Van Hoye
* slightly "improved" handling of parameters & location of source-table
* bit of extra output on where the data comes from (in case of tempdb)
* added @target_table parameter as to allow data be returned via a new (local) table instead of returing it directly to the client

Usage:
create table #myTempTable (id int, value varchar(100))
insert into #myTempTable values (10, 'hihi'), (11, 'haha')

Keep the connection open where the temptable is created and run the following query from any connection you want.

exec sp_select 'tempdb..#myTempTable'

Also "normal" tables can be inspected.

exec sp_select 'msdb.dbo.MSdbms'
*/
CREATE PROCEDURE dbo.sp_select(@table_name sysname, 
                               @spid int = NULL, 
                               @max_pages int = 1000,
                               @target_table sysname = NULL)
AS

    SET NOCOUNT ON
  
    DECLARE @object_id int
          , @table sysname
          , @db_name sysname
          , @db_id int
          , @file_name varchar(MAX)
          , @status int
          , @rowcount int
          , @spid_found int
          , @host_name sysname
          , @login_name sysname
        
    -- add database in case this is a temp-table
    SELECT @table_name = (CASE WHEN @table_name LIKE '#%' THEN 'tempdb..' ELSE '' END) + @table_name
    SELECT @object_id = OBJECT_ID(@table_name),
           @table     = PARSENAME(@table_name, 1),
           @db_id     = ISNULL(DB_ID(PARSENAME(@table_name, 3)), DB_ID())
  
    IF PARSENAME(@table_name, 3) = 'tempdb'
        BEGIN
            -- determine the default trace file, get the actual table name
            SELECT @file_name = SUBSTRING(path, 0, LEN(path) - CHARINDEX('\', REVERSE(path)) + 1) + '\Log.trc'                   
            FROM sys.traces
            WHERE is_default = 1;

            CREATE TABLE #objects (object_id int primary key, db_id int, spid int, host_name sysname, login_name sysname)
        
            -- Match the spid with db_id and object_id via the default trace file
            INSERT INTO #objects (object_id, db_id, spid, host_name, login_name)
            SELECT DISTINCT o.object_id, dr.dbid, gt.spid, dr.hostname, dr.loginame
              FROM sys.fn_trace_gettable(@file_name, DEFAULT) AS gt
              JOIN tempdb.sys.objects AS o
                ON gt.ObjectID = o.object_id
              LEFT OUTER JOIN (SELECT DISTINCT spid, dbid, loginame, hostname
                                 FROM master..sysprocesses
                                WHERE spid = @spid 
                                   OR @spid IS NULL) dr
                ON dr.spid = gt.spid
             WHERE gt.DatabaseID = 2
               AND gt.EventClass = 46 -- (Object:Created Event from sys.trace_events)
               AND o.create_date >= DATEADD(ms, -100, gt.StartTime)
               AND o.create_date <= DATEADD(ms, 100, gt.StartTime)
               AND o.name like @table + Replicate('[_]', 5) + '%'
           
            SET @rowcount = @@ROWCOUNT
            
            IF @rowcount = 0
              BEGIN
                RAISERROR('Unable to figure out which temp table with name [%s] to select from.', 16,1, @table_name)
                RETURN(-1)
              END
            
            IF @rowcount > 1            
              BEGIN
                -- take the first one linked to the current database
                IF NOT EXISTS ( SELECT * 
                                  FROM #objects
                                 WHERE db_id = DB_ID())
                    BEGIN
                        RaisError('Unable to figure out which temptable to use. Please specifiy a spid to filter on which table to select from.', 16, 1)
                        SELECT related_db =  db_name(db_id), spid, login_name, host_name              
                          FROM #objects
                         ORDER BY 1
                        Return(-1)
                    END
                    
                DELETE #objects
                 WHERE db_id <> DB_ID()
                 
                SELECT @rowcount = @rowcount - @@ROWCOUNT
              END
            
            IF @rowcount > 1
              BEGIN
                RAISERROR('There are %d temp tables with the name [%s] active on the spid %d. There must be something wrong in this procedure. Showing the first one', 16, 1, @rowcount, @table_name, @spid)
                -- We'll continue with the first match.
              END

            SELECT TOP 1 @object_id  = object_id, 
                         @spid_found = spid,
                         @login_name = RTrim(login_name),
                         @host_name  = RTrim(host_name)
              FROM #objects
             ORDER BY object_id
        END
  
    IF @object_id IS NULL
        BEGIN
            RAISERROR('The table [%s] does not exist', 16, 1, @table_name)
            RETURN (-1)
        END
  
    IF @spid IS NULL AND @spid_found <> @@SPID
        BEGIN
            PRINT '(Selecting from ' + @table + ' as found on spid ' + Convert(varchar, @spid_found) + ', connection by [' + @login_name + '] from [' + @host_name + '], object_id = ' + Convert(varchar, @object_id) + ')'
        END
        
    EXEC @status = master..sp_selectpages @object_id = @object_id, 
                                          @db_id     = @db_id, 
                                          @max_pages = @max_pages,
                                          @target_table = @target_table
  
    RETURN (@status)
    
GO
if OBJECT_ID('sp_select') is null
  PRINT 'Failed to create procedure sp_select...'
ELSE
  PRINT 'Correctly created procedure sp_select...'
GO

