USE master
GO

if OBJECT_ID('sp_selectpages') is not null
  BEGIN
    PRINT 'dropping procedure sp_selectpages'
    DROP PROCEDURE sp_selectpages
  END
GO
/*
Original Version: John Huang's

Revised by: Fabiano Neves Amorim
E-Mail: fabiano_amorim@bol.com.br
http://fabianosqlserver.spaces.live.com/
http://www.simple-talk.com/author/fabiano-amorim/

Revised by: Filip De Vos
http://foxtricks.blogspot.com

Revised by: Roby Van Hoye
* convert varchar fields to native formats again
* added @target_table parameter to allow the output to be stored in newly created table instead of being returned to the client directly
* WIP : added support for (n)varchar(max) fields (varbinary in beta!). SO FAR ONLY IN-ROW DATA IS RETURNED.

Usage:

DECLARE @i Int
declare @d int
set @d = db_id('acc_fsdb35_it1')
SET @i = object_id('acc_fsdb35_it1..t_entity')

EXEC dbo.sp_selectpages @object_id = @i, @db_id = @d, @max_pages = 10000
*/
CREATE PROCEDURE sp_selectpages(@object_id int, 
                                @db_id int = NULL, 
                                @max_pages int = 100,
                                @target_table sysname = NULL)
AS
BEGIN
    SET NOCOUNT ON

    DECLARE @SQL nvarchar(MAX),
            @c_page_fid smallint,
            @c_page_pid int,
            @rowcount int,           
            @column_list nvarchar(max),
            @column_list2 nvarchar(max),
            @page varchar(41),
            @create_table nvarchar(max),
            @counter int,
            @c_ParentObject varchar(500),
            @c_next_ParentObject varchar(500),
            @c_column_type sysname,
            @c_column_name sysname,
            @c_Value nvarchar(4000),
            @output_table nvarchar(500)

    SELECT @db_id = ISNULL(@db_id, DB_ID()),
           @output_table = QuoteName(ISNULL(ParseName(@target_table, 3), DB_Name()))
                         + '.' + QuoteName(ISNULL(ParseName(@target_table, 2), 'dbo'))
                         + '.' + QuoteName(ISNULL(ParseName(@target_table, 1), '#output_table'))

    IF Object_id(@output_table) IS NOT NULL
        BEGIN
            RaisError('The target table [%s] already exists, please drop it manualy or select a different name.', 16, 1, @output_table)
            Return(-1)
        END
            
    IF Object_Name(@object_id, @db_id) IS NULL
        BEGIN
            RAISERROR ('The object with id [%d] does not exist in the database with id [%d]', 16, 1, @object_id, @db_id);
            RETURN(-1)
        END
        
    CREATE TABLE #DBCC_IND(row_id int IDENTITY(1,1) PRIMARY KEY,
                           PageFID smallint,
                           PagePID int,
                           IAMFID int,
                           IAMPID int,
                           ObjectID int,
                           IndexID int,
                           PartitionNumber bigint,
                           PartitionID bigint,
                           Iam_Chain_Type varchar(80),
                           PageType int,
                           IndexLevel int,
                           NexPageFID int,
                           NextPagePID int,
                           PrevPageFID int,
                           PrevPagePID int)

    CREATE TABLE #DBCC_Page(row_id int IDENTITY(1, 1) PRIMARY KEY NONCLUSTERED,
                            Page varchar(100),
                            ParentObject varchar(500),
                            Object varchar(500),
                            Field sysname,
                            Value nvarchar(max))
                            
    CREATE CLUSTERED INDEX idx0_DBCC_Page ON #DBCC_Page (Object)
    
    CREATE TABLE #inline_blob (row_id int NOT NULL,
                               PageSlot varchar(520) NOT NULL PRIMARY KEY (PageSlot, row_id),
                               column_name sysname NOT NULL,
                               column_type sysname NOT NULL,
                               sub_Value varchar(50))
                               
    CREATE TABLE #out_of_row_blob (row_id int NOT NULL IDENTITY(1, 1) PRIMARY KEY,
                                   PageSlot varchar(520) NOT NULL,
                                   column_name sysname NOT NULL, 
                                   column_type sysname NOT NULL, 
                                   link_id int NOT NULL, 
                                   file_id int NOT NULL, 
                                   page_id int NOT NULL, 
                                   slot_id int NOT NULL,
                                   size    int NOT NULL)

    CREATE TABLE #raw_result(PageSlot varchar(520),                          
                         Field sysname
                            PRIMARY KEY (PageSlot, Field),
                         Value nvarchar(max))

    CREATE TABLE #columns(column_id int PRIMARY KEY CLUSTERED,
                          column_name sysname UNIQUE,
                          column_type sysname,
                          column_length int,
                          column_precision int,
                          column_scale int,
                          column_definition nvarchar(200))
                          
    CREATE INDEX idx0_columns ON #columns (column_type, column_length) INCLUDE (column_name)
                          
    -- get the columns definition for this table
    SELECT @rowcount = 0,
           @SQL = N' INSERT INTO #columns (column_id, column_name, column_type, column_length, column_precision, column_scale)'
                + N' SELECT col_id        = c.column_id,'
                + N'        col_name      = c.name,'
                + N'        col_type      = t.name,'
                + N'        col_length    = c.max_length,'
                + N'        col_precision = c.precision,'
                + N'        col_scale     = c.scale'
                + N'   FROM ' + QuoteName(DB_Name(@db_id)) + N'.sys.columns c'
                + N'   JOIN ' + QuoteName(DB_Name(@db_id)) + N'.sys.types t'
                + N'     ON t.system_type_id = c.system_type_id'
                + N'    AND t.system_type_id = t.user_type_id'
                + N'  WHERE object_id = @object_id'
                + N' SELECT @rowcount = @@ROWCOUNT' -- select * from sys.columns
    
    EXEC sp_executesql @stmt      = @SQL, 
                       @params    = N'@object_id int, @rowcount int OUTPUT', 
                       @object_id = @object_id, 
                       @rowcount  = @rowcount OUTPUT
    
    IF @rowcount = 0
        BEGIN
            RAISERROR('No columns to return for table with id [%d]', 16, 1, @object_id)
            RETURN(-1)
        END
        
    UPDATE #columns
       SET column_definition = column_type + (CASE column_type WHEN N'bigint'           THEN N''
                                                               WHEN N'binary'           THEN N'(' + CONVERT(NVARCHAR(MAX), column_length) + N')'
                                                               WHEN N'bit'              THEN N''
                                                               WHEN N'char'             THEN N'(' + CONVERT(NVARCHAR(MAX), column_length) + N')'
                                                               WHEN N'datetime'         THEN N''
                                                               WHEN N'decimal'          THEN N'(' + CONVERT(NVARCHAR(MAX), column_precision) + N',' + CONVERT(NVARCHAR(MAX), column_scale) + N')'
                                                               WHEN N'float'            THEN N''
                                                               WHEN N'image'            THEN N''
                                                               WHEN N'int'              THEN N''
                                                               WHEN N'money'            THEN N''
                                                               WHEN N'nchar'            THEN N'(' + CONVERT(NVARCHAR(MAX), column_length / 2) + N')'
                                                               WHEN N'ntext'            THEN N''
                                                               WHEN N'numeric'          THEN N'(' + CONVERT(NVARCHAR(MAX), column_precision) + N',' + CONVERT(NVARCHAR(MAX), column_scale) + N')'
                                                               WHEN N'nvarchar'         THEN N'(' + (CASE column_length WHEN -1 THEN N'max' ELSE CONVERT(NVARCHAR(MAX), column_length / 2) END) + N')'
                                                               WHEN N'real'             THEN N''
                                                               WHEN N'smalldatetime'    THEN N''
                                                               WHEN N'smallint'         THEN N''
                                                               WHEN N'smallmoney'       THEN N''
                                                               WHEN N'sql_variant'      THEN N''
                                                               WHEN N'text'             THEN N''
                                                               WHEN N'timestamp'        THEN N''
                                                               WHEN N'tinyint'          THEN N''
                                                               WHEN N'uniqueidentifier' THEN N''
                                                               WHEN N'varbinary'        THEN N'(' + (CASE column_length WHEN -1 THEN N'max' ELSE CONVERT(NVARCHAR(MAX), column_length) END) + N')'
                                                               WHEN N'varchar'          THEN N'(' + (CASE column_length WHEN -1 THEN N'max' ELSE CONVERT(NVARCHAR(MAX), column_length) END) + N')'
                                                               WHEN N'xml'              THEN N''
                                                                                        ELSE N'<Not Supported>' END)
        
      
    SELECT @column_list  = NULL,
           @column_list2 = NULL,
           @create_table = NULL
           
    SELECT @column_list  = (CASE WHEN @column_list  IS NULL THEN '' ELSE @column_list  + ', ' END) + column_name,
           @column_list2 = (CASE WHEN @column_list2 IS NULL THEN '' ELSE @column_list2 + ', ' END) + 'Convert(' + column_definition + ', ' + column_name + ')',
           @create_table = (CASE WHEN @create_table IS NULL THEN 'CREATE TABLE ' + @output_table + ' (' ELSE @create_table + ', ' END) + column_name + ' ' + column_definition
      FROM #columns
     ORDER BY column_id

    -- get list of pages used by this table
    SELECT @create_table = @create_table + ')',
           @SQL = N'DBCC IND(' + QuoteName(DB_Name(@db_id)) + N', ' + CONVERT(varchar(10), @object_id) + N', 1) WITH NO_INFOMSGS'

    UPDATE STATISTICS #columns

    DBCC TRACEON(3604) WITH NO_INFOMSGS

    INSERT INTO #DBCC_IND
    EXEC (@SQL)
    
    UPDATE STATISTICS #DBCC_IND
    
    DECLARE page_loop CURSOR LOCAL FAST_FORWARD FOR
    SELECT TOP (@max_pages) 
           sPageFID = Convert(nvarchar(20), PageFID), 
           sPagePID = Convert(nvarchar(20), PagePID),
           sPage    = Convert(nvarchar(20), PageFID) + Convert(nvarchar(20), PagePID), 
           sSQL     = N'DBCC PAGE (' + QuoteName(DB_Name(@db_id)) + N',' + Convert(nvarchar(20), PageFID) + N',' + Convert(nvarchar(20), PagePID) + N', 3) WITH TABLERESULTS, NO_INFOMSGS '
      FROM #DBCC_IND
     WHERE PageType = 1 -- data page
     ORDER BY PageFID, PagePID
     
    OPEN page_loop

    FETCH NEXT FROM page_loop INTO @c_page_fid, @c_page_pid, @page, @SQL

    WHILE @@FETCH_STATUS = 0
        BEGIN              
            TRUNCATE TABLE #DBCC_Page
            
            PRINT @SQL
            
            INSERT INTO #DBCC_Page (ParentObject, Object, Field, Value)
            EXEC (@SQL)
            
            --UPDATE STATISTICS #DBCC_Page
                         
            -- read all 'ordinary' data
            INSERT INTO #raw_result (PageSlot, Field, Value)
            SELECT PageSlot = @page + ':' + SubString(ParentObject, 6, CharIndex('Offset', ParentObject, 7) - 7),
                   Field,
                   Value    = (CASE WHEN Value = '[NULL]' AND Object LIKE '%Length (physical) 0' THEN NULL ELSE Value END)
              FROM #DBCC_Page
             WHERE Object LIKE 'Slot %'
               AND Field NOT IN ('', 'Record Type', 'Record Attributes')
               AND ParentObject NOT IN ('PAGE HEADER:')
               
               
            -- read all the 'INLINE-BLOB-data', will be processed later on
            INSERT INTO #inline_blob (row_id, PageSlot, column_name, column_type, sub_Value)
            SELECT row_id,
                   PageSlot = @page + ':' + SubString(ParentObject, 6, CharIndex('Offset', ParentObject, 7) - 7),                   
                   c.column_name, 
                   c.column_type,
                   sub_Value = SubString(dp.Value, 21, 35)
              FROM #columns c
              JOIN #DBCC_Page dp
                ON Left(dp.Object, Len(c.column_name + ' = [BLOB Inline Data]')) = c.column_name + ' = [BLOB Inline Data]'                
             WHERE c.column_length = -1                       
               AND c.column_type   IN  (N'varchar', N'nvarchar', N'varbinary')

            UNION ALL

            SELECT row_id,
                   PageSlot = @page + ':' + SubString(ParentObject, 6, CharIndex('Offset', ParentObject, 7) - 7),                   
                   c.column_name, 
                   c.column_type,
                   sub_Value = SubString(dp.Value, 21, 35)
              FROM #columns c
              JOIN #DBCC_Page dp
                ON Left(dp.Object, Len(c.column_name + ' = [Binary data]')) = c.column_name + ' = [Binary data]'
             WHERE c.column_length > 0                       
               AND c.column_type IN  (N'varbinary', N'binary')
               
            -- See if there are 'out of row' entries, if so, store them for later processing
            -- => might need some optimizing =/
            ; WITH base (ParentObject, Object, column_name, column_type)
                AS (SELECT DISTINCT
                            dp.ParentObject,
                            dp.Object,
                            c.column_name,
                            c.column_type
                       FROM #columns c
                       JOIN #DBCC_Page dp
                         ON Left(dp.Object, Len(c.column_name + ' = [BLOB Inline Root]')) = c.column_name + ' = [BLOB Inline Root]'                                    
                      WHERE c.column_length = -1
                        AND c.column_type IN  (N'varchar', N'nvarchar', N'varbinary')),
                   links (ParentObject, Object, column_name, column_type, link)
                AS (SELECT DISTINCT
                           b.ParentObject,
                           b.Object,
                           b.column_name,
                           b.column_type,
                           link = dp.Object
                      FROM base b
                      JOIN #DBCC_Page dp
                        ON dp.ParentObject = b.Object
                       AND dp.Object Like 'Link %'
                       AND dp.Field IN ('Size', 'RowID'))
                                              
            INSERT #out_of_row_blob (PageSlot, column_name, column_type, link_id, file_id, page_id, slot_id, size)
            SELECT PageSlot = @page + ':' + SubString(l.ParentObject, 6, CharIndex('Offset', l.ParentObject, 7) - 7),
                   column_name,
                   column_type,
                   link_id = Convert(int, SubString(dp1.Object, 6, 10)),
                   -- link values look like : (x:yyy:z) being (file_id:page_id:slot_id)
                   file_id = SubString(dp1.Value, 2, CharIndex(':', dp1.Value, 3) - 2),
                   page_id = SubString(dp1.Value, CharIndex(':', dp1.Value, 3) + 1,  CharIndex(':', dp1.Value, CharIndex(':', dp1.Value, 3) + 1) - CharIndex(':', dp1.Value, 3) - 1 ),
                   slot_id = SubString(dp1.Value, 
                                                                               (CharIndex(':', dp1.Value, CharIndex(':', dp1.Value, 3) + 1) + 1), -- offset for second ':'
                                                 CharIndex(')', dp1.Value, 6) - (CharIndex(':', dp1.Value, CharIndex(':', dp1.Value, 3) + 1) + 1) -- length
                                       ),
                   size    = dp2.Value
             FROM links l
             JOIN #DBCC_Page dp1
               ON dp1.ParentObject = l.Object
              AND dp1.Object = l.link
              AND dp1.Field = 'RowId'
              AND dp1.Value LIKE '(%:%:%)'
             JOIN #DBCC_Page dp2
               ON dp2.ParentObject = l.Object
              AND dp2.Object = l.link
              AND dp2.Field = 'Size'
              
               
            FETCH NEXT FROM page_loop INTO @c_page_fid, @c_page_pid, @page, @SQL
        END    
    CLOSE page_loop
    DEALLOCATE page_loop
    
    -- let helper proc do the inline-blob data
    EXEC sp_selectpages_inlineblob
    
    EXEC sp_selectpages_outofrowblob @db_id = @db_id
    
    -- now combine all the 'vertical' data found and push it into the actual table (horizontal)
    UPDATE STATISTICS #raw_result
    
    SELECT @SQL = @create_table
                + N' INSERT ' + @output_table + ' (' + @column_list + ')'
                + N' SELECT ' + @column_list2
                + N'   FROM (SELECT p = PageSlot,'
                + N'                x_FieldName_x = Field,'
                + N'                x_Value_x     = Value'
                + N'           FROM #raw_result) Tab'
                + N'  PIVOT (MAX(Tab.x_Value_x) FOR Tab.x_FieldName_x IN( ' + @column_list + ' )) AS pvt'
                + (CASE WHEN @target_table IS NULL THEN N' SELECT * FROM ' + @output_table
                                                   ELSE N' SELECT info = ''Data was stored in table ' + @output_table + '''' END) 

    --PRINT @SQL
    EXEC (@SQL)
    
    RETURN (0)
END
GO
if OBJECT_ID('sp_selectpages') is null
  PRINT 'Failed to create procedure sp_selectpages...'
ELSE
  PRINT 'Correctly created procedure sp_selectpages...'