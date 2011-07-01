USE tempdb -- feel free to pick another one, but with tempdb we can't do too much harm =)
GO

-- cleanup
IF OBJECT_ID('t_test_out_of_row_blob') IS NOT NULL
    DROP TABLE t_test_out_of_row_blob

IF OBJECT_ID('tempdb..#DBCC_IND') IS NOT NULL
    DROP TABLE #DBCC_IND


IF OBJECT_ID('tempdb..#DBCC_Page') IS NOT NULL
    DROP TABLE #DBCC_Page  
    
GO
-- set up test table
CREATE TABLE t_test_out_of_row_blob (row_id     int NOT NULL IDENTITY(1, 1) PRIMARY KEY, 
                                     blob       varchar(max) NOT NULL,
                                     blob_size  int )
  
GO

-- record with out of row info
DECLARE @counter int,
        @blob    varchar(max)
        
SELECT @counter = 1,
       @blob = ''

WHILE @counter < 1000
    BEGIN
        SELECT @blob = @blob + 'Hello world ' + Convert(nvarchar(max), @counter) + ',',
               @counter = @counter + 1
    END
    
INSERT t_test_out_of_row_blob (blob) VALUES (@blob)

UPDATE t_test_out_of_row_blob SET blob_size = Len(blob)
GO

-- find out what this looks like (thx to John Huang's, Fabiano Neves Amorim, Filip De Vos)

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
                            ParentObject varchar(500),
                            Object varchar(500),
                            Field sysname,
                            Value nvarchar(max))                          
                            
DECLARE @db_id int,
        @object_id int,
        @SQL nvarchar(max),
        @c_pageFID smallint,
        @c_pagePID int
        
SELECT @db_id = DB_ID(),
       @object_id = Object_id('t_test_out_of_row_blob')
                            
SELECT @SQL = N'DBCC IND(' + QuoteName(DB_Name(@db_id)) + N', ' + CONVERT(varchar(10), @object_id) + N', 1) WITH NO_INFOMSGS'

DBCC TRACEON(3604) WITH NO_INFOMSGS
INSERT INTO #DBCC_IND
EXEC (@SQL)

DECLARE page_loop CURSOR LOCAL FAST_FORWARD
    FOR SELECT PageFID,
               PagePID
              FROM #DBCC_IND
             WHERE PageType = 1 -- data page
             ORDER BY PageFID, PagePID
OPEN page_loop
FETCH NEXT FROM page_loop INTO @c_pageFID, @c_pagePID

WHILE @@FETCH_STATUS = 0
    BEGIN              
        
        SELECT @SQL = N'DBCC PAGE (' + QuoteName(DB_Name(@db_id)) + N',' + Convert(nvarchar(20), @c_pageFID) + N',' + Convert(nvarchar(20), @c_pagePID) + N', 3) WITH TABLERESULTS, NO_INFOMSGS '
        PRINT @SQL
        
        INSERT INTO #DBCC_Page (ParentObject, Object, Field, Value)
        EXEC (@SQL)
        
        SELECT * FROM #DBCC_Page ORDER BY row_id        

        FETCH NEXT FROM page_loop INTO @c_pageFID, @c_pagePID
    END
CLOSE page_loop
DEALLOCATE page_loop
 
 
 
-- In my case, only one data-page is returned (which makes sense for this one record)
-- The lines that refer to my blob look like this : 

SELECT row_id, Object, Field, Value FROM #DBCC_Page WHERE Left(ParentObject, 25) = 'blob = [BLOB Inline Root]'

--  row_id Object     Field     Value
---------- -------------------------------
--      53 Link 0     Size      8040
--      54 Link 0     RowId     (1:380:0)
--      55 Link 1     Size      15876
--      56 Link 1     RowId     (1:377:0)

-- Taking the first link, I go to page (1:380:0) to fetch the first 8040 bytes from my blob

DBCC PAGE ([tempdb], 1, 377, 1) WITH TABLERESULTS, NO_INFOMSGS 

-- In the output here I find (among other things)
-- Record Size	8054 => which does NOT match my 8040 bytes from above.. I could understand it being less, but more ???
-- Hello world 1 to Hello world 510 (partially) but with some strange offset I can't quite understand yet
