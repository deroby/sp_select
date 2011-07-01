USE master
GO

if OBJECT_ID('sp_selectpages_outofrowblob') is not null
  BEGIN
    PRINT 'dropping procedure sp_selectpages_outofrowblob'
    DROP PROCEDURE sp_selectpages_outofrowblob
  END
GO
/*
    Helper procedure for sp_selectpages
*/
CREATE PROCEDURE sp_selectpages_outofrowblob (@db_id int)
AS
BEGIN
    SET NOCOUNT ON
    
    DECLARE @nvarchar_max nvarchar(max),
            @varchar_max varchar(max),
            @varbinary_max varbinary(max),
            @c_PageSlot varchar(520),
            @c_next_PageSlot varchar(520),            
            @c_column_name sysname,
            @c_next_column_name sysname,
            @c_column_type sysname,
            @c_next_column_type sysname,
            @c_Value char(44),
            @c_link_id int,
            @c_file_id int,
            @c_page_id int,
            @c_slot_id int,
            @SQL nvarchar(max),
            @mind_offset bit
            
            
     -- create hex-values & tables
    CREATE TABLE #hex4 (int_value int NOT NULL,
                        hex_value char(1) NOT NULL)
                            
    INSERT #hex4 (int_value, hex_value) VALUES (0, '0')
    INSERT #hex4 (int_value, hex_value) VALUES (1, '1')
    INSERT #hex4 (int_value, hex_value) VALUES (2, '2')
    INSERT #hex4 (int_value, hex_value) VALUES (3, '3')
    INSERT #hex4 (int_value, hex_value) VALUES (4, '4')
    INSERT #hex4 (int_value, hex_value) VALUES (5, '5')
    INSERT #hex4 (int_value, hex_value) VALUES (6, '6')
    INSERT #hex4 (int_value, hex_value) VALUES (7, '7')
    INSERT #hex4 (int_value, hex_value) VALUES (8, '8')
    INSERT #hex4 (int_value, hex_value) VALUES (9, '9')
    INSERT #hex4 (int_value, hex_value) VALUES (10, 'a')
    INSERT #hex4 (int_value, hex_value) VALUES (11, 'b')
    INSERT #hex4 (int_value, hex_value) VALUES (12, 'c')
    INSERT #hex4 (int_value, hex_value) VALUES (13, 'd')
    INSERT #hex4 (int_value, hex_value) VALUES (14, 'e')
    INSERT #hex4 (int_value, hex_value) VALUES (15, 'f')
    
    CREATE UNIQUE CLUSTERED INDEX uq0_hex4 ON #hex4 (hex_value) WITH (FILLFACTOR = 100)

    CREATE TABLE #hex8 (int_value int     NOT NULL,
                        hex_value char(2) NOT NULL)
                        
    INSERT #hex8 (int_value, hex_value)
    SELECT int_value = h2.int_value * 16 
                     + h1.int_value, 
           hex_value = h2.hex_value 
                     + h1.hex_value 
      FROM #hex4 h1, #hex4 h2
                        
    CREATE UNIQUE CLUSTERED INDEX uq0_hex8 ON #hex8 (hex_value) WITH (FILLFACTOR = 100)
    
    CREATE TABLE #hex16(int_value int     NOT NULL,
                        hex_value char(4) NOT NULL)
                       
    INSERT #hex16 (int_value, hex_value)
    SELECT int_value = h4.int_value * 16 * 16 * 16 
                     + h3.int_value * 16 * 16
                     + h2.int_value * 16 
                     + h1.int_value, 
           hex_value = h2.hex_value 
                     + h1.hex_value 
                     + h4.hex_value 
                     + h3.hex_value
      FROM #hex4 h1, #hex4 h2, #hex4 h3, #hex4 h4
      
    CREATE UNIQUE CLUSTERED INDEX uq0_hex16 ON #hex16 (hex_value) WITH (FILLFACTOR = 100)
    
    -- look for BLOB inline-data (varchar(max))
    --------------------------------------------------------------------
    -- blob dump looks like this : 
    -- 000000000E46F9F8:   41414141 41414141 41414141 41414141 †AAAAAAAAAAAAAAAA
    --          1         2         3         4         5
    -- 12345678901234567890123456789012345678901234567890123456789012345678901234567890
    --
    -- => preprocessed into sub_Value, hence (partial) blob dump looks like this : 
    -- 41414141 41414141 41414141 41414141
    --          1         2         3     
    -- 12345678901234567890123456789012345
        
    
    CREATE TABLE #split_varchar_max (sub_id int NOT NULL, pos int NOT NULL)
    
    INSERT #split_varchar_max (sub_id, pos) VALUES (1, 1)
    INSERT #split_varchar_max (sub_id, pos) VALUES (2, 3)
    INSERT #split_varchar_max (sub_id, pos) VALUES (3, 5)
    INSERT #split_varchar_max (sub_id, pos) VALUES (4, 7)
    
    INSERT #split_varchar_max (sub_id, pos) VALUES (5, 10)
    INSERT #split_varchar_max (sub_id, pos) VALUES (6, 12)
    INSERT #split_varchar_max (sub_id, pos) VALUES (7, 14)
    INSERT #split_varchar_max (sub_id, pos) VALUES (8, 16)
    
    INSERT #split_varchar_max (sub_id, pos) VALUES (9, 19)    
    INSERT #split_varchar_max (sub_id, pos) VALUES (10, 21)
    INSERT #split_varchar_max (sub_id, pos) VALUES (11, 23)
    INSERT #split_varchar_max (sub_id, pos) VALUES (12, 25)
    
    INSERT #split_varchar_max (sub_id, pos) VALUES (13, 28)
    INSERT #split_varchar_max (sub_id, pos) VALUES (14, 30)
    INSERT #split_varchar_max (sub_id, pos) VALUES (15, 32)
    INSERT #split_varchar_max (sub_id, pos) VALUES (16, 34)

    INSERT #split_varchar_max (sub_id, pos) VALUES (17, 37)
    INSERT #split_varchar_max (sub_id, pos) VALUES (18, 39)
    INSERT #split_varchar_max (sub_id, pos) VALUES (19, 41)
    INSERT #split_varchar_max (sub_id, pos) VALUES (20, 43)
   
    CREATE UNIQUE CLUSTERED INDEX uq0_split_varchar_max ON #split_varchar_max (sub_id) WITH (FILLFACTOR = 100)
        
    -- look for BLOB inline-data (nvarchar(max))
    --------------------------------------------------------------------
    -- blob dump looks like this : 
    -- 00000000100D17A8:   41004100 41004100 41004100 41004100 †A.A.A.A.A.A.A.A.
    --          1         2         3         4         5
    -- 12345678901234567890123456789012345678901234567890123456789012345678901234567890
    --  
    -- => preprocessed into sub_Value, hence (partial) blob dump looks like this : 
    -- partial blob dump looks like this : 
    -- 41004100 41004100 41004100 41004100
    --          1         2         3     
    -- 12345678901234567890123456789012345
   
    CREATE TABLE #split_nvarchar_max (sub_id int NOT NULL, pos int NOT NULL)
    
    INSERT #split_nvarchar_max (sub_id, pos) VALUES (1, 1)
    INSERT #split_nvarchar_max (sub_id, pos) VALUES (2, 5)
    
    INSERT #split_nvarchar_max (sub_id, pos) VALUES (3, 10)
    INSERT #split_nvarchar_max (sub_id, pos) VALUES (4, 14)
    
    INSERT #split_nvarchar_max (sub_id, pos) VALUES (5, 19)
    INSERT #split_nvarchar_max (sub_id, pos) VALUES (6, 23)
    
    INSERT #split_nvarchar_max (sub_id, pos) VALUES (7, 28)
    INSERT #split_nvarchar_max (sub_id, pos) VALUES (8, 32)
    
    INSERT #split_nvarchar_max (sub_id, pos) VALUES (9, 37)
    INSERT #split_nvarchar_max (sub_id, pos) VALUES (10, 41)
    
    CREATE UNIQUE CLUSTERED INDEX uq0_split_nvarchar_max ON #split_nvarchar_max (sub_id) WITH (FILLFACTOR = 100)
    
    
    SELECT * FROM #out_of_row_blob ORDER BY PageSlot, column_name, link_id
    
    UPDATE STATISTICS #out_of_row_blob
    
    SELECT @varchar_max  = '',
           @nvarchar_max = N'',
           @varbinary_max = 0x
           
    DECLARE outofrow_blob_loop CURSOR LOCAL FAST_FORWARD
        FOR SELECT PageSlot,
                   column_name, 
                   column_type,
                   link_id,
                   file_id,
                   page_id,
                   slot_id
              FROM #out_of_row_blob
             ORDER BY PageSlot, column_name, link_id
    OPEN outofrow_blob_loop 
    FETCH NEXT FROM outofrow_blob_loop INTO @c_PageSlot, @c_column_name, @c_column_type, @c_link_id, @c_file_id, @c_page_id, @c_slot_id
    WHILE @@FETCH_STATUS = 0
        BEGIN
            -- first of all, fetch the PAGE (re-using already existing table #DBCC_Page
            
            
            SELECT @SQL = N'DBCC PAGE (' + QuoteName(DB_Name(@db_id)) + N',' + Convert(nvarchar(20), @c_file_id) + N',' + Convert(nvarchar(20), @c_page_id) + N', 1) WITH TABLERESULTS, NO_INFOMSGS ',
                   @mind_offset = 1
                   
            TRUNCATE TABLE #DBCC_Page
               
               PRINT @SQL
                        
            INSERT INTO #DBCC_Page (ParentObject, Object, Field, Value)
            EXEC (@SQL)
            
            -- Memory dump records look more or less like this : 
            -- 0000000000000050:   41004100 41004100 41004100 41004100 41004100 †A.A.A.A.A.A.A.A.A.A.
            -- 12345678901234567890123456789012345678901234567890123456789012345678901234567890123456
            --          1         2         3         4         5         6         7         8

            -- fetch information from the relevant slot            
            DECLARE slot_loop CURSOR LOCAL FAST_FORWARD
                FOR SELECT SubString(Value, 21, 44)
                      FROM #DBCC_Page
                     WHERE ParentObject LIKE 'Slot ' + Convert(varchar, @c_slot_id) + ',%'
                     ORDER BY row_id
            OPEN slot_loop 
            FETCH NEXT FROM slot_loop INTO @c_Value
            WHILE @@FETCH_STATUS = 0
                BEGIN
            
                    -- mind the initial offset !!
                    -- xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
                    -- 08000e0f 0000a41f 00000000 03004100 41004100
                    -- 12345678901234567890123456789012345678901234
                    -- 0        1         2         3
                    -- 11223344 55667788 99001122 3344
                    
                    SELECT @varchar_max = @varchar_max + Char(h.int_value)
                      FROM #hex8 h
                      JOIN #split_varchar_max s
                        ON SubString(@c_Value, s.pos, 2) = h.hex_value
                       AND s.sub_id > (CASE @mind_offset WHEN 1 THEN 14 ELSE 0 END)  
                     WHERE @c_column_type = N'varchar'
                       
                     ORDER BY s.sub_id
             
                    SELECT @varbinary_max = @varbinary_max  + Convert(binary(1), h.int_value)
                      FROM #hex8 h
                      JOIN #split_varchar_max s -- single byte too !
                        ON SubString(@c_Value, s.pos, 2) = h.hex_value
                       AND s.sub_id > (CASE @mind_offset WHEN 1 THEN 14 ELSE 0 END)  
                     WHERE @c_column_type IN (N'varbinary', N'binary')
                       
                     ORDER BY s.sub_id

                    SELECT @nvarchar_max = @nvarchar_max + NChar(h.int_value)
                      FROM #hex16 h
                      JOIN #split_nvarchar_max s
                        ON SubString(@c_Value, s.pos, 4) = h.hex_value
                       AND s.sub_id > (CASE @mind_offset WHEN 1 THEN 7 ELSE 0 END)  
                     WHERE @c_column_type = N'nvarchar'                       
                     ORDER BY s.sub_id
                     
                    SELECT @mind_offset = 0
                     
                    FETCH NEXT FROM slot_loop INTO @c_Value
                END
            CLOSE slot_loop
            DEALLOCATE slot_loop
                
            FETCH NEXT FROM outofrow_blob_loop INTO @c_next_PageSlot, @c_next_column_name, @c_next_column_type, @c_link_id, @c_file_id, @c_page_id, @c_slot_id
            
            IF @@FETCH_STATUS <> 0 OR ( ISNULL(@c_next_PageSlot, '') <> @c_PageSlot) OR ( ISNULL(@c_next_column_name, '') <> @c_column_name)
                BEGIN
                    -- save what we have, can't use CASE here as it probably would mess up datatypes
                    INSERT #raw_result (PageSlot, Field, Value)
                    SELECT PageSlot = @c_PageSlot,
                           Field    = @c_column_name,
                           Value    = @varchar_max 
                     WHERE @c_column_type = 'varchar'
                     UNION ALL
                    SELECT PageSlot = @c_PageSlot,
                           Field    = @c_column_name,
                           Value    = @nvarchar_max 
                     WHERE @c_column_type = 'nvarchar'
                     UNION ALL
                    SELECT PageSlot = @c_PageSlot,
                           Field    = @c_column_name,
                           Value    = @varbinary_max 
                     WHERE @c_column_type IN ('varbinary', 'binary')
                     
                    IF @@ERROR <> 0
                        BEGIN
                            RaisError('%s: Failed to add the following data to the result :', 16, 1)
                            SELECT PageSlot = @c_PageSlot,
                                   Field    = @c_column_name,
                                   Type     = @c_column_type,
                                   varchar_max_Value = @varchar_max,
                                   nvarchar_max_Value = @nvarchar_max ,
                                   varbinary_max_Value = @varbinary_max 
                             
                        END
                     
                    -- reset veriables
                    SELECT @varchar_max = '',
                           @nvarchar_max = N'',
                           @varbinary_max = 0x
                END
                
            SELECT @c_PageSlot    = @c_next_PageSlot,
                   @c_column_name = @c_next_column_name, 
                   @c_column_type = @c_next_column_type
                   
        END
    CLOSE outofrow_blob_loop
    DEALLOCATE outofrow_blob_loop

    RETURN (0)
END
GO
if OBJECT_ID('sp_selectpages_outofrowblob') is null
    PRINT 'Failed to create procedure sp_selectpages_outofrowblob...'
ELSE
    PRINT 'Correctly created procedure sp_selectpages_outofrowblob...'