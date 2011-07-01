USE test
GO


IF OBJECT_ID('t_test_source') IS NOT NULL DROP TABLE t_test_source
GO
SELECT row_id = IDENTITY(int, 1, 1) ,       

       char_field             = Convert(char(50), ''),
       char_field_len         = Convert(int, 0),
       
       varchar_field          = Convert(varchar(500), ''),
       varchar_field_len      = Convert(int, 0),
       maxvarchar_field       = Convert(varchar(max), ''),
       maxvarchar_field_len   = Convert(int, 0),
       
       nchar_field            = Convert(nchar(50), ''),
       nchar_field_len        = Convert(int, 0),
       
       nvarchar_field         = Convert(nvarchar(500), ''),
       nvarchar_field_len     = Convert(int, 0),
       maxnvarchar_field      = Convert(nvarchar(max), ''),
       maxnvarchar_field_len  = Convert(int, 0),
       
       binary_field           = Convert(binary(50), 0x),
       binary_field_len       = Convert(int, 0),
       
       varbinary_field        = Convert(varbinary(500), 0x),
       varbinary_field_len    = Convert(int, 0),
       maxvarbinary_field     = Convert(varbinary(max), 0x),
       maxvarbinary_field_len = Convert(int, 0),
              
       row_crc          = Convert(int, 0),
       row_size         = Convert(int, 0)
                     
  INTO t_test_source
  
GO
  
INSERT t_test_source (char_field, varchar_field, maxvarchar_field, nchar_field, nvarchar_field, maxnvarchar_field, binary_field, varbinary_field, maxvarbinary_field)
SELECT char_field         = Convert(char(4),       'ABC_') + Convert(char(50), NEWID()),
       varchar_field      = Convert(varchar(500),  'DEF_') + Convert(varchar(500), NEWID()),
       maxvarchar_field   = Convert(varchar(max),  'EFG_') + Convert(varchar(max), Replicate(convert(varchar(max), 'row_id(' + convert(varchar, row_id) + ')'), row_id * 100)),
       char_field         = Convert(nchar(4),      'HIJ_') + Convert(nchar(50), NEWID()),
       nvarchar_field     = Convert(nvarchar(500), 'KLM_') + Convert(nvarchar(500), NEWID()),
       maxvarchar_field   = Convert(nvarchar(max), 'NOP_') + Convert(varchar(max), Replicate(convert(nvarchar(max), 'row_id(' + convert(nvarchar, row_id) + ')'), row_id * 47)),                      
       binary_field       = Convert(binary(4),     'QRS_') + Convert(binary(50), NEWID()),
       varbinary_field    = Convert(varbinary(500),'TUV_') + Convert(varbinary, NEWID()),
       maxvarbinary_field = Convert(varbinary(max),'WXY_') + Convert(varbinary(max), Replicate(convert(varchar(max), 'row_id(' + convert(varchar, row_id) + ')'), row_id * 97))
  FROM t_test_source
GO 10

-- need some NULL's too
INSERT t_test_source (char_field, varchar_field, maxvarchar_field, nchar_field, nvarchar_field, maxnvarchar_field, binary_field, varbinary_field, maxvarbinary_field)
SELECT DISTINCT char_field         = (CASE WHEN ABS(BINARY_CHECKSUM(NewID())) % 99 < 75 THEN NULL ELSE char_field END),
                varchar_field      = (CASE WHEN ABS(BINARY_CHECKSUM(NewID())) % 99 < 75 THEN NULL ELSE varchar_field END),
                maxvarchar_field   = (CASE WHEN ABS(BINARY_CHECKSUM(NewID())) % 99 < 75 THEN NULL ELSE maxvarchar_field END),
                nchar_field        = (CASE WHEN ABS(BINARY_CHECKSUM(NewID())) % 99 < 75 THEN NULL ELSE nchar_field END),
                nvarchar_field     = (CASE WHEN ABS(BINARY_CHECKSUM(NewID())) % 99 < 75 THEN NULL ELSE nvarchar_field END),
                maxnvarchar_field  = (CASE WHEN ABS(BINARY_CHECKSUM(NewID())) % 99 < 75 THEN NULL ELSE maxnvarchar_field END),
                binary_field       = (CASE WHEN ABS(BINARY_CHECKSUM(NewID())) % 99 < 75 THEN NULL ELSE binary_field END),
                varbinary_field    = (CASE WHEN ABS(BINARY_CHECKSUM(NewID())) % 99 < 75 THEN NULL ELSE varbinary_field END),
                maxvarbinary_field = (CASE WHEN ABS(BINARY_CHECKSUM(NewID())) % 99 < 75 THEN NULL ELSE maxvarbinary_field END)                
  FROM t_test_source
  
-- derive calculated fields to make testing easier later on
UPDATE t_test_source 
   SET char_field_len         = (CASE WHEN char_field         IS NULL THEN -1 ELSE Len(char_field)         END),
       varchar_field_len      = (CASE WHEN varchar_field      IS NULL THEN -1 ELSE Len(varchar_field)      END),
       maxvarchar_field_len   = (CASE WHEN maxvarchar_field   IS NULL THEN -1 ELSE Len(maxvarchar_field)   END),
       nchar_field_len        = (CASE WHEN nchar_field        IS NULL THEN -1 ELSE Len(nchar_field)        END),
       nvarchar_field_len     = (CASE WHEN nvarchar_field     IS NULL THEN -1 ELSE Len(nvarchar_field)     END),
       maxnvarchar_field_len  = (CASE WHEN maxnvarchar_field  IS NULL THEN -1 ELSE Len(maxnvarchar_field)  END),
       binary_field_len       = (CASE WHEN binary_field       IS NULL THEN -1 ELSE Len(binary_field)       END),
       varbinary_field_len    = (CASE WHEN varbinary_field    IS NULL THEN -1 ELSE Len(varbinary_field)    END),
       maxvarbinary_field_len = (CASE WHEN maxvarbinary_field IS NULL THEN -1 ELSE Len(maxvarbinary_field) END)
       
UPDATE t_test_source 
   SET row_crc = BINARY_CHECKSUM(row_id, char_field, char_field_len, 
                                         varchar_field, varchar_field_len, 
                                         maxvarchar_field, maxvarchar_field_len,
                                         nchar_field, nchar_field_len, 
                                         nvarchar_field, nvarchar_field_len, 
                                         maxnvarchar_field, maxnvarchar_field_len,
                                         binary_field, binary_field_len, 
                                         varbinary_field, varbinary_field_len,
                                         maxvarbinary_field, maxvarbinary_field_len),
       row_size = DataLength( row_id)
                + ISNULL(DataLength( char_field), 0)
                + ISNULL(DataLength( char_field_len), 0)
                + ISNULL(DataLength( varchar_field), 0)
                + ISNULL(DataLength( varchar_field_len), 0)
                + ISNULL(DataLength( maxvarchar_field), 0)
                + ISNULL(DataLength( maxvarchar_field_len), 0)
                + ISNULL(DataLength( nchar_field), 0)
                + ISNULL(DataLength( nchar_field_len), 0)
                + ISNULL(DataLength( nvarchar_field), 0)
                + ISNULL(DataLength( nvarchar_field_len), 0)
                + ISNULL(DataLength( maxnvarchar_field), 0)
                + ISNULL(DataLength( maxnvarchar_field_len), 0)
                + ISNULL(DataLength( binary_field), 0)
                + ISNULL(DataLength( binary_field_len), 0)
                + ISNULL(DataLength( varbinary_field), 0)
                + ISNULL(DataLength( varbinary_field_len), 0)
                + ISNULL(DataLength( maxvarbinary_field), 0)
                + ISNULL(DataLength( maxvarbinary_field_len), 0)
                + ISNULL(DataLength( row_crc), 0)

SELECT rows_found = COUNT(*), 
       _checksum = CHECKSUM_AGG(ALL row_crc)
  FROM t_test_source
 

GO
-- make temp table to do the actual testing

IF OBJECT_ID('tempdb..#test') IS NOT NULL DROP TABLE #test

SELECT *
  INTO #test
  FROM t_test_source 
 WHERE row_id = 200

-- decent test-size ?
EXEC tempdb..sp_spaceused #test

SELECT * FROM #test ORDER BY row_id