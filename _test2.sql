USE test
GO

IF OBJECT_ID('t_test_result') IS NOT NULL DROP TABLE t_test_result
GO
DECLARE @status         int
 
EXEC @status = sp_select @table_name     = N'#test',
                         @spid           = default,
                         @max_pages      = default,
                         @target_table   = N'test.dbo.t_test_result' 
 
SELECT return_value = @status

GO

UPDATE t_test_result 
   SET char_field_len         = (CASE WHEN char_field         IS NULL THEN -1 ELSE Len(char_field)         END),
       varchar_field_len      = (CASE WHEN varchar_field      IS NULL THEN -1 ELSE Len(varchar_field)      END),
       maxvarchar_field_len   = (CASE WHEN maxvarchar_field   IS NULL THEN -1 ELSE Len(maxvarchar_field)   END),
       nchar_field_len        = (CASE WHEN nchar_field        IS NULL THEN -1 ELSE Len(nchar_field)        END),
       nvarchar_field_len     = (CASE WHEN nvarchar_field     IS NULL THEN -1 ELSE Len(nvarchar_field)     END),
       maxnvarchar_field_len  = (CASE WHEN maxnvarchar_field  IS NULL THEN -1 ELSE Len(maxnvarchar_field)  END),
       binary_field_len       = (CASE WHEN binary_field       IS NULL THEN -1 ELSE Len(binary_field)       END),
       varbinary_field_len    = (CASE WHEN varbinary_field    IS NULL THEN -1 ELSE Len(varbinary_field)    END),
       maxvarbinary_field_len = (CASE WHEN maxvarbinary_field IS NULL THEN -1 ELSE Len(maxvarbinary_field) END)

GO
UPDATE t_test_result
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
                
SELECT info = 'Source', rows_found = COUNT(*), _checksum = CHECKSUM_AGG(ALL row_crc)
  FROM t_test_source
 UNION ALL 
SELECT info = 'Result', rows_found = COUNT(*), _checksum = CHECKSUM_AGG(ALL row_crc)
  FROM t_test_result



GO          
/*      
-- look for differences
SELECT src.*, rslt.*
  FROM t_test_source src
  FULL OUTER JOIN t_test_result rslt
               ON rslt.row_id = src.row_id
 WHERE src.row_id IS NULL
    OR rslt.row_id IS NULL
    OR ISNULL(src.row_crc, 0) <> ISNULL(rslt.row_crc, 0)
    OR ISNULL(src.row_size, 0) <> ISNULL(rslt.row_size, 0)
      
*/   
SELECT info = 'source', * FROM t_test_source WHERE row_id = 200
UNION ALL
SELECT info = 'result', * FROM t_test_result WHERE row_id = 200
