CREATE TEMPORAL TABLE global_position TIME ATTRIBUTE start_time PRIMARY KEY one_key AS
SELECT                                                                              
  loopback_index one_key,                                                           
  HOP_START(proctime, INTERVAL '0.05' SECOND, INTERVAL '5' SECOND) start_time,
  AVG(X) X,                                                                        
  AVG(Y) Y
/*  
  SUM(
    Y * 
    EXP(
      CAST(CAST(proctime AS TIMESTAMP) as BIGINT) - 
      --proctime - 
      HOP_START(proctime, INTERVAL '0.05' SECOND, INTERVAL '5' SECOND)
    )
  )      
*/  
FROM write_position                                                                
GROUP BY loopback_index, HOP(proctime, INTERVAL '0.05' SECOND, INTERVAL '5' SECOND)

-- proctime:                                    TIME ATTRIBUTE(PROCTIME)
-- CAST(proctime AS TIMESTAMP):                 TIMESTAMP(3)
-- CAST(proctime AS NUMERIC):                   ! Cast function cannot convert value of type TIME ATTRIBUTE(PROCTIME) to type DECIMAL(1073741823, 0)
-- CAST(proctime AS SQL_TIMESTAMP):             ! SQL validation failed. class org.apache.calcite.sql.SqlIdentifier: SQL_TIMESTAMP
-- CAST(proctime AS TIME):                      TIME(0)
-- CAST(proctime AS DATETIME):                  ! SQL validation failed. class org.apache.calcite.sql.SqlIdentifier: DATETIME
-- INTERVAL '5' SECOND:                         INTERVAL SECOND
-- CAST(proctime as BIGINT):                    ! Cast function cannot convert value of type TIME ATTRIBUTE(PROCTIME) to type BIGINT
-- CAST(CAST(proctime AS TIMESTAMP) as BIGINT): ! Cast function cannot convert value of type TIMESTAMP(3) to type BIGINT

/*
Cannot apply '-' to arguments of type '<TIME ATTRIBUTE(PROCTIME)> - <TIME ATTRIBUTE(PROCTIME)>'. Supported form(s): 
'<NUMERIC> - <NUMERIC>'
'<DATETIME_INTERVAL> - <DATETIME_INTERVAL>'
'<DATETIME> - <DATETIME_INTERVAL>'
*/

