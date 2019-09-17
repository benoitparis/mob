INSERT INTO send_client                                   
SELECT                                                    
  loopback_index,                                         
  actor_identity,     
  ROW(X, Y, time_string1, time_string2, time_string3, time_string4, time_string5) payload                          
FROM (                                                    
  SELECT                                                  
    o.loopback_index                      AS loopback_index,  
    o.actor_identity                      AS actor_identity,  
    r.X                                   AS X,               
    r.Y                                   AS Y,               
    CAST(o.proctime AS VARCHAR)           AS time_string1,     
    CAST(o.proctime AS VARCHAR)           AS time_string2,     
    CAST(o.proctime AS VARCHAR)           AS time_string3,     
    CAST('dsadsdsa' AS VARCHAR)           AS time_string4,
    CAST(o.proctime AS VARCHAR)           AS time_string5
  FROM                                                    
    query_global_position AS o                                  
  --JOIN LATERAL TABLE (global_position(o.proctime)) AS r            
  --JOIN global_position FOR SYSTEM_TIME AS OF o.proctime AS r
  JOIN (
    SELECT                                                                              
      loopback_index,
      --MAX(proctime_last) proctime_last,
      AVG(X) X,
      AVG(Y) Y
    FROM (
      SELECT
        actor_identity,
        LAST_VALUE(loopback_index) loopback_index,
        LAST_VALUE(proctime) proctime_last, 
        LAST_VALUE(X) X,
        LAST_VALUE(Y) Y
      FROM write_position
      GROUP BY actor_identity
    ) last_position                                                 
    GROUP BY loopback_index
  ) r
    ON r.loopback_index = o.loopback_index                       

)                                                         
