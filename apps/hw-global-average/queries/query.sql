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
    write_position AS o                                  
  JOIN LATERAL TABLE (global_position(o.proctime)) AS r            
    ON r.one_key = o.loopback_index                       
)                                                         
