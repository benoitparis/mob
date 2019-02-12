
INSERT INTO send_client                                   
SELECT                                                    
  loopback_index,                                         
  actor_identity,                                         
  ROW(X, Y, time_string) payload                          
FROM (                                                    
  SELECT                                                  
    o.loopback_index                  AS loopback_index,  
    o.actor_identity                  AS actor_identity,  
    r.X                               AS X,               
    r.Y                               AS Y,               
    CAST(o.proctime AS VARCHAR)      AS time_string      
  FROM                                                    
    write_position AS o                                  
  JOIN LATERAL TABLE (global_position(o.proctime)) AS r            
    ON r.one_key = o.loopback_index                       
)                                                         
