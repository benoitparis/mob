INSERT INTO send_client                                   
SELECT                                                    
  loopback_index,                                         
  actor_identity,     
  ROW(X, Y) payload                          
FROM (                                                    
  SELECT                                                  
    o.loopback_index                      AS loopback_index,  
    o.actor_identity                      AS actor_identity,  
    r.X                                   AS X,               
    r.Y                                   AS Y
  FROM query_global_position AS o                                  
  -- ew
     , LATERAL TABLE (global_position(o.proctime)) AS r            
  WHERE o.loopback_index = r.loopback_index                       
)                                                         
