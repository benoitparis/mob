CREATE TEMPORAL TABLE global_position TIME ATTRIBUTE max_proctime PRIMARY KEY side AS
SELECT   
/*
  SUM(CASE WHEN us.side IN ('left', 'right') THEN 0 ELSE null END) constant_dummy, -- the length we have to go
  MAX(CASE WHEN us.side IN ('left', 'right') THEN ly.proctime ELSE null END) max_proctime,             
*/  
  us.side side,
  MAX(ly.proctime) max_proctime,
  SUM(CASE WHEN us.side = 'left'  THEN ly.y ELSE 0 END) / (0.01 + SUM(CASE WHEN us.side = 'left'  THEN 1 ELSE 0 END)) leftY,
  SUM(CASE WHEN us.side = 'right' THEN ly.y ELSE 0 END) / (0.01 + SUM(CASE WHEN us.side = 'right' THEN 1 ELSE 0 END)) rightY
FROM last_y AS ly
   , LATERAL TABLE (user_side(ly.proctime)) AS us 
WHERE ly.actor_identity = us.actor_identity
GROUP BY us.side