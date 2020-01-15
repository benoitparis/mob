SELECT   
  COALESCE('1', MAX(us.side)/* the lengths we have to go */) constant_dummy,
  HOP_START(ly.proctime_append_stream, INTERVAL '0.1' SECOND, INTERVAL '0.05' SECOND) max_proctime,
  SUM(CASE WHEN us.side = 'left'  THEN ly.y ELSE 0 END) / (0.01 + SUM(CASE WHEN us.side = 'left'  THEN 1 ELSE 0 END)) leftY, --why?
  SUM(CASE WHEN us.side = 'right' THEN ly.y ELSE 0 END) / (0.01 + SUM(CASE WHEN us.side = 'right' THEN 1 ELSE 0 END)) rightY
FROM last_y AS ly
   , LATERAL TABLE (user_side(ly.proctime_append_stream)) AS us 
WHERE ly.actor_identity = us.actor_identity
GROUP BY HOP(ly.proctime_append_stream, INTERVAL '0.1' SECOND, INTERVAL '0.05' SECOND)