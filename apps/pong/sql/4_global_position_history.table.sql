SELECT
  CAST(COALESCE('1', null) AS VARCHAR) dummy_key,
  MAX(ly.proctime_append_stream) max_proctime,
  CAST(SUM(CASE WHEN us.side = 'left'  THEN ly.y ELSE 0 END) / (0.0001 + SUM(CASE WHEN us.side = 'left'  THEN 1 ELSE 0 END)) AS DOUBLE) leftY, --why?
  CAST(SUM(CASE WHEN us.side = 'right' THEN ly.y ELSE 0 END) / (0.0001 + SUM(CASE WHEN us.side = 'right' THEN 1 ELSE 0 END)) AS DOUBLE) rightY
FROM (
  SELECT
    actor_identity,
    MAX(proctime_append_stream) AS proctime_append_stream,
    LAST_VALUE(y) AS y
  FROM write_y
  GROUP BY actor_identity
) AS ly
JOIN user_side AS us 
  ON ly.actor_identity = us.actor_identity