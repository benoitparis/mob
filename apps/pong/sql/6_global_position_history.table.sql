SELECT
  CAST(COALESCE('1', null) AS VARCHAR) dummy_key, -- There's a JIRA for that
  MAX(ly.proctime_append_stream) max_proctime,
  -- 0.003 / 0.00001 = 300 is the middle of the screen. Default value when there are no players.
  CAST(SUM(CASE WHEN us.side = 'left'  THEN ly.y ELSE 0.003 END) / (0.00001 + SUM(CASE WHEN us.side = 'left'  THEN 1 ELSE 0 END)) AS DOUBLE) leftY,
  CAST(SUM(CASE WHEN us.side = 'right' THEN ly.y ELSE 0.003 END) / (0.00001 + SUM(CASE WHEN us.side = 'right' THEN 1 ELSE 0 END)) AS DOUBLE) rightY
FROM (
  SELECT *
  FROM (
    SELECT
      actor_identity,
      MAX(proctime_append_stream) AS proctime_append_stream,
      LAST_VALUE(y) AS y
    FROM write_y
    GROUP BY actor_identity
  ) last_y
  JOIN active_users 
    ON last_y.actor_identity = active_users.actor_identity
  WHERE active_users.is_active
) AS ly
JOIN user_side AS us 
  ON ly.actor_identity = us.actor_identity