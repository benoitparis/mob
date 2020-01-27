SELECT
  CAST(COALESCE('1', null) AS VARCHAR) dummy_key,
  MAX(ly.proctime_append_stream) max_proctime,
  CAST(SUM(CASE WHEN us.side = 'left'  THEN ly.y ELSE 0.003 END) / (0.00001 + SUM(CASE WHEN us.side = 'left'  THEN 1 ELSE 0 END)) AS DOUBLE) leftY,
  CAST(SUM(CASE WHEN us.side = 'right' THEN ly.y ELSE 0.003 END) / (0.00001 + SUM(CASE WHEN us.side = 'right' THEN 1 ELSE 0 END)) AS DOUBLE) rightY
FROM (
  SELECT *
  FROM (
    SELECT
      actor_identity,
      MAX(proctime_append_stream) AS proctime_append_stream,
      --LAST_VALUE(PROCTIME_MATERIALIZE(proctime_append_stream)) AS proctime_append_stream, 
        -- LAST_VALUE aggregate function does not support type: ''TIMESTAMP_WITHOUT_TIME_ZONE''.
      LAST_VALUE(y) AS y
    FROM write_y
    --JOIN (SELECT MAX(proctime_append_stream) current_time_field FROM tick_service) current_time_table ON true
    GROUP BY actor_identity
    --HAVING MAX(proctime_append_stream) > (MAX(current_time_table.current_time_field) - INTERVAL '10' SECOND)
    -- -> ça ferait des choses? HAVING (PROCTIME_MATERIALIZE(MAX(proctime_append_stream))) > (PROCTIME_MATERIALIZE(PROCTIME()) - INTERVAL '10' SECOND)
  )
) AS ly
JOIN user_side AS us 
  ON ly.actor_identity = us.actor_identity
  --AND ly.proctime_append_stream = (PROCTIME_MATERIALIZE(us.actor_identity) - INTERVAL '10' SECOND)