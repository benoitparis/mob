/*
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
  )
) AS ly
JOIN user_side AS us 
  ON ly.actor_identity = us.actor_identity
*/
/*
SELECT
  CAST(ly.actor_identity                       AS VARCHAR) || ' - ' ||
  --CAST(ly.proctime_append_stream               AS VARCHAR) || ' - ' ||
  CAST(ly.last_value_proctime_append_stream    AS VARCHAR) || ' - ' ||
  CAST(ly.y                                    AS VARCHAR) || ' - ' ||
  --CAST(ly.current_time_field               AS VARCHAR) || ' - ' ||
  CAST(us.side                             AS VARCHAR) || ' - '
  AS debug_message
FROM (
  SELECT 
    *
  FROM (
    SELECT
      actor_identity,
      --MAX(proctime_append_stream)    AS proctime_append_stream,
      LAST_VALUE(CAST(PROCTIME_MATERIALIZE(proctime_append_stream) AS BIGINT))    AS last_value_proctime_append_stream,
      LAST_VALUE(y)                  AS y
      --, LAST_VALUE(current_time_field) AS current_time_field
    FROM write_y
    --JOIN (SELECT LAST_VALUE(CAST(PROCTIME_MATERIALIZE(proctime_append_stream) AS VARCHAR)) current_time_field FROM tick_service) current_time_table ON true
    GROUP BY actor_identity
    --HAVING MAX(proctime_append_stream) > (MAX(current_time_table.current_time_field) - INTERVAL '10' SECOND)
  )
) AS ly
JOIN user_side AS us 
  ON ly.actor_identity = us.actor_identity
*/

SELECT
  COALESCE(CAST(actor_identity                                       AS VARCHAR), '_') || ' - ' ||
  COALESCE(CAST(max_proctime_append_stream                           AS VARCHAR), '_') || ' - ' ||
  COALESCE(CAST(sStart                                               AS VARCHAR), '_') || ' - ' ||
  COALESCE(CAST(sEnd                                                 AS VARCHAR), '_') || ' - ' ||
  COALESCE(CAST(max_proctime_append_stream - COALESCE(sEnd, 0)       AS VARCHAR), '_') || ' - ' ||
  COALESCE(CAST(0 < (max_proctime_append_stream - COALESCE(sEnd, 0)) AS VARCHAR), '_') || ' - ' 
  AS debug_message
FROM (
  SELECT
    *
  FROM (
    SELECT
      actor_identity,
      UNIX_TIMESTAMP(CAST(PROCTIME_MATERIALIZE(MAX(proctime_append_stream)) AS VARCHAR)) max_proctime_append_stream
    FROM write_y
    GROUP BY actor_identity
  ) last_activity
  LEFT JOIN (
    SELECT 
      actor_identity,
      LAST_VALUE(UNIX_TIMESTAMP(CAST(PROCTIME_MATERIALIZE(sStart) AS VARCHAR))) AS sStart,
      LAST_VALUE(UNIX_TIMESTAMP(CAST(PROCTIME_MATERIALIZE(sEnd  ) AS VARCHAR))) AS sEnd
    FROM (
      SELECT
        actor_identity,
        SESSION_START(proctime_append_stream, INTERVAL '10' SECOND) AS sStart,
        SESSION_END(proctime_append_stream, INTERVAL '10' SECOND) AS sEnd
      FROM write_y 
      GROUP BY actor_identity, SESSION(proctime_append_stream, INTERVAL '10' SECOND)
    )
    GROUP BY actor_identity
  ) session_info
    ON last_activity.actor_identity = session_info.actor_identity
)