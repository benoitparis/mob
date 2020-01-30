SELECT
  actor_identity,
  0 < (max_proctime_append_stream - COALESCE(sEnd, 0)) AS is_active
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