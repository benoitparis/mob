SELECT
  activity.loopback_index,
  activity.actor_identity,
  sEnd IS NULL OR (CAST(tsActivity AS TIMESTAMP) > CAST(sEnd AS TIMESTAMP)) AS active
FROM (
  SELECT
    loopback_index,
    actor_identity,
    LAST_VALUE(CAST(proctime_append_stream AS VARCHAR)) AS tsActivity -- LAST_VALUE doesn't accept TIMESTAMP types
  FROM write_y
  GROUP BY loopback_index, actor_identity
) AS activity
LEFT JOIN
(
  SELECT
    actor_identity,
    LAST_VALUE(CAST(sEnd AS VARCHAR)) AS sEnd -- LAST_VALUE doesn't accept TIMESTAMP types
  FROM (
    SELECT
      actor_identity,
      SESSION_END(proctime_append_stream, INTERVAL '20' SECOND) sEnd
    FROM write_y
    GROUP BY actor_identity, SESSION(proctime_append_stream, INTERVAL '20' SECOND)
  )
  GROUP BY actor_identity
) inactivity
ON activity.actor_identity = inactivity.actor_identity