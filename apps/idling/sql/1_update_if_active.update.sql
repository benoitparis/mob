INSERT INTO send_client
SELECT
  sess.loopback_index,                                         
  sess.actor_identity,
  ROW(
    COALESCE(CAST(tick_number AS VARCHAR), '')
  )
FROM (
  SELECT
    LAST_VALUE(tick_number) tick_number
  FROM services.tick
) ts
JOIN (
  SELECT
    activity.loopback_index,
    activity.actor_identity,
    activity.tsActivity,
    inactivity.sEnd
  FROM (
    SELECT
      loopback_index,
      actor_identity,
      LAST_VALUE(CAST(proctime_append_stream AS VARCHAR)) AS tsActivity
    FROM write_state
    GROUP BY loopback_index, actor_identity
  ) AS activity
  LEFT JOIN
  (
    SELECT
      loopback_index,
      actor_identity,
      LAST_VALUE(CAST(sEnd AS VARCHAR)) AS sEnd
    FROM (
      SELECT
        loopback_index,
        actor_identity,
        SESSION_END(write_state.proctime_append_stream, INTERVAL '10' SECOND) sEnd
      FROM write_state
      GROUP BY loopback_index, actor_identity, SESSION(write_state.proctime_append_stream, INTERVAL '10' SECOND)
    )
    GROUP BY loopback_index, actor_identity
  ) inactivity
  ON activity.actor_identity = inactivity.actor_identity
) sess
ON true
WHERE true
  AND MOD(tick_number, 10) = 0
  AND (sess.sEnd IS NULL OR (CAST(tsActivity AS TIMESTAMP) > CAST(sEnd AS TIMESTAMP)))