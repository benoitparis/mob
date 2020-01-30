INSERT INTO debug_sink
SELECT
  COALESCE(CAST('acti and inact: ' AS VARCHAR), '')                                                          || ' - ' ||
  COALESCE(CAST(tsActivity AS VARCHAR), '')                                                                  || ' - ' ||
  COALESCE(CAST(clicks AS VARCHAR), '')                                                                      || ' - ' ||
  COALESCE(CAST(sEnd AS VARCHAR), '')                                                                        || ' - ' ||
  COALESCE(CAST(CAST(sEnd AS TIMESTAMP) AS VARCHAR), '')                                                     || ' - ' ||
  COALESCE(CAST('' AS VARCHAR), '')                                                                          || ' - ' ||
  COALESCE(CAST((sEnd IS NULL OR (CAST(tsActivity AS TIMESTAMP) > CAST(sEnd AS TIMESTAMP))) AS VARCHAR), '') || ' - ' 
FROM (
  SELECT
    activity.loopback_index,
    activity.actor_identity,
    activity.tsActivity,
    inactivity.clicks,
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
      LAST_VALUE(clicks) AS clicks,
      LAST_VALUE(CAST(sEnd AS VARCHAR)) AS sEnd
    FROM (
      SELECT
        loopback_index,
        actor_identity,
        count(*) clicks,
        SESSION_END(write_state.proctime_append_stream, INTERVAL '10' SECOND) sEnd
      FROM write_state
      GROUP BY loopback_index, actor_identity, SESSION(write_state.proctime_append_stream, INTERVAL '10' SECOND)
    )
    GROUP BY loopback_index, actor_identity
  ) inactivity
  ON activity.actor_identity = inactivity.actor_identity
)

UNION

SELECT
  COALESCE(CAST('sessions' AS VARCHAR), '')                                                            || ' - ' ||
  COALESCE(CAST(loopback_index AS VARCHAR), '')                                                        || ' - ' ||
  COALESCE(CAST(actor_identity AS VARCHAR), '')                                                        || ' - ' ||
  COALESCE(CAST(count(*) AS VARCHAR), '')                                                              || ' - ' ||
  COALESCE(CAST(SESSION_END(write_state.proctime_append_stream, INTERVAL '10' SECOND) AS VARCHAR), '') || ' - ' 
FROM write_state
GROUP BY loopback_index, actor_identity, SESSION(write_state.proctime_append_stream, INTERVAL '10' SECOND)
