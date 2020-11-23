INSERT INTO services.debug
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
    activity.client_id,
    activity.tsActivity,
    inactivity.clicks,
    inactivity.sEnd
  FROM (
    SELECT
      client_id,
      LAST_VALUE(CAST(ts AS VARCHAR)) AS tsActivity
    FROM write_state
    GROUP BY client_id
  ) AS activity
  LEFT JOIN
  (
    SELECT
      client_id,
      LAST_VALUE(clicks) AS clicks,
      LAST_VALUE(CAST(sEnd AS VARCHAR)) AS sEnd
    FROM (
      SELECT
        client_id,
        count(*) clicks,
        SESSION_END(write_state.ts, INTERVAL '10' SECOND) sEnd
      FROM write_state
      GROUP BY client_id, SESSION(write_state.ts, INTERVAL '10' SECOND)
    )
    GROUP BY client_id
  ) inactivity
  ON activity.client_id = inactivity.client_id
)

UNION

SELECT
  COALESCE(CAST('sessions' AS VARCHAR), '')                                                            || ' - ' ||
  COALESCE(CAST(client_id AS VARCHAR), '')                                                             || ' - ' ||
  COALESCE(CAST(count(*) AS VARCHAR), '')                                                              || ' - ' ||
  COALESCE(CAST(SESSION_END(write_state.ts, INTERVAL '10' SECOND) AS VARCHAR), '') || ' - ' 
FROM write_state
GROUP BY client_id, SESSION(write_state.ts, INTERVAL '10' SECOND)
