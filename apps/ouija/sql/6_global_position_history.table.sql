SELECT
  CAST(COALESCE('1', null) AS VARCHAR) dummy_key, -- There's a JIRA for that
  MAX(lp.proctime_append_stream) max_proctime,
  -- 0.003 / 0.00001 = 300 is the middle of the screen. Default value when there are no players.
  CAST(SUM(lp.x) / (0.00001 + SUM(1)) AS DOUBLE) ballX,
  CAST(SUM(lp.y) / (0.00001 + SUM(1)) AS DOUBLE) ballY
FROM (
  SELECT *
  FROM (
    SELECT
      actor_identity,
      MAX(proctime_append_stream) AS proctime_append_stream,
      LAST_VALUE(y) AS y,
      LAST_VALUE(x) AS x
    FROM write_position
    GROUP BY actor_identity
  ) last_position
) AS lp