CREATE VIEW mobcatalog.pong.global_position_history AS
SELECT
  CAST(COALESCE('1', null) AS VARCHAR) dummy_key, -- There's a JIRA for that
  MAX(ly.ts) max_proctime,
  -- 0.003 / 0.00001 = 300 is the middle of the screen. Default value when there are no players.
  CAST(SUM(CASE WHEN us.side = 'left'  THEN ly.y ELSE 0.003 END) / (0.00001 + SUM(CASE WHEN us.side = 'left'  THEN 1 ELSE 0 END)) AS DOUBLE) leftY,
  CAST(SUM(CASE WHEN us.side = 'right' THEN ly.y ELSE 0.003 END) / (0.00001 + SUM(CASE WHEN us.side = 'right' THEN 1 ELSE 0 END)) AS DOUBLE) rightY
FROM (
  SELECT *
  FROM user_active_v ua
  JOIN (
    SELECT
      client_id,
      MAX(ts) AS ts,
      LAST_VALUE(y) AS y
    FROM write_y
    GROUP BY client_id
  ) last_y ON ua.client_id = last_y.client_id
) AS ly
JOIN user_side AS us 
  ON ly.client_id = us.client_id