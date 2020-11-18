/* implicit subscription */
CREATE VIEW user_count_v AS
SELECT
  ua.client_id,
  countLeft   ,
  countObserve,
  countRight  ,
  countGlobal
FROM user_active_v ua
JOIN (
  SELECT
    SUM(CASE WHEN side = 'left'    THEN 1 ELSE 0 END) AS countLeft,
    SUM(CASE WHEN side = 'observe' THEN 1 ELSE 0 END) AS countObserve,
    SUM(CASE WHEN side = 'right'   THEN 1 ELSE 0 END) AS countRight,
    SUM(1) AS countGlobal
  FROM user_side
) ON true
