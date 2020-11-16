/* implicit subscription */
CREATE VIEW user_count_v AS
SELECT
  choose_side.client_id,
  countLeft   ,
  countObserve,
  countRight  ,
  countGlobal
FROM choose_side
JOIN (
  SELECT
    SUM(CASE WHEN side = 'left'    THEN 1 ELSE 0 END) AS countLeft,
    SUM(CASE WHEN side = 'observe' THEN 1 ELSE 0 END) AS countObserve,
    SUM(CASE WHEN side = 'right'   THEN 1 ELSE 0 END) AS countRight,
    SUM(1) AS countGlobal
  FROM user_side
  JOIN user_activity ua1 ON user_side.client_id = ua1.client_id
  WHERE ua1.active
) ON true
JOIN user_activity ua2 ON choose_side.client_id = ua2.client_id
WHERE ua2.active
