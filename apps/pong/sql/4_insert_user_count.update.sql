/* implicit subscription */
INSERT INTO user_count
SELECT
  choose_side.loopback_index,
  choose_side.actor_identity,
  ROW(
    CAST(countLeft AS VARCHAR),
    CAST(countObserve AS VARCHAR),
    CAST(countRight AS VARCHAR),
    CAST(countGlobal AS VARCHAR)
  )
FROM choose_side
JOIN (
  SELECT
    SUM(CASE WHEN side = 'left'    THEN 1 ELSE 0 END) AS countLeft,
    SUM(CASE WHEN side = 'observe' THEN 1 ELSE 0 END) AS countObserve,
    SUM(CASE WHEN side = 'right'   THEN 1 ELSE 0 END) AS countRight,
    SUM(1) AS countGlobal
  FROM user_side
  JOIN user_activity ua1 ON user_side.actor_identity = ua1.actor_identity
  WHERE ua1.active
) ON true
JOIN user_activity ua2 ON choose_side.actor_identity = ua2.actor_identity
WHERE ua2.active
