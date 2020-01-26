/* implicit subscription style? */
INSERT INTO user_count
SELECT
  loopback_index,
  actor_identity,
  ROW(
    CAST(countLeft AS VARCHAR),
    CAST(countRight AS VARCHAR),
    CAST(countGlobal AS VARCHAR)
  )
FROM choose_side
JOIN (
  SELECT
    SUM(CASE WHEN side = 'left'  THEN 1 ELSE 0 END) AS countLeft,
    SUM(CASE WHEN side = 'right' THEN 1 ELSE 0 END) AS countRight,
    SUM(1) AS countGlobal
  FROM user_side
) ON true
/* faudra mettre du last_value dans tout �a */
/* et du WITH DELAY p */
  