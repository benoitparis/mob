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
    COUNT(DISTINCT CASE WHEN payload.side = 'left'  THEN actor_identity ELSE 'none' END) - 1 AS countLeft,
    COUNT(DISTINCT CASE WHEN payload.side = 'right' THEN actor_identity ELSE 'none' END) - 1 AS countRight,
    COUNT(DISTINCT actor_identity) AS countGlobal
  FROM choose_side
) ON 1 = 1
/* faudra mettre du last_value dans tout ça */
