SELECT
  actor_identity,
  LAST_VALUE(payload.side) AS side
FROM choose_side
GROUP BY actor_identity