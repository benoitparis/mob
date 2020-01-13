-- Pas vraiment un ack sur le user_side state, mais au moins un passage par Flink
INSERT INTO ack_side
SELECT
  loopback_index,
  actor_identity,
  ROW(side)
FROM choose_side
