CREATE TEMPORAL TABLE racket_last_positions TIME ATTRIBUTE last_write_time PRIMARY KEY player_identity AS
SELECT
  wy.proctime last_write_time,
  wy.actor_identity player_identity,
  us.side,
  y position
FROM write_y wy
JOIN LATERAL TABLE (user_side(wy.proctime)) AS us ON us.actor_identity = wy.actor_identity
