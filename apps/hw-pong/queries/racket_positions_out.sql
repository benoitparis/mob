-- Pas vraiment un ack sur le user_side state, mais au moins un passage par Flink
INSERT INTO racket_positions
SELECT
  wy.loopback_index,                                         
  wy.actor_identity,
  ROW(
    CASE WHEN us.side = 'left'  THEN wy.y ELSE 0 END,
    CASE WHEN us.side = 'right' THEN wy.y ELSE 0 END
  )
  /*
  leftY
  y
  loopback_index,
  actor_identity,
  user_side.side
  */
FROM write_y wy
JOIN LATERAL TABLE (user_side(wy.proctime)) AS us ON us.actor_identity = wy.actor_identity
