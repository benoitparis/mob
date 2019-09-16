INSERT INTO racket_positions
SELECT
  qgp.loopback_index,                                         
  qgp.actor_identity,
  ROW(
    leftY,
    rightY
  )
FROM query_global_position qgp
JOIN LATERAL TABLE (global_position(qgp.proctime)) AS gp 
  ON qgp.payload.side = gp.side
