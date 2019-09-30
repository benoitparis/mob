INSERT INTO racket_positions
SELECT
  qgp.loopback_index,                                         
  qgp.actor_identity,
  ROW(
    CAST(leftY AS DECIMAL(38, 18)),
    CAST(rightY AS DECIMAL(38, 18))
  )
FROM query_global_position_flat AS qgp
   , LATERAL TABLE (global_position(qgp.proctime)) AS gp 
WHERE qgp.constant_dummy = gp.constant_dummy
