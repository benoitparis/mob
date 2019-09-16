CREATE TEMPORAL TABLE global_position TIME ATTRIBUTE start_time PRIMARY KEY side AS
SELECT                                                                              
  us.side AS side,
  HOP_START(proctime, INTERVAL '0.05' SECOND, INTERVAL '2.0' SECOND) start_time,
  SUM(CASE WHEN us.side = 'left'  THEN wy.y ELSE 0 END) / (0.01 + SUM(CASE WHEN us.side = 'left'  THEN 1 ELSE 0 END)) leftY,
  SUM(CASE WHEN us.side = 'right' THEN wy.y ELSE 0 END) / (0.01 + SUM(CASE WHEN us.side = 'right' THEN 1 ELSE 0 END)) rightY
FROM write_y wy
JOIN LATERAL TABLE (user_side(wy.proctime)) AS us ON us.actor_identity = wy.actor_identity
GROUP BY us.side, HOP(proctime, INTERVAL '0.05' SECOND, INTERVAL '2.0' SECOND)
