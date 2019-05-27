CREATE TEMPORAL TABLE global_position TIME ATTRIBUTE start_time PRIMARY KEY one_key AS
SELECT                                                                              
  loopback_index one_key,
  HOP_START(proctime, INTERVAL '0.05' SECOND, INTERVAL '5.0' SECOND) start_time, 
  AVG(X) X,
  AVG(Y) Y
FROM write_position                                                                
GROUP BY loopback_index, HOP(proctime, INTERVAL '0.05' SECOND, INTERVAL '5.0' SECOND)
