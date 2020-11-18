CREATE TEMPORAL TABLE FUNCTION global_position_temporal TIME ATTRIBUTE max_proctime PRIMARY KEY dummy_key AS
SELECT * 
FROM global_position_history