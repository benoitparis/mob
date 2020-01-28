INSERT INTO debug_sink
SELECT content.debug_message
FROM aggregated_time_manipulations_retract
WHERE flag = true