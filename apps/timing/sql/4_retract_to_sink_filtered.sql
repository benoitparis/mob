INSERT INTO services.debug
SELECT content.debug_message
FROM aggregated_time_manipulations_retract
WHERE accumulate_flag