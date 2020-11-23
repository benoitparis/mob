INSERT INTO services.debug
SELECT
  COALESCE(CAST(accumulate_flag AS VARCHAR), '') || ' - ' ||
  COALESCE(CAST(client_id AS VARCHAR), '')                                     
FROM idling_computation_retract
