INSERT INTO click_count
SELECT
  client_id,
  ROW (
    count_value
  )
FROM click_count_retract
WHERE accumulate_flag