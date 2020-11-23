INSERT INTO user_count
SELECT
  content.client_id,
  ROW(
    CAST(content.countLeft AS VARCHAR),
    CAST(content.countObserve AS VARCHAR),
    CAST(content.countRight AS VARCHAR),
    CAST(content.countGlobal AS VARCHAR)
  )
FROM user_count_retract
WHERE accumulate_flag