SELECT
  CAST(UNIX_TIMESTAMP() AS VARCHAR) || ' ' || message
FROM post_message
