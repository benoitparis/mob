CREATE VIEW user_side AS
SELECT
  client_id,
  LAST_VALUE(payload.side) AS side
FROM choose_side
GROUP BY client_id