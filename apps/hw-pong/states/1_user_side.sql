CREATE TEMPORAL TABLE user_side TIME ATTRIBUTE chosen_at PRIMARY KEY actor_identity AS
SELECT
  actor_identity,
  proctime chosen_at,
  payload.side
FROM choose_side
