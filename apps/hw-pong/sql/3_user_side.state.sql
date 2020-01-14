CREATE TEMPORAL TABLE FUNCTION user_side TIME ATTRIBUTE chosen_at PRIMARY KEY actor_identity AS
SELECT
  actor_identity,
  proctime_append_stream chosen_at,
  payload.side AS side
FROM choose_side
