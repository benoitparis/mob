
INSERT INTO outputTable 
SELECT 
  loopback_index, actor_identity, payload
FROM (
  SELECT
    inputTable.*,
    ROW(table_name, key_value) payload
  FROM inputTable
)

-- talk nicely:
-- 'payload.' ~ '' ?
-- nesting
-- ROW arity > 2
