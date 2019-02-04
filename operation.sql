
INSERT INTO outputTable 
SELECT 
  ROW(loopback_index, actor_identity, table_name, key_value)
FROM inputTable

-- talk nicely:
-- 'payload.' ~ '' ?
-- nesting
-- ROW arity > 2
