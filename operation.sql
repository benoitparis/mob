
INSERT INTO outputTable 
SELECT 
  ROW(table_name, key_value)
FROM inputTable

-- talk nicely:
-- 'payload.' ~ '' ?
-- nesting
-- ROW arity > 2
