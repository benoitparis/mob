
INSERT INTO outputTable 
SELECT 
  ROW(col1, col2)
FROM (
  SELECT 
    col1,
    ROW(CAST(col2.colA AS VARCHAR), CAST(col2.colB AS VARCHAR)) as col2
  FROM inputTable
)

-- talk nicely:
-- 'payload.' ~ '' ?
-- nesting
-- ROW arity > 2
