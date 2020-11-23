INSERT INTO services.debug
SELECT COALESCE(CAST('simple time manipulations: '
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  tick_number                                    --
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  constant_dummy_source                          --
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  proctime_append_stream                         --
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  ''--PROCTIME()                                 -- Always null (even when COALESCEd). Magical.
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  PROCTIME_MATERIALIZE(PROCTIME())               --
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  PROCTIME_MATERIALIZE(proctime_append_stream)   --
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  ''                                             --
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  ''                                             --
  AS VARCHAR), '') || ' - ' || COALESCE(CAST(  ''                                             --
  AS VARCHAR), '')
FROM services.tick
