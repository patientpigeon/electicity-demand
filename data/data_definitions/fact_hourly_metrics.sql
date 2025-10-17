CREATE TABLE utilities_workspace.utilities_warehouse.fact_hourly_metrics (
  city STRING,
  weather_id BIGINT,
  electricity_use_id BIGINT,
  date_time TIMESTAMP,
  temp_c DOUBLE,
  temp_f DOUBLE,
  precip_mm DOUBLE,
  power_consumption_total INT)
USING delta
LOCATION 'utilities_warehouse/fact_hourly_metrics'
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');