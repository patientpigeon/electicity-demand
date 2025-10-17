
CREATE TABLE utilities_workspace.utilities.weather_extract (
  city STRING,
  date_time STRING,
  temp_c DOUBLE,
  temp_f DOUBLE,
  condition STRING,
  precip_mm DOUBLE,
  snow_cm DOUBLE,
  humidity BIGINT,
  cloud BIGINT,
  feelslike_c DOUBLE,
  windchill_c DOUBLE,
  heatindex_c DOUBLE,
  dewpoint_c DOUBLE,
  will_it_rain BIGINT,
  chance_of_rain BIGINT,
  will_it_snow BIGINT,
  chance_of_snow BIGINT,
  wind_dir STRING,
  pressure_mb DOUBLE,
  wind_kph DOUBLE,
  vis_km DOUBLE,
  gust_kph DOUBLE,
  uv DOUBLE,
  is_day BIGINT,
  sunrise STRING,
  sunset STRING)
USING delta
LOCATION 'utilities/weather_extract'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');



CREATE TABLE utilities_workspace.utilities.weather (
  id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  city STRING NOT NULL,
  date_time TIMESTAMP NOT NULL,
  temp_c DOUBLE,
  temp_f DOUBLE,
  condition STRING,
  precip_mm DOUBLE,
  snow_cm DOUBLE,
  humidity INT,
  cloud INT,
  feelslike_c DOUBLE,
  windchill_c DOUBLE,
  heatindex_c DOUBLE,
  dewpoint_c DOUBLE,
  will_it_rain INT,
  chance_of_rain INT,
  will_it_snow INT,
  chance_of_snow INT,
  wind_dir STRING,
  pressure_mb DOUBLE,
  wind_kph DOUBLE,
  vis_km DOUBLE,
  gust_kph DOUBLE,
  uv DOUBLE,
  is_day INT,
  sunrise STRING,
  sunset STRING,
  CONSTRAINT `pk_weather` PRIMARY KEY (`city`, `date_time`),
  CONSTRAINT `fk_weather` FOREIGN KEY (`city`) REFERENCES `utilities_workspace`.`utilities`.`city` (`city`))
USING delta
LOCATION 'utilities/weather'
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.identityColumns' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');