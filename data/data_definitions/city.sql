
CREATE TABLE utilities_workspace.utilities.city_extract (
  `Geoname ID` STRING,
  Name STRING,
  `ASCII Name` STRING,
  `Alternate Names` STRING,
  `Feature Class` STRING,
  `Feature Code` STRING,
  `Country Code` STRING,
  `Country name EN` STRING,
  `Country Code 2` STRING,
  `Admin1 Code` STRING,
  `Admin2 Code` STRING,
  `Admin3 Code` STRING,
  `Admin4 Code` STRING,
  Population STRING,
  Elevation STRING,
  `DIgital Elevation Model` STRING,
  Timezone STRING,
  `Modification date` STRING,
  `LABEL EN` STRING,
  Coordinates STRING)
USING delta
LOCATION 'utilities/city_extract'
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');



CREATE TABLE utilities_workspace.utilities.city (
  city STRING NOT NULL,
  alternate_city_names STRING,
  country STRING,
  admin_code_1 STRING,
  admin_code_2 STRING,
  admin_code_3 STRING,
  admin_code_4 STRING,
  population STRING,
  elevation STRING,
  timezone STRING,
  longitude STRING,
  latitude STRING,
  CONSTRAINT `pk_city` PRIMARY KEY (`city`))
USING delta
LOCATION 'utilities/city'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7');