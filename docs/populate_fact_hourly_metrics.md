# Populate Warehouse Pipeline

## Overview
This job merges data from utilities.weather and utilities.electricity_use into utilities_warehouse.fact_hourly_metrics. The fact table acts as our primary warehouse table that is used for intuitive analytics querying and live dashboards.

## Workflow
There are two merge statements, one for weather and one for electricity use. For each statement, if there is an existing record for said city and hour *and* the id for the weather/electricity_use table is missing, the record will get updated to populate the id and data for the said weather/electricity_use record. If no record exists for said city and hour then a new one is inserted with the data from the weather/electricity_use table.

__Parameters__
- spark_app_name
    - defaults to 'Populate_Fact_Hourly_Metrics_Job'

## Notes
