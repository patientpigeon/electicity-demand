# Electricity Use Pipeline

## Overview
The electricity use pipeline extracts data from an API and loads it into our cleaned table. The job is run every hour to stay up to date as well as due to us not being able to query anything but the current stats.

Jobs used
- `pipeline/jobs/electricity_use/electricity_use_extract.py` 
- `pipeline/jobs/electricity_use/electricity_use_clean.py`
- `pipeline/jobs/electricity_use/electricity_use_main.py`
- `pipeline/jobs/electricity_use/electricity_use_databricks_starter.ipynb`

Relevant tables
- `utilities.electricity_use_extract`
- `utilities.electricity_use`

## Workflow
### Task 1 : Electricity Use Extract
This job extracts relevant data from the electricity maps API and loads it "as is" in our extract table. We typically append the results of the API call to our extract table so that we know exactly what is coming in before any transformations.

__Parameters__
- zone : the electricity zone you wish to query the API for
    - example : "US-NW-PGE"
    - defaults to "US-NW-PGE" (Portland General Electric)
- spark_app_name
    - defaults to 'Electricity_Use_Extract_Job'


### Task 2 : Electricity Use Clean
This job retrieves data from a specified table and performs the following transformations:
- adds a city column so that the table can join with our city data
- convert data types of fields specified in a configuration file
- ensure that the data we are about to put in the output table is in fact new
  - this is achieved by a left anti join to filter out any records that already exist in the output table
- append the result to an output table

__Parameters__
- city (required) : must relate to the utilities.city table
  - example : 'Portland'
- input_table (required) : 
    - example : '/src/data/test_data/electricity_use_extract'
- output_table (required :
    - example : '/src/data/test_data/electricity_use'
- write_options
    - defaults to '{"header": "true"}'
- write_mode
    - defaults to 'append'
- spark_app_name
    - defaults to 'Electricity_Use_Clean_Job'
  


### Electricity Use Main
Electricity Use Main acts as our orchestrator job to schedule both the Electricity Use Extract and Electricity Use Clean jobs. Parameters are split based on which job they belong to (all extract parameters begin with "extract" and all clean parameters begin with "clean"). This parameter splitting allows us to pass parameters to our jobs that may otherwise be named the same (ex: "output_table" exists in both jobs).

### Electricity Use Databricks Starter
The Databricks Starter is a simple file that exists solely as an entry point for our Databricks workflow to execute our job.

## Notes
- Electricity Use data can be queried via API at [ElectricityMaps.com](https://app.electricitymaps.com).
