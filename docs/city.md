# City Pipeline

## Overview
The city pipeline extracts data from geonames files and loads them into our cleaned table. The job is run ad-hoc as our geographical data does not change.

Jobs used
- `pipeline/generic_jobs/file_extract.py` 
- `pipeline/jobs/city/city_clean.py`
- `pipeline/jobs/city/city_main.py`
- `pipeline/jobs/city/city_databricks_starter.ipynb`

Relevant tables
- `utilities.city_extract`
- `utilities.city_clean`

## Workflow
### Task 1 : File Extract
This is a generic job that can process any file type into a delta table. We are using it here to extract the file contents "as is" before we begin any transformations. We typically append the results of this file to our extract table so that we know exactly what is coming in before any transformations.

__Parameters__
- input_file (required) : 
    - example : '/src/data/files/geonames.csv'
- output_table (required) :
    - example : '/src/data/test_data/city_extract'
- file_type :
    - defaults to 'csv'
- read_options
    - defaults to '{"header": "true", "delimiter": ";"}'
- write_options
    - defaults to '{"header": "true", "delta.columnMapping.mode": "name"}'
- write_mode
    - defaults to 'append'
- spark_app_name
    - defaults to 'File_Extract_Job'


### Task 2 : City Clean
This job retrieves data from a specified table and performs the following transformations:
- split coordinate column into latitude and longitude
- rename columns based on values in our configuration file
- select only the columns we'd like to keep
- write the final result to an output table

We overwrite the existing data in our cities table as this job is only ran once to populate the table.

__Parameters__
- input_table (required) : 
    - example : '/src/data/test_data/city_extract'
- output_table (required :
    - example : '/src/data/test_data/city'
- write_options
    - defaults to '{"header": "true"}'
- write_mode
    - defaults to 'overwrite'
- spark_app_name
    - defaults to 'City_Clean_Job'
  


### City Main
City Main acts as our orchestrator job to schedule both the File Extract and City Clean jobs. Parameters are split based on which job they belong to (all extract parameters begin with "extract" and all clean parameters begin with "clean"). This parameter splitting allows us to pass parameters to our jobs that may otherwise be named the same (ex: "output_table" exists in both jobs).

### City Databricks Starter
The Databricks Starter is a simple file that exists solely as an entry point for our Databricks workflow to execute our job.

## Notes
Geonames data can be downloaded or extracted via API at [public.opendatasoft.com](https://public.opendatasoft.com/explore/dataset/geonames-all-cities-with-a-population-1000/table/?disjunctive.cou_name_en&sort=name).
