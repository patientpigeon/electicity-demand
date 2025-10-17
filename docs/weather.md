# Weather Pipeline

## Overview
The weather pipeline extracts data from a weather API and loads it into our cleaned table. The job is run every 5 days due to some limitations with the API (see Notes for more).

Jobs used
- `pipeline/jobs/weather/weather_extract.py` 
- `pipeline/jobs/weather/weather_clean.py`
- `pipeline/jobs/weather/weather_main.py`
- `pipeline/jobs/weather/weather_databricks_starter.ipynb`

Relevant tables
- `utilities.weather_extract`
- `utilities.weather_clean`

## Workflow
### Task 1 : Weather Extract
This job extracts relevant data from the weather API and loads it "as is" in our extract table. We typically append the results of the API call to our extract table so that we know exactly what is coming in before any transformations.

__Parameters__
- city (required) : 
    - example : 'Portland'
- start_date : the beginning date we want to retrieve data from via the API call
    - example : "2025-10-01"
    - defaults to the current data minus 7 days
- end_date : the end date we want to retrieve data from via the API cal
    - example : "2025-11-01"
    - defaults to yesterday
- spark_app_name
    - defaults to 'Weather_Extract_Job'


### Task 2 : Weather Clean
This job retrieves data from a specified table and performs the following transformations:
- convert data types of fields specified in a configuration file
- ensure that the data we are about to put in the output table is in fact new
  - this is achieved by a left anti join to filter out any records that already exist in the output table
- append the result to an output table

__Parameters__
- input_table (required) : 
    - example : '/src/data/test_data/weather_extract'
- output_table (required :
    - example : '/src/data/test_data/weather'
- write_options
    - defaults to '{"header": "true"}'
- write_mode
    - defaults to 'append'
- spark_app_name
    - defaults to 'Weather_Clean_Job'
  


### Weather Main
Weather Main acts as our orchestrator job to schedule both the Weather Extract and Weather Clean jobs. Parameters are split based on which job they belong to (all extract parameters begin with "extract" and all clean parameters begin with "clean"). This parameter splitting allows us to pass parameters to our jobs that may otherwise be named the same (ex: "output_table" exists in both jobs).

### Weather Databricks Starter
The Databricks Starter is a simple file that exists solely as an entry point for our Databricks workflow to execute our job.

## Notes
- Weather data can be queried via API at [WeatherAPI.com](https://www.weatherapi.com).
- There are two crucial API limitations
  - We can only retrieve history up to 7 days ago
  - If we query the history of today, the entirety of today is retrieved, including forecasting for the remaining hours of the day
    - To ensure data quality we do not query the API for today but rather yesterday
