# City Pipeline

## Overview
The city pipeline extracts data from geonames files and loads them into our cleaned table.

Jobs used
- `src/utils/common_jobs/file_extract.py` 
- `src/data_pipelines/city/city_clean.py`.

Relevant tables
- `city_extract`
- `city_clean`

## Workflow
### Task 1 : File Extract

This is a generic job that can process any file type into a delta table. We are using it here to extract the file contents "as is" before we begin any transformations.

Parameters.
- input_file (required) : 
    - example : 
- output_table (required) :
    - example : 


### Task 2 : City Clean



## Notes
Geonames data can be downloaded or extracted via API at [public.opendatasoft.com](https://public.opendatasoft.com/explore/dataset/geonames-all-cities-with-a-population-1000/table/?disjunctive.cou_name_en&sort=name).