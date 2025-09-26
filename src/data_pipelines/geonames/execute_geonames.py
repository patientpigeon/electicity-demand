# use this file to execute the geonames data pipeline
# this will call the extract, transform, and load scripts in order
# you can run this file from the command line using:
# python -m python -m src.data_pipelines.geonames.execute_geonames

# import yaml
# with open("/dbfs/FileStore/config/geonames.yaml") as f:
#     config = yaml.safe_load(f)

# print(config["data_dir"])

# pass the spark session to each script
