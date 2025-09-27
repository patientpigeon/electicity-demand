# This file allows us to run Spark jobs in different environments (e.g., local, dev, prod)
# by loading the appropriate configuration settings based on an environment variable.
# To set up in a new environment:
# 1. Change the APP_ENV variable in the .env file.
#       Example: "APP_ENV="local"
# 2. Create a corresponding config file in the config directory (e.g., config/config.dev.py, config/config.prod.py)
#       Within the config file, set a database_root_path variable to point to the desired delta location
# 3. Ensure the config file has a create_config function so you can run it and write the config to an INI file.
#
#
# Example config file content (config.local.py):
# import configparser
#
#
# def create_config():
#     config = configparser.ConfigParser()
#
#     config["DEFAULT"] = {
#         "database_root_path": "/Users/path/to/repo/electicity-demand/data/test_data",
#     }

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from src.utils import shared_helpers as sh
import configparser
import pyspark
from delta import *
import os

# Load environment variables from the .env file
load_dotenv()

# Retrieve environment variable to determine config file
env = os.getenv("APP_ENV")
config_file = f"config/config.{env}.ini"

# Load configuration from the specified INI file
config = configparser.ConfigParser()
config.read(config_file)

# Initialize Spark session with the specified warehouse directory
database_root_path = config.get("DEFAULT", "database_root_path")

builder = (
    pyspark.sql.SparkSession.builder.appName("data_pipelines")
    .config("spark.sql.warehouse.dir", database_root_path)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
