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
