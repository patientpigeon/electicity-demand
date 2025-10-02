from pyspark.sql import SparkSession
from pipeline.utils.shared_helpers import read_api
from dotenv import load_dotenv
import os
from pipeline.utils.config_loader import get_env_variable, load_config_file, spark_session


# def extract_weather_data():
#     spark = SparkSession.builder.getOrCreate()
#     api_url = "https://api.weather.com/data"
#     df = read_api(api_url)  # Shared function to read API data
#     save_as_table(df, "raw_weather")  # Shared function to save as a table
#     return df


# if __name__ == "__main__":
#     extract_weather_data()


import pyspark
from delta import *

builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.debug.maxToStringFields", "1000")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


env = get_env_variable()
config_params = load_config_file(env)
database_path_root = config_params.get("database_path_root", "./tmp/delta")

load_dotenv()

weather_api_key = os.getenv("WEATHER_API_KEY")
