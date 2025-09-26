from pyspark.sql import DataFrame
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()


def read_api(api_url: str) -> DataFrame:
    pass


def save_as_table(df: DataFrame, table_name: str):
    # df.write.mode("overwrite").saveAsTable(table_name)
    pass


def load_table(table_name: str) -> DataFrame:
    # return spark.read.table(table_name)
    pass


def clean_weather_data(df: DataFrame) -> DataFrame:
    pass


def aggregate_weather_data(df: DataFrame) -> DataFrame:
    pass


def test():
    print("test")


# extracts a file and loads it into a parquet format
# currently only supports csv files with headers
def extract_csv(file_path: str, data_path: str, spark, options: dict = None):
    # Retrieve the base path from environment variables and construct full paths for ingestion and saving
    file_base_path = os.getenv("FILE_ROOT_PATH")

    full_file_path = os.path.join(file_base_path, file_path)

    file_df = spark.read.options(**options).csv(full_file_path)

    # Save the DataFrame in Parquet format to the specified path
    file_df.write.saveAsTable(data_path, mode="overwrite", partitionBy=None)
