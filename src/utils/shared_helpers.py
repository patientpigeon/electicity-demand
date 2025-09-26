from pyspark.sql import DataFrame


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


# extracts a csv file and loads it into a parquet format
def extract_csv(file_path: str, data_destination: str, spark, options: dict = None):
    if options is None:
        options = {"header": "true", "delimiter": ";"}

    # Read the CSV file into a DataFrame
    file_df = spark.read.options(**options).csv(file_path)

    # Save the DataFrame to the specified path
    file_df.write.format("delta").option("delta.columnMapping.mode", "name").mode("overwrite").save(data_destination)
