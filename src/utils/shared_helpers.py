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
