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
    pass


# Extracts a file and loads it as a Delta table
def extract_file(
    file_path: str,
    data_destination: str,
    file_type: str = None,
    read_options: dict = None,
    write_options: dict = None,
    write_mode: str = None,
    spark=None,
):
    # Set default options if none are provided
    if file_type is None:
        file_type = "csv"
    if read_options is None:
        read_options = {"header": "true", "delimiter": ";"}
    if write_options is None:
        write_options = {"header": "true", "delta.columnMapping.mode": "name"}
    if write_mode is None:
        write_mode = "append"

    # Call the appropriate extract function based on file type
    if file_type == "csv":
        extract_csv(file_path, data_destination, read_options, write_options, write_mode, spark)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")


# Extracts a CSV file and loads it as a Delta table
def extract_csv(
    csv_path: str,
    data_destination: str,
    read_options: dict = None,
    write_options: dict = None,
    write_mode: str = None,
    spark=None,
):
    # read the CSV file into a DataFrame
    csv_df = spark.read.options(**read_options).csv(csv_path)

    # Save the DataFrame to the specified path
    csv_df.write.format("delta").options(**write_options).mode(write_mode).save(data_destination)
