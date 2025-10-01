from pyspark.sql import DataFrame
import pyspark.sql.functions as sf, yaml


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


# Renames columns in a DataFrame based on a provided mapping
def rename_columns(df: DataFrame, column_mappings: dict) -> DataFrame:
    for old_name, new_name in column_mappings.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
        else:
            pass  # log a warning here
    return df


# Splits a coordinate column into latitude and longitude
def split_coordinates(df: DataFrame, coord_column: str = "coordinates") -> DataFrame:
    updated_df = (
        df.withColumn("longitude", sf.substring_index(df[coord_column], ",", 1))
        .withColumn("latitude", sf.substring_index(df[coord_column], ",", -1))
        .drop(coord_column)
    )
    return updated_df


# Selects specific columns from a DataFrame
def select_columns(df: DataFrame, columns_to_select: list) -> DataFrame:
    return df.select(*columns_to_select)


# Loads the configuration file and returns column mappings and selected columns
def load_config(config_file: str):
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)

    column_mappings = config.get("column_mappings", {})
    column_select = config.get("column_select", [])
    return column_mappings, column_select
