from pyspark.sql import DataFrame
import pyspark.sql.functions as sf, yaml
from src.utils.config_loader import get_env_variable, load_config_file, spark_session


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
def load_column_config(config_file: str):
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)

    column_mappings = config.get("column_mappings", {})
    column_select = config.get("column_select", [])
    return column_mappings, column_select


# Loads the configuration file and returns argument mappings
def load_argument_config(config_file: str):
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)

    file_extract_arg_map = config.get("file_extract_arg_map", {})
    geonames_clean_arg_map = config.get("geonames_clean_arg_map", {})
    return file_extract_arg_map, geonames_clean_arg_map


def run_job(function, arguments):
    """Run job with the given arguments and initiate a spark session."""

    # Retrieve configuration based on environment
    env = get_env_variable()
    config = load_config_file(env)
    database_path_root = config.get("database_path_root", "./tmp/delta")

    # Remove spark app name to avoid passing it to our function
    spark_app_name = getattr(arguments, "spark_app_name", "Default_Spark_App")
    delattr(arguments, "spark_app_name")

    # Use the context manager to handle Spark session lifecycle
    # The Spark session will be automatically stopped after the job is done
    with spark_session(
        spark_app_name,
        {
            "spark.sql.warehouse.dir": database_path_root,
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
    ) as spark:
        function(**vars(arguments), spark=spark)


def rename_args(arg_mapping: dict, current_args) -> dict:
    """Rename arguments based on a provided mapping."""
    renamed_args = {}
    for k, v in arg_mapping.items():
        if k in vars(current_args):
            renamed_args[v] = getattr(current_args, k)
    return renamed_args
