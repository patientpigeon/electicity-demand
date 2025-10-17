from pyspark.sql import DataFrame
import yaml
from datetime import datetime, timedelta


def default_start_date(days_ago=7):
    """Returns a string for the date N days ago (default 7), in YYYY-MM-DD format."""
    return (datetime.today() - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def default_end_date():
    """Returns today's date minus 1 day as a string in YYYY-MM-DD format."""
    return (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def extract_csv(
    csv_path: str,
    data_destination: str,
    read_options: dict = None,
    write_options: dict = None,
    write_mode: str = None,
    spark=None,
):
    """Extracts a CSV file and loads it as a Delta table.
    Args:
        csv_path (str): Path to the input CSV file.
        data_destination (str): Path to the output Delta table.
        read_options (dict, optional): Options for reading the CSV file.
        write_options (dict, optional): Options for writing the Delta table.
        write_mode (str, optional): Write mode for the Delta table.
        spark (SparkSession, optional): Spark session to use.

    Returns: None
    """
    # read the CSV file into a DataFrame
    csv_df = spark.read.options(**read_options).csv(csv_path)

    # Save the DataFrame to the specified path
    csv_df.write.format("delta").options(**write_options).mode(write_mode).save(data_destination)


def extract_file(
    file_path: str,
    data_destination: str,
    file_type: str = None,
    read_options: dict = None,
    write_options: dict = None,
    write_mode: str = None,
    spark=None,
):
    """Extracts a file and loads it as a Delta table.
    Args:
        file_path (str): Path to the input file.
        data_destination (str): Path to the output Delta table.
        file_type (str, optional): Type of the input file (e.g., 'csv').
        read_options (dict, optional): Options for reading the input file.
        write_options (dict, optional): Options for writing the Delta table.
        write_mode (str, optional): Write mode for the Delta table.
        spark (SparkSession, optional): Spark session to use.

    Returns: None
    """
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


def get_nested_dict_values(api_resp_dict: dict, path: list) -> any:
    """
    Extract nested values from dictionary using a list of keys with a default fallback.
    Keys will be all but the last element, default will be the last element
    Example: path = ['powerConsumptionBreakdown', 'nuclear', 0]

    Args:
        api_resp_dict (dict): The dictionary to extract values from.
        path (list): List of keys leading to the desired value, with the last element as default.
    Returns:
        any: The extracted value or the default if any key is missing.
    """
    *keys, default = path
    api_resp_val = api_resp_dict

    # each loop through the keys digs one level deeper into the dictionary
    for k in keys:
        api_resp_val = api_resp_val.get(k, default)
        # prevent NoneType error if value is None
        if api_resp_val is None:
            return default
    return api_resp_val


def load_config_keys(config_file: str, *keys) -> tuple:
    """
    Load specified keys from a YAML config file.

    Args:
        config_file (str): Path to the YAML config file.
        *keys: Keys to extract from the config.

    Returns:
        tuple: Values for each requested key, in order.
    """
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)
    return tuple(config.get(key) for key in keys)


def rename_columns(df: DataFrame, column_mappings: dict) -> DataFrame:
    """Rename columns in a DataFrame based on a provided mapping.
    Args:
        df (DataFrame): Input DataFrame.
        column_mappings (dict): Dictionary mapping old column names to new column names.

    Returns:
        DataFrame: DataFrame with renamed columns.
    """
    for old_name, new_name in column_mappings.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
        else:
            pass  # log a warning here
    return df
