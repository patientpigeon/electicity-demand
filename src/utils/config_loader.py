# This file allows us to run Spark jobs in different environments (e.g., local, dev, prod)
# by loading the appropriate configuration settings based on an environment variable.
# To set up in a new environment:
# 1. Change the APP_ENV variable in the .env file.
#       Example: APP_ENV="local"
# 2. Create a corresponding config file in the config directory (e.g., config_local.yaml)
#       Within the config file, set a database_path_root variable to point to the desired delta location
#
#
# Example config file content (config_local.yaml):
#
# database_path_root: /app/data/test_data
# app_path_root: /app/
#
#


from contextlib import contextmanager
from dotenv import load_dotenv
import yaml
from delta import *
import os


def get_env_variable() -> str:
    """Retrieve the application environment from the .env file."""
    load_dotenv()
    return os.getenv("APP_ENV", "local")


def load_config_file(env: str) -> dict:
    """Load the configuration file based on the environment."""
    config_file = f"config/config_{env}.yaml"
    with open(config_file, "r") as file:
        return yaml.safe_load(file)


@contextmanager
def spark_session(appName: str, configs: dict = None):
    """Create and return a Spark session with Delta Lake support."""
    import pyspark
    from delta import configure_spark_with_delta_pip

    builder = pyspark.sql.SparkSession.builder.appName(appName)
    if configs:
        for k, v in configs.items():
            builder = builder.config(k, v)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()
