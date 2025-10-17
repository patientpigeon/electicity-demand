# This file allows us to run Spark jobs in different environments (e.g., local, dev, prod)
# by loading the appropriate configuration settings based on an environment variable.
# To set up in a new environment:
# 1. Create APP_ENV and API_KEY variables in the .env file
#       Example:    APP_ENV="local"
#                   WEATHER_API_KEY="weather_api_key"
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
    """Retrieve the application environment from the .env file.
    Args:
        None
    Returns:
        str: The application environment (e.g., 'local', 'dev', 'prod')."""
    load_dotenv()
    return os.getenv("APP_ENV", "local")


def load_config_file(env: str) -> dict:
    """Load the configuration file based on the environment.
    Args:
        env (str): The application environment (e.g., 'local', 'dev', 'prod').
    Returns:
        dict: The configuration parameters loaded from the environment specific YAML file.
    """
    app_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    config_file = f"{app_root}/config/config_{env}.yaml"
    with open(config_file, "r") as file:
        return yaml.safe_load(file)


@contextmanager
def spark_session(appName: str, configs: dict = None):
    """Create and return a Spark session
    Args:
        appName (str): The name of the Spark application.
        configs (dict, optional): Additional Spark configurations. Defaults to None.
    Yields:
        SparkSession: An active Spark session.
    Returns:
        None
    """
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


def run_job(function, arguments):
    """Run a job with the given arguments and initiate a spark session for that job.
    Args:
        function (callable): The job function to be executed.
        arguments (argparse.Namespace): The arguments to be passed to the job function.
    Returns:
        None
    """

    # Retrieve configuration based on environment
    env = get_env_variable()
    config_params = load_config_file(env)
    database_path_root = config_params.get("database_path_root", "./tmp/delta")

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
