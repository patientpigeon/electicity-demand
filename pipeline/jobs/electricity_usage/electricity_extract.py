from pipeline.utils import config_loader as cl
from pipeline.utils import shared_helpers as sh
from dotenv import load_dotenv
import requests, os, argparse
from pyspark.sql.functions import lit
from delta import *


def main(zone: str, spark=None):
    """Extract electricity usage data from an API and save it as a Delta table."""

    # Retrieve database root based on environment
    env = cl.get_env_variable()
    config_params = cl.load_config_file(env)
    database_path_root = config_params.get("database_path_root", "./tmp/delta")

    # Load environment variables
    load_dotenv()
    api_url = f"https://api.electricitymaps.com/v3/power-breakdown/latest?zone={zone}"
    api_key = os.getenv("ELECTRICITY_API_KEY")

    # Defining API mappings
    app_root = config_params.get("app_path_root")
    job_config_file = f"{app_root}/config/electricity_extract.yaml"
    # api_fields_extract, dict_schema = sh.load_config_keys(job_config_file, "api_fields_extract", "dict_schema")

    # Make the API request
    response = requests.get(api_url, headers={"auth-token": api_key})

    # Process the API response into a Spark DataFrame
    if response.status_code == 200:
        api_dict = response.json()
    else:
        print(f"Error: {response.status_code}")

    print(api_dict)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--zone", help="", default="US-NW-PGE")
    parser.add_argument("--spark_app_name", help="", default="Electricity_Extract_Job")

    args = parser.parse_args()

    cl.run_job(main, args)
