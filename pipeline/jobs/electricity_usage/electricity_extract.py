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
    base_fields, power_consumption_breakdown, power_production_breakdown, columns_to_select = sh.load_config_keys(
        job_config_file, "base_fields", "power_consumption_breakdown", "power_production_breakdown", "columns_to_select"
    )

    # Make the API request
    response = requests.get(api_url, headers={"auth-token": api_key})

    # Process the API response into a dictionary
    if response.status_code == 200:
        api_response_dict = response.json()
    else:
        print(f"Error: {response.status_code}")

    # Build a row dictionary by extracting each field using the mapping
    row = {}
    for col, path in base_fields.items():
        row[col] = sh.get_nested_dict_values(api_response_dict, path)
    for col, path in power_consumption_breakdown.items():
        row[col] = sh.get_nested_dict_values(api_response_dict["powerConsumptionBreakdown"], path)
    for col, path in power_production_breakdown.items():
        row[col] = sh.get_nested_dict_values(api_response_dict["powerProductionBreakdown"], path)

    # Wrap the row in a list for DataFrame creation
    rows = [row]
    api_df = spark.createDataFrame(rows)

    # Reorder columns based on the config
    updated_columns_df = api_df.select(*columns_to_select)

    # Save DataFrame as a Delta table
    updated_columns_df.write.format("delta").mode("append").save(f"{database_path_root}/electricity_extract")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--zone", help="", default="US-NW-PGE")
    parser.add_argument("--spark_app_name", help="", default="Electricity_Extract_Job")

    args = parser.parse_args()

    cl.run_job(main, args)
