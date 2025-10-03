from pipeline.utils import config_loader as cl
from pipeline.utils import shared_helpers as sh
from dotenv import load_dotenv
import requests, os, argparse
import pyspark
from delta import *


def main(city: str, start_date: str, end_date: str, spark=None):
    """Extract weather data from an API and save it as a Delta table."""

    # Retrieve database root based on environment
    env = cl.get_env_variable()
    config_params = cl.load_config_file(env)
    database_path_root = config_params.get("database_path_root", "./tmp/delta")

    # Load environment variables
    load_dotenv()
    api_url = "http://api.weatherapi.com/v1/history.json"
    api_key = os.getenv("WEATHER_API_KEY")

    # Defining API mappings
    app_root = config_params.get("app_path_root")
    job_config_file = f"{app_root}/config/weather_extract.yaml"
    api_fields_extract, dict_schema = sh.load_config_keys(job_config_file, "api_fields_extract", "dict_schema")

    # Set up API parameters
    api_params = {"key": api_key, "q": args.city, "dt": args.start_date, "end_dt": args.end_date}

    # Make the API request
    response = requests.get(api_url, params=api_params)

    # Process the API response into a Spark DataFrame
    if response.status_code == 200:
        api_df = response.json()
    else:
        print(f"Error: {response.status_code}")

    rows = []
    for day in api_df["forecast"]["forecastday"]:
        for hour in day["hour"]:
            rows.append({**api_fields_extract})

    api_df = spark.createDataFrame(
        data=rows,
        schema=dict_schema,
    )

    api_df.printSchema()
    api_df.write.format("delta").mode("append").save(f"{database_path_root}/weather_extract")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--city", type=str, default="Portland", help="City to get weather data for")
    parser.add_argument("--start_date", type=str, required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end_date", type=str, required=True, help="End date in YYYY-MM-DD format")
    parser.add_argument("--spark_app_name", help="", default="Weather_Extract_Job")

    args = parser.parse_args()

    cl.run_job(main, args)
