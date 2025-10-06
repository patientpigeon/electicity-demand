from pipeline.utils import config_loader as cl
from pipeline.utils import shared_helpers as sh
from dotenv import load_dotenv
import requests, os, argparse
from pyspark.sql.functions import lit
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
    api_location, api_day, api_hour, api_astro = sh.load_config_keys(
        job_config_file, "api_day", "api_location", "api_hour", "api_astro"
    )

    # Set up API parameters
    api_params = {"key": api_key, "q": city, "dt": start_date, "end_dt": end_date}

    # Make the API request
    response = requests.get(api_url, params=api_params)

    # Process the API response into a Spark DataFrame
    if response.status_code == 200:
        api_response_dict = response.json()
    else:
        print(f"Error: {response.status_code}")

    def get_nested_dict_values(api_resp_dict: dict, path: list) -> any:
        """
        Extract nested values from dictionary using a list of keys with a default fallback.
        Keys will be all but the last element, default will be the last element
        Example: path = ['powerConsumptionBreakdown', 'nuclear', 0]
        """
        *keys, default = path
        for k in keys:
            api_resp_val = api_resp_dict.get(k, default)
        return api_resp_val

    rows = []
    for day in api_response_dict["forecast"]["forecastday"]:
        location = api_response_dict.get("location")
        astro = day.get("astro")
        for hour in day["hour"]:
            row = {}
            for col, path in api_day.items():
                row[col] = get_nested_dict_values(day, path)
            for col, path in api_location.items():
                row[col] = get_nested_dict_values(location, path)
            for col, path in api_hour.items():
                row[col] = get_nested_dict_values(hour, path)
            for col, path in api_astro.items():
                row[col] = get_nested_dict_values(astro, path)
            rows.append(row)

    api_df = spark.createDataFrame(rows)

    # Save DataFrame as a Delta table
    # print(api_response_dict)
    # api_df.select("is_day", "cloud", "temp_f").show(26, False)
    api_df.select("name", "time", "sunrise", "sunset", "temp_f").show(26, False)
    # api_df.write.format("delta").mode("append").save(f"{database_path_root}/weather_extract")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--city", type=str, required=True, help="City to get weather data for")
    parser.add_argument(
        "--start_date",
        type=str,
        default=sh.default_start_date(),
        help="Start date in YYYY-MM-DD format. Default is 7 days ago (the maximum allowed by the API).",
    )
    parser.add_argument(
        "--end_date", type=str, default=sh.default_end_date(), help="End date in YYYY-MM-DD format. Default is today."
    )
    parser.add_argument("--spark_app_name", help="", default="Weather_Extract_Job")

    args = parser.parse_args()

    cl.run_job(main, args)
