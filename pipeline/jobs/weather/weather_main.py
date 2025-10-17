from pipeline.utils import shared_helpers as sh
from pipeline.utils import config_loader as cl
from pipeline.utils import main_helpers as mh
from pipeline.jobs.weather import weather_extract, weather_clean
import argparse


def main(
    extract_city: str,
    extract_start_date: str,
    extract_end_date: str,
    clean_input_table: str,
    clean_output_table: str,
    clean_write_options: dict,
    clean_write_mode: str,
    spark=None,
):
    """Run the weather pipeline."""

    # Get current arguments
    current_args = locals()

    # Retrieve app root based on environment
    env = cl.get_env_variable()
    config_params = cl.load_config_file(env)
    app_root = config_params.get("app_path_root")

    # Defining argument mappings
    job_config_file = f"{app_root}/config/weather_main.yaml"
    extract_job_arg_map, clean_job_arg_map = sh.load_config_keys(
        job_config_file, "weather_extract_arg_map", "weather_clean_arg_map"
    )

    # Renaming arguments for each job
    extract_job_args = mh.rename_subjob_args(extract_job_arg_map, current_args)
    clean_job_args = mh.rename_subjob_args(clean_job_arg_map, current_args)

    # Adding the spark session to the arguments
    extract_job_args["spark"] = spark
    clean_job_args["spark"] = spark

    # Running the jobs in sequence
    weather_extract.main(**extract_job_args)
    weather_clean.main(**clean_job_args)

    print("\n\n\nsuccess\n\n")


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser()

    # Define expected arguments
    # API Extract
    parser.add_argument("--extract_city", type=str, required=True, help="City to get weather data for")
    parser.add_argument(
        "--extract_start_date", type=str, default=sh.default_start_date(), help="Start date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--extract_end_date", type=str, default=sh.default_end_date(), help="End date in YYYY-MM-DD format"
    )
    # Weather Clean
    parser.add_argument("--clean_input_table", help="please enter full table name", required=True)
    parser.add_argument("--clean_output_table", help="please enter full table name", required=True)
    parser.add_argument("--clean_write_options", default='{"header": "true", "delta.columnMapping.mode": "name"}')
    parser.add_argument("--clean_write_mode", default="append")

    # Spark app name for the pipeline
    parser.add_argument("--spark_app_name", default="Weather_Pipeline")

    # Parse the arguments
    args = parser.parse_args()

    # Run main
    cl.run_job(main, args)
