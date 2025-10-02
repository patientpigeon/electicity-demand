from src.utils import shared_helpers as sh
from src.utils.config_loader import get_env_variable, load_config_file
from src.data_pipelines.geonames import geonames_clean
from src.utils.common_jobs import file_extract
import argparse, inspect, json


def main(
    extract_input_file: str,
    extract_output_table: str,
    extract_file_type: str,
    extract_read_options: dict,
    extract_write_options: dict,
    extract_write_mode: str,
    clean_input_table: str,
    clean_output_table: str,
    clean_write_options: dict,
    clean_write_mode: str,
    spark=None,
):
    """Run the geonames data pipeline."""

    # Retrieve app root based on environment
    env = get_env_variable()
    config = load_config_file(env)
    app_root = config.get("app_path_root")

    # Defining argument mappings
    job_config_file = f"{app_root}/config/geonames_main.yaml"
    extract_job_arg_map, clean_job_arg_map = sh.load_argument_config(job_config_file)

    # Renaming arguments for each job
    extract_job_args = sh.rename_args(extract_job_arg_map, args)
    clean_job_args = sh.rename_args(clean_job_arg_map, args)

    # Adding the spark session to the arguments
    extract_job_args["spark"] = spark
    clean_job_args["spark"] = spark

    # Running the jobs in sequence
    file_extract.main(**extract_job_args)
    geonames_clean.main(**clean_job_args)

    print("\n\n\nsuccess\n\n")


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser()

    # Define expected arguments
    # File Extract
    parser.add_argument("--extract_input_file", help="please enter full file path", required=True)
    parser.add_argument("--extract_output_table", help="please enter full table name", required=True)
    parser.add_argument("--extract_file_type", default="csv")
    parser.add_argument("--extract_read_options", default='{"header": "true", "delimiter": ";"}')
    parser.add_argument("--extract_write_options", default='{"header": "true", "delta.columnMapping.mode": "name"}')
    parser.add_argument("--extract_write_mode", default="append")
    # Geonames Clean
    parser.add_argument("--clean_input_table", help="", required=True)
    parser.add_argument("--clean_output_table", help="", required=True)
    parser.add_argument("--clean_write_options", help="", default='{"header": "true"}')
    parser.add_argument("--clean_write_mode", help="", default="append")
    # Spark app name for the pipeline
    parser.add_argument("--spark_app_name", default="Geonames_Pipeline")

    # Parse the arguments
    args = parser.parse_args()

    # Run main
    sh.run_job(main, args)
