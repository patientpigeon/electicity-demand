from pipeline.utils import shared_helpers as sh
from pipeline.utils import config_loader as cl
from pipeline.utils import main_helpers as mh
from pipeline.jobs.city import city_clean
from pipeline.generic_jobs import file_extract
import argparse


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
    """Run the city pipeline."""

    # Retrieve app root based on environment
    env = cl.get_env_variable()
    config_params = cl.load_config_file(env)
    app_root = config_params.get("app_path_root")

    # Defining argument mappings
    job_config_file = f"{app_root}/config/city_main.yaml"
    extract_job_arg_map, clean_job_arg_map = sh.load_config_keys(
        job_config_file, "file_extract_arg_map", "city_clean_arg_map"
    )

    # Renaming arguments for each job
    extract_job_args = mh.rename_subjob_args(extract_job_arg_map, args)
    clean_job_args = mh.rename_subjob_args(clean_job_arg_map, args)

    # Adding the spark session to the arguments
    extract_job_args["spark"] = spark
    clean_job_args["spark"] = spark

    # Running the jobs in sequence
    file_extract.main(**extract_job_args)
    city_clean.main(**clean_job_args)

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
    # City Clean
    parser.add_argument("--clean_input_table", help="", required=True)
    parser.add_argument("--clean_output_table", help="", required=True)
    parser.add_argument("--clean_write_options", help="", default='{"header": "true"}')
    parser.add_argument("--clean_write_mode", help="", default="append")
    # Spark app name for the pipeline
    parser.add_argument("--spark_app_name", default="City_Pipeline")

    # Parse the arguments
    args = parser.parse_args()

    # Run main
    cl.run_job(main, args)
