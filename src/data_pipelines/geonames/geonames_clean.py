from src.utils.config_loader import get_env_variable, load_config_file
from src.utils import shared_helpers as sh
import argparse
import json


def main(
    input_table: str,
    output_table: str,
    write_options: dict,
    write_mode: str,
    spark=None,
):
    """Clean geonames data and save it as a table."""

    # Convert options from string to dict if needed
    if isinstance(write_options, str):
        write_options = json.loads(write_options)

    # Retrieve app root based on environment
    env = get_env_variable()
    config = load_config_file(env)
    app_root = config.get("app_path_root")

    # Defining column mappings
    job_config_file = f"{app_root}/config/geonames_clean.yaml"
    column_mappings, column_select = sh.load_column_config(job_config_file)

    # Load the extracted data
    file_df = spark.read.format("delta").load(input_table)

    # Split coordinates column into latitude and longitude
    updated_coords_df = sh.split_coordinates(file_df, "Coordinates")

    # Rename columns based on the mapping
    renamed_df = sh.rename_columns(updated_coords_df, column_mappings)

    # Select only the relevant columns
    updated_columns = sh.select_columns(renamed_df, column_select)

    # Save the cleaned data
    updated_columns.write.mode(write_mode).format("delta").options(**write_options).save(output_table)


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="This script is used to clean geonames files")

    # Define expected arguments
    parser.add_argument("--input_table", help="", required=True)
    parser.add_argument("--output_table", help="", required=True)
    parser.add_argument("--write_options", help="", default='{"header": "true"}')
    parser.add_argument("--write_mode", help="", default="append")
    parser.add_argument("--spark_app_name", help="", default="Geonames_Clean_Job")

    # Parse the arguments
    args = parser.parse_args()
    args.write_options = json.loads(args.write_options)

    # Run main
    sh.run_job(main, args)
