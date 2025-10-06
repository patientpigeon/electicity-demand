from pipeline.utils.config_loader import get_env_variable, load_config_file
from pipeline.utils import shared_helpers as sh
from pipeline.utils import config_loader as cl
import argparse
import json
import pyspark.sql.functions as sf


def main(
    input_table: str,
    output_table: str,
    write_options: dict,
    write_mode: str,
    spark=None,
):
    """Clean city data and save it as a table."""

    # Convert options from string to dict if needed
    if isinstance(write_options, str):
        write_options = json.loads(write_options)

    # Retrieve app root based on environment
    env = get_env_variable()
    config_params = load_config_file(env)
    app_root = config_params.get("app_path_root")

    # Defining column mappings
    job_config_file = f"{app_root}/config/city_clean.yaml"
    column_mappings, column_select = cl.load_config_keys(job_config_file, "column_mappings", "column_select")

    # Load the extracted data
    file_df = spark.read.format("delta").load(input_table)

    # Split coordinates column into latitude and longitude
    updated_coords_df = (
        file_df.withColumn("longitude", sf.substring_index(file_df["Coordinates"], ",", 1))
        .withColumn("latitude", sf.substring_index(file_df["Coordinates"], ",", -1))
        .drop("Coordinates")
    )
    # Rename columns based on the mapping
    renamed_df = sh.rename_columns(updated_coords_df, column_mappings)

    # Select only the relevant columns
    updated_columns = renamed_df.select(*column_select)

    # Save the cleaned data
    updated_columns.write.mode(write_mode).format("delta").options(**write_options).save(output_table)


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser()

    # Define expected arguments
    parser.add_argument("--input_table", help="", required=True)
    parser.add_argument("--output_table", help="", required=True)
    parser.add_argument("--write_options", help="", default='{"header": "true"}')
    parser.add_argument("--write_mode", help="", default="overwrite")
    parser.add_argument("--spark_app_name", help="", default="City_Clean_Job")

    # Parse the arguments
    args = parser.parse_args()
    args.write_options = json.loads(args.write_options)

    # Run main
    cl.run_job(main, args)
