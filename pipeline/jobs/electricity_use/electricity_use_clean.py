from pipeline.utils import config_loader as cl
from pipeline.utils import shared_helpers as sh
from pyspark.sql.functions import lit
import argparse, json


def main(city: str, input_table: str, output_table: str, write_options: str, write_mode: str, spark=None):
    """Clean electricity use data and save it as a table."""

    # Retrieve environment specific config parameters
    env = cl.get_env_variable()
    config_params = cl.load_config_file(env)

    # Retrieve job specific config parameters
    app_root = config_params.get("app_path_root")
    job_config_file = f"{app_root}/config/electricity_use_clean.yaml"
    columns_to_select = sh.load_config_keys(job_config_file, "columns_to_select")[0]

    # Convert options from string to dict if needed
    if isinstance(write_options, str):
        write_options = json.loads(write_options)

    # Load the extracted data
    electricity_use_extract_df = spark.read.format("delta").load(input_table)

    # Adding city column
    electricity_clean_df = electricity_use_extract_df.withColumn("city", lit(city))

    # Convert our fields to the correct data types for the output table
    for k, v in columns_to_select.items():
        electricity_clean_df = electricity_clean_df.withColumn(k, electricity_clean_df[k].cast(v))

    # Ensure no duplicate insertion into cleaned table
    output_df = spark.read.format("delta").load(output_table)
    deduped_df = electricity_clean_df.join(
        output_df.select(*electricity_clean_df.columns), on=["city", "date_time"], how="left_anti"
    )

    # Write the cleaned data
    deduped_df.write.format("delta").mode(write_mode).options(**write_options).save(output_table)


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser()

    # Define expected arguments
    parser.add_argument("--city", help="", required=True)
    parser.add_argument("--input_table", help="", required=True)
    parser.add_argument("--output_table", help="", required=True)
    parser.add_argument("--write_options", help="", default='{"header": "true"}')
    parser.add_argument("--write_mode", help="", default="append")
    parser.add_argument("--spark_app_name", help="", default="Electricity_Use_Clean_Job")

    # Parse the arguments
    args = parser.parse_args()

    cl.run_job(main, args)
