from pipeline.utils import config_loader as cl
import argparse, json
from pyspark.sql import functions as sf


def main(input_table: str, output_table: str, write_options: str, write_mode: str, spark=None):
    """Clean weather data and save it as a table."""

    # Convert options from string to dict if needed
    if isinstance(write_options, str):
        write_options = json.loads(write_options)

    # Load the extracted data
    file_df = spark.read.format("delta").load(input_table)

    # Add identifier column
    identified_df = file_df.withColumn("id", sf.monotonically_increasing_id())

    # Ensure there are no duplicates between our dataframe and the output table
    identified_df.createOrReplaceTempView("identified_df_view")
    output_df = spark.read.format("delta").load(output_table)

    deduped_df = identified_df.join(
        output_df.select(*identified_df.columns), on=["city", "date", "time"], how="left_anti"
    )

    # Write the cleaned data
    deduped_df.write.format("delta").mode(write_mode).options(**write_options).save(output_table)


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser()

    # Define expected arguments
    parser.add_argument("--input_table", help="", required=True)
    parser.add_argument("--output_table", help="", required=True)
    parser.add_argument("--write_options", help="", default='{"header": "true"}')
    parser.add_argument("--write_mode", help="", default="append")
    parser.add_argument("--spark_app_name", help="", default="City_Clean_Job")

    # Parse the arguments
    args = parser.parse_args()

    cl.run_job(main, args)
