import json
from pipeline.utils import shared_helpers as sh
from pipeline.utils import config_loader as cl
import argparse


def main(
    input_file: str,
    output_table: str,
    file_type: str = None,
    read_options: dict = None,
    write_options: dict = None,
    write_mode: str = None,
    spark=None,
):
    """Extract data from a file and save it as a table."""

    # Convert options from string to dict if needed
    if isinstance(read_options, str):
        read_options = json.loads(read_options)
    if isinstance(write_options, str):
        write_options = json.loads(write_options)

    # Extract geonames data from the specified file and save it as a table
    sh.extract_file(input_file, output_table, file_type, read_options, write_options, write_mode, spark)


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="This script extracts data from a file and place it in a table")

    # Define expected arguments
    parser.add_argument("--input_file", help="please enter full file path", required=True)
    parser.add_argument("--output_table", help="please enter full table name", required=True)
    parser.add_argument("--file_type", default="csv")
    parser.add_argument("--read_options", default='{"header": "true", "delimiter": ";"}')
    parser.add_argument("--write_options", default='{"header": "true", "delta.columnMapping.mode": "name"}')
    parser.add_argument("--write_mode", default="append")
    parser.add_argument("--spark_app_name", default="File_Extract_Job")

    # Parse the arguments
    args = parser.parse_args()

    # Run main
    cl.run_job(main, args)
