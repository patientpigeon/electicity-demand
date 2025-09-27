from src.utils.config_loader import spark
from src.utils import shared_helpers as sh
import argparse

# Set up argument parser
parser = argparse.ArgumentParser(description="This script is used to extract data from a file and place it in a table")

# Define expected arguments
parser.add_argument("--input_file", help="please enter full file path", required=True)
parser.add_argument("--output_table", help="plaease enter full file path", required=True)
parser.add_argument("--file_type", help="if no file type is specified the extract_file function will default to csv")
parser.add_argument(
    "--read_options",
    help="if no read options are specified the extract_file function will default to header=True and delimiter=';'",
)
parser.add_argument(
    "--write_options",
    help="if no write options are specified the extract_file function will default to header=True and delta.columnMapping.mode=name",
)
parser.add_argument(
    "--write_mode", help="if no write mode is specified the extract_file function will default to append"
)

# Parse the arguments
args = parser.parse_args()

# Extract geonames data from the specified file and save it as a table
sh.extract_file(
    args.input_file, args.output_table, args.file_type, args.read_options, args.write_options, args.write_mode, spark
)
