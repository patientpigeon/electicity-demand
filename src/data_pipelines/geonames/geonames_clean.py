from src.utils.config_loader import spark, config
from src.utils.shared_helpers import rename_columns, split_coordinates, select_columns, load_config
import argparse, pyspark.sql.functions as sf
import yaml, json

# Set up argument parser
parser = argparse.ArgumentParser(description="This script is used to clean geonames files")

# Define expected arguments
parser.add_argument("--input_table", help="", required=True)
parser.add_argument("--output_table", help="", required=True)
parser.add_argument("--write_options", help="", default='{"header": "true"}')
parser.add_argument("--write_mode", help="", default="append")

# Parse the arguments
args = parser.parse_args()
args.write_options = json.loads(args.write_options)

# Defining column mappings
app_root = config.get("DEFAULT", "app_path_root")
job_config_file = f"{app_root}/config/geonames_clean.yaml"
column_mappings, column_select = load_config(job_config_file)


# Load the extracted data
file_df = spark.read.format("delta").load(args.input_table)

# Split coordinates column into latitude and longitude
updated_coords_df = split_coordinates(file_df, "Coordinates")

# Rename columns based on the mapping
renamed_df = rename_columns(updated_coords_df, column_mappings)

# Select only the relevant columns
updated_columns = select_columns(renamed_df, column_select)

# Save the cleaned data
updated_columns.write.mode(args.write_mode).format("delta").options(**args.write_options).save(args.output_table)
