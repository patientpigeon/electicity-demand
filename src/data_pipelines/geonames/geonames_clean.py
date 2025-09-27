from src.utils.config_loader import spark
import pyspark.sql.functions as sf
import argparse

# Set up argument parser
parser = argparse.ArgumentParser(description="This script is used to clean geonames files")

# Define expected arguments
parser.add_argument("--input_table", help="", required=True)
parser.add_argument("--output_table", help="", required=True)
parser.add_argument("--write_options", help="", default={"header": "true"})
parser.add_argument("--write_mode", help="", default="append")

# Parse the arguments
args = parser.parse_args()


# Load the extracted data
file_df = spark.read.format("delta").load(args.input_table)

# Normalize column names
normalized_columns = (
    file_df.withColumnRenamed("Name", "city")
    .withColumnRenamed("Alternate Names", "alternate_city_names")
    .withColumnRenamed("Country Name EN", "country")
    .withColumnRenamed("Admin1 Code", "admin_code_1")
    .withColumnRenamed("Admin2 Code", "admin_code_2")
    .withColumnRenamed("Admin3 Code", "admin_code_3")
    .withColumnRenamed("Admin4 Code", "admin_code_4")
    .withColumnRenamed("Population", "population")
    .withColumnRenamed("Elevation", "elevation")
    .withColumnRenamed("Timezone", "timezone")
    .withColumnRenamed("Coordinates", "coordinates")
    .select(
        "city",
        "alternate_city_names",
        "country",
        "admin_code_1",
        "admin_code_2",
        "admin_code_3",
        "admin_code_4",
        "population",
        "elevation",
        "timezone",
        "coordinates",
    )
)

# Split coordinates into latitude and longitude
updated_columns = (
    normalized_columns.withColumn("longitude", sf.substring_index(normalized_columns.coordinates, ",", 1))
    .withColumn("latitude", sf.substring_index(normalized_columns.coordinates, ",", -1))
    .drop("coordinates")
)

# Save the cleaned data
updated_columns.write.mode(args.write_mode).format("delta").options(**args.write_options).save(args.output_table)
