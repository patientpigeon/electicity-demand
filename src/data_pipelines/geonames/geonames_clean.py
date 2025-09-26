from pyspark.sql import SparkSession
from dotenv import load_dotenv
import pyspark.sql.functions as sf
import os

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "/your/desired/path").getOrCreate()

load_dotenv()

file_root_path = os.getenv("DATA_ROOT_PATH")
geonames_extract_table = os.getenv("GEONAMES_EXTRACT_TABLE")

file_df = spark.read.parquet(os.path.join(file_root_path, geonames_extract_table))

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

# updated_columns.show(5, truncate=False)
# df.printSchema()

# print([repr(col) for col in df.columns])

data_base_path = os.getenv("DATA_ROOT_PATH")
data_path = os.getenv("GEONAMES_TRANSFORMED_TABLE")
full_transformed_path = os.path.join(data_base_path, data_path)
updated_columns.write.mode("overwrite").saveAsTable(full_transformed_path)
