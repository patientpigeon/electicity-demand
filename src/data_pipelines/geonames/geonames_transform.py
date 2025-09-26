from src.utils.config_loader import spark, config
import pyspark.sql.functions as sf
import os

# Extract config values
database_root_path = config.get("DEFAULT", "database_root_path")
geonames_extract_table = config.get("Geonames", "extract_table")

# Build paths
geonames_extract_path = os.path.join(database_root_path, geonames_extract_table)

file_df = spark.read.format("delta").load(geonames_extract_path)

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

updated_columns = (
    normalized_columns.withColumn("longitude", sf.substring_index(normalized_columns.coordinates, ",", 1))
    .withColumn("latitude", sf.substring_index(normalized_columns.coordinates, ",", -1))
    .drop("coordinates")
)

# updated_columns.show(5, truncate=False)
# df.printSchema()

# print([repr(col) for col in df.columns])

transformed_table_ = config.get("Geonames", "transform_table")
updated_columns.write.mode("overwrite").format("delta").save(transformed_table_)
