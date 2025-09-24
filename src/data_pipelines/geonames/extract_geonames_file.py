from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

spark = SparkSession.builder.getOrCreate()

# Load environment variables from the .env file
load_dotenv()

ingest_file_location = (
    os.getenv("INGEST_FILE_LOCATION") + "data/files/united_states_cities.csv"
)
save_table_location = os.getenv("INGEST_FILE_LOCATION") + "data/test_data/geodata"

df = (
    spark.read.option("header", "true")
    .option("numPartitions", 10)
    .csv(ingest_file_location)
)

# df.select("*").show()

df.write.format("parquet").save(save_table_location)
