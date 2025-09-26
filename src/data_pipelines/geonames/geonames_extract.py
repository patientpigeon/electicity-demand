from pyspark.sql import SparkSession
from src.utils import shared_helpers as sh
from dotenv import load_dotenv
import os


load_dotenv()

# Initialize Spark session with warehouse directory from environment variable
data_root_path = os.getenv("DATA_ROOT_PATH")
spark = SparkSession.builder.config("spark.sql.warehouse.dir", data_root_path).getOrCreate()

# Extract geonames data from the specified file and save it as a table
geonames_file = os.getenv("GEONAMES_FILE")
geonames_extract_table = os.getenv("GEONAMES_EXTRACT_TABLE")
sh.extract_file(geonames_file, geonames_extract_table, spark)
