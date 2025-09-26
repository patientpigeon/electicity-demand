from src.utils.config_loader import config, spark
from src.utils import shared_helpers as sh
import os

# Extract config values
file_ingest_root_path = config.get("DEFAULT", "file_ingest_root_path")
geonames_file = config.get("Geonames", "geonames_file")
database_root_path = config.get("DEFAULT", "database_root_path")
geonames_extract_table = config.get("Geonames", "extract_table")
csv_options = eval(config.get("Geonames", "csv_options"))

# Build paths
geonames_file_path = os.path.join(file_ingest_root_path, geonames_file)
geonames_extract_path = os.path.join(database_root_path, geonames_extract_table)

# Extract geonames data from the specified file and save it as a table
sh.extract_csv(geonames_file_path, geonames_extract_path, spark, csv_options)
