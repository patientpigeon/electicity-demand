import configparser


def create_config():
    config = configparser.ConfigParser()

    config["DEFAULT"] = {
        "app_root_path": "/Users/andrewpurchase/Documents/electicity-demand",
        "file_ingest_root_path": "/Users/andrewpurchase/Documents/electicity-demand/data/files",
        "database_root_path": "/Users/andrewpurchase/Documents/electicity-demand/data/test_data",
    }

    config["Geonames"] = {
        "geonames_file": "united_states_cities.csv",
        "extract_table": "geonames_extracted",
        "transform_table": "geonames_transformed",
        "csv_options": '{"header": "true", "delimiter": ";"}',
    }

    config["Spark"] = {"Master": "local[*]", "AppName": "GeoNamesDataPipeline"}

    with open("config/config.local.ini", "w") as configfile:
        config.write(configfile)


if __name__ == "__main__":
    create_config()
