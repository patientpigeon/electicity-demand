import configparser


def create_config():
    config = configparser.ConfigParser()

    config["DEFAULT"] = {
        "database_root_path": "/Users/andrewpurchase/Documents/electicity-demand/data/test_data",
    }


if __name__ == "__main__":
    create_config
