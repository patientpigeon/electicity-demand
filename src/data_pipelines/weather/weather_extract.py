from pyspark.sql import SparkSession
from src.utils.shared_helpers import read_api, save_as_table


def extract_weather_data():
    spark = SparkSession.builder.getOrCreate()
    api_url = "https://api.weather.com/data"
    df = read_api(api_url)  # Shared function to read API data
    save_as_table(df, "raw_weather")  # Shared function to save as a table
    return df


if __name__ == "__main__":
    extract_weather_data()
