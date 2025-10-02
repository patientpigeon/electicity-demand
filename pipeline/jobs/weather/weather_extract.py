from pipeline.utils.config_loader import get_env_variable, load_config_file, spark_session

from dotenv import load_dotenv
import requests, os
import pyspark
from delta import *

builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.debug.maxToStringFields", "1000")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


env = get_env_variable()
config_params = load_config_file(env)
database_path_root = config_params.get("database_path_root", "./tmp/delta")


load_dotenv()
api_url = "http://api.weatherapi.com/v1/history.json"
api_key = os.getenv("WEATHER_API_KEY")

params = {"key": api_key, "q": "Portland", "dt": "2025-09-27", "end_dt": "2025-10-01"}
response = requests.get(api_url, params=params)
if response.status_code == 200:
    df = response.json()
else:
    print(f"Error: {response.status_code}")

rows = []
for day in df["forecast"]["forecastday"]:
    for hour in day["hour"]:
        rows.append(
            {
                "date": day["date"],
                "time": hour["time"],
                "temp_c": hour["temp_c"],
                "temp_f": hour["temp_f"],
                "condition": hour["condition"]["text"],
                "is_day": hour["is_day"],
                "precip_mm": hour["precip_mm"],
                "snow_cm": hour["snow_cm"],
                "humidity": hour["humidity"],
                "cloud": hour["cloud"],
                "feelslike_c": hour["feelslike_c"],
                "windchill_c": hour["windchill_c"],
                "heatindex_c": hour["heatindex_c"],
                "dewpoint_c": hour["dewpoint_c"],
                "will_it_rain": hour["will_it_rain"],
                "chance_of_rain": hour["chance_of_rain"],
                "will_it_snow": hour["will_it_snow"],
                "chance_of_snow": hour["chance_of_snow"],
                "wind_dir": hour["wind_dir"],
                "pressure_mb": hour["pressure_mb"],
                "wind_kph": hour["wind_kph"],
                "vis_km": hour["vis_km"],
                "gust_kph": hour["gust_kph"],
                "uv": hour["uv"],
                "sunrise": day["astro"]["sunrise"],
                "sunset": day["astro"]["sunset"],
            }
        )

df = spark.createDataFrame(
    data=rows,
    schema=[
        "sunrise",
        "sunset",
        "date",
        "time",
        "temp_c",
        "temp_f",
        "condition",
        "is_day",
        "precip_mm",
        "snow_cm",
        "humidity",
        "cloud",
        "feelslike_c",
        "windchill_c",
        "heatindex_c",
        "dewpoint_c",
        "will_it_rain",
        "chance_of_rain",
        "will_it_snow",
        "chance_of_snow",
        "wind_dir",
        "pressure_mb",
        "wind_kph",
        "vis_km",
        "gust_kph",
        "uv",
    ],
)
df.printSchema()
df.write.format("delta").mode("append").save(f"{database_path_root}/weather_extract")
