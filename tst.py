from pipeline.utils import config_loader as cl
from pipeline.utils import shared_helpers as sh
from dotenv import load_dotenv
import requests, os, argparse
from pyspark.sql.functions import lit
from delta import *


def main(zone: str, spark=None):
    # """Extract electricity usage data from an API and save it as a Delta table."""

    # # Retrieve database root based on environment
    # env = cl.get_env_variable()
    # config_params = cl.load_config_file(env)
    # database_path_root = config_params.get("database_path_root", "./tmp/delta")

    # # Load environment variables
    # load_dotenv()
    # api_url = f"https://api.electricitymaps.com/v3/power-breakdown/latest?zone={zone}"
    # api_key = os.getenv("ELECTRICITY_API_KEY")

    # # Defining API mappings
    # app_root = config_params.get("app_path_root")
    # job_config_file = f"{app_root}/config/electricity_extract.yaml"
    # # api_fields_extract, dict_schema = sh.load_config_keys(job_config_file, "api_fields_extract", "dict_schema")

    # # Make the API request
    # response = requests.get("https://api.electricitymaps.com/v3/power-breakdown/latest?zone=US-NW-PGE", headers={"auth-token": api_key})

    # # Process the API response into a Spark DataFrame
    # if response.status_code == 200:
    #     api_dict = response.json()
    # else:
    #     print(f"Error: {response.status_code}")

    # print(api_dict)
    # print("\n\n\n\n")

    # print(api_dict["datetime"])
    # print(api_dict["powerConsumptionBreakdown"]["nuclear"])
    # print("\n\n\n\n")

    # api_fields_extract = {"datetime": "api_dict['datetime']", "nuclear": "api_dict['powerConsumptionBreakdown']['nuclear']", "nuclear2": "api_dict['powerProductionBreakdown']['nuclear']"}

    # row = [{
    #     "cons_date_time": api_dict.get("datetime"),
    #     "cons_updated_at": api_dict.get("updatedAt"),
    #     "cons_createdAt": api_dict.get("createdAt"),
    #     "cons_nuclear": api_dict.get("powerConsumptionBreakdown").get("nuclear", 0),
    #     "cons_geothermal": api_dict.get("powerConsumptionBreakdown").get("geothermal", 0),
    #     "cons_biomass": api_dict.get("powerConsumptionBreakdown").get("biomass", 0),
    #     "cons_coal": api_dict.get("powerConsumptionBreakdown").get("coal", 0),
    #     "cons_wind": api_dict.get("powerConsumptionBreakdown").get("wind", 0),
    #     "cons_solar": api_dict.get("powerConsumptionBreakdown").get("solar", 0),
    #     "cons_hydro": api_dict.get("powerConsumptionBreakdown").get("hydro", 0),
    #     "cons_gas": api_dict.get("powerConsumptionBreakdown").get("gas", 0),
    #     "cons_oil": api_dict.get("powerConsumptionBreakdown").get("oil", 0),
    #     "cons_unknown": api_dict.get("powerConsumptionBreakdown").get("unknown", 0),
    #     "cons_hydro_discharge": api_dict.get("powerConsumptionBreakdown").get("hydro discharge", 0),
    #     "cons_battery_discharge": api_dict.get("powerConsumptionBreakdown").get("battery discharge", 0),
    #     "prod_nuclear": api_dict.get("powerProductionBreakdown").get("nuclear", 0),
    #     "prod_geothermal": api_dict.get("powerProductionBreakdown").get("geothermal", 0),
    #     "prod_biomass": api_dict.get("powerProductionBreakdown").get("biomass", 0),
    #     "prod_coal": api_dict.get("powerProductionBreakdown").get("coal", 0),
    #     "prod_wind": api_dict.get("powerProductionBreakdown").get("wind", 0),
    #     "prod_solar": api_dict.get("powerProductionBreakdown").get("solar", 0),
    #     "prod_hydro": api_dict.get("powerProductionBreakdown").get("hydro", 0),
    #     "prod_gas": api_dict.get("powerProductionBreakdown").get("gas", 0),
    #     "prod_oil": api_dict.get("powerProductionBreakdown").get("oil", 0),
    #     "prod_unknown": api_dict.get("powerProductionBreakdown").get("unknown", 0),
    #     "prod_hydro_charge": api_dict.get("powerProductionBreakdown").get("hydro charge", 0),
    #     "prod_battery_charge": api_dict.get("powerProductionBreakdown").get("battery charge", 0),
    #     "powerImportBreakdown": api_dict.get("powerImportBreakdown"),
    #     "powerExportBreakdown": api_dict.get("powerExportBreakdown"),
    #     "fossilFreePercentage": api_dict.get("fossilFreePercentage", 0),
    #     "renewablePercentage": api_dict.get("renewablePercentage", 0),
    #     "powerConsumptionTotal": api_dict.get("powerConsumptionTotal", 0),
    #     "powerProductionTotal": api_dict.get("powerProductionTotal", 0),
    #     "powerImportTotal": api_dict.get("powerImportTotal", 0),
    #     "powerExportTotal": api_dict.get("powerExportTotal", 0),
    #     "isEstimated": api_dict.get("isEstimated", True),
    #     "estimationMethod": api_dict.get("estimationMethod", "UNKNOWN"),
    #     "temporalGranularity": api_dict.get("temporalGranularity", "unknown")

    # }]
    # print(row)

    # df = spark.createDataFrame(row, "cons_date_time string, cons_updated_at string, cons_createdAt string, cons_nuclear int, cons_geothermal int, cons_biomass int, cons_coal int, cons_wind int, cons_solar int, cons_hydro int, cons_gas int, cons_oil int, cons_unknown int, cons_hydro_discharge int, cons_battery_discharge int, prod_nuclear int, prod_geothermal int, prod_biomass int, prod_coal int, prod_wind int, prod_solar int, prod_hydro int, prod_gas int, prod_oil int, prod_unknown int, prod_hydro_charge int, prod_battery_charge int, powerImportBreakdown map<string,int>, powerExportBreakdown map<string,int>, fossilFreePercentage int, renewablePercentage int, powerConsumptionTotal int, powerProductionTotal int, powerImportTotal int, powerExportTotal int, isEstimated boolean, estimationMethod string, temporalGranularity string")
    # df.show()

    # # Convert API response to Spark DataFrame
    # api_df = spark.createDataFrame(data = rows, schema = dict_schema)

    # # Add City to the DataFrame for context
    # city_df = api_dict.withColumn("city", lit(city))

    # # Save DataFrame as a Delta table
    # city_df.write.format("delta").mode("append").save(f"{database_path_root}/electricity_extract")
    # Retrieve database root based on environment
    env = cl.get_env_variable()
    config_params = cl.load_config_file(env)

    app_root = config_params.get("app_path_root")
    job_config_file = f"{app_root}/config/electricity_extract.yaml"
    api_fields_extract, dict_schema = sh.load_config_keys(job_config_file, "api_fields_extract", "dict_schema")

    print(f"{dict_schema}")
    print(f"[{dict_schema}]")
    print("\n\n")
    print(dict_schema)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--zone", help="", default="US-NW-PGE")
    parser.add_argument("--spark_app_name", help="", default="Electricity_Extract_Job")

    args = parser.parse_args()

    main(args)
    # cl.run_job(main, args)
