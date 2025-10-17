from pipeline.utils import config_loader as cl
import argparse


def main(spark):
    # Weather merge into fact_hourly_metrics table
    # If data already exists for this city and time interval, but the weather_id is different or nonexistant, update the record to insert the weather data
    # If no data currently exists for this city and time interval, insert a new record
    spark.sql("""
        MERGE INTO utilities_warehouse.fact_hourly_metrics AS target
        USING utilities.weather AS source
        ON 
            target.city = source.city 
            AND target.date_time = source.date_time
        WHEN MATCHED AND (target.weather_id != source.id or target.weather_id IS NULL) THEN
            UPDATE SET 
              target.weather_id = source.id,    
              target.temp_c = source.temp_c,
              target.temp_f = source.temp_f,
              target.precip_mm = source.precip_mm   
        WHEN NOT MATCHED 
            THEN INSERT (
                city, 
                weather_id, 
                date_time,
                temp_c, 
                temp_f, 
                precip_mm
            )
            VALUES (
              source.city, 
              source.id, 
              source.date_time, 
              source.temp_c, 
              source.temp_f, 
              source.precip_mm
            )
    """)

    # Electricity use merge into fact_hourly_metrics table
    # If data already exists for this city and time interval, but the electricity_use_id is different or nonexistant, update the record to insert the electricity data
    # If no data currently exists for this city and time interval, insert a new record
    spark.sql("""
        MERGE INTO utilities_warehouse.fact_hourly_metrics AS target
        USING utilities.electricity_use AS source
        ON 
            target.city = source.city 
            AND target.date_time = source.date_time
        WHEN MATCHED AND (target.electricity_use_id != source.id or target.electricity_use_id IS NULL) THEN
            UPDATE SET 
                target.electricity_use_id = source.id,    
                target.power_consumption_total = source.power_consumption_total
        WHEN NOT MATCHED 
            THEN INSERT (
                city, 
                electricity_use_id, 
                date_time, 
                power_consumption_total
            )
            VALUES (
              source.city, 
              source.id, 
              source.date_time, 
              source.power_consumption_total
            )
    """)


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="This script extracts data from a file and place it in a table")

    # Define expected arguments
    parser.add_argument("--spark_app_name", default="Populate_Fact_Hourly_Metrics_Job")

    # Parse the arguments
    args = parser.parse_args()

    cl.run_job(main, args)
