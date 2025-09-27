from src.utils.config_loader import spark
import os

print(os.getcwd())

df = spark.read.format("delta").load("data/test_data/geonames_clean")

df.show()


df.write.mode("overwrite").options(header=True).format("csv").save("data/files/output.csv")


# spark.sql("create table if not exists my_table2 (name varchar(255),location varchar(50))")
# spark.sql("show tables;").show()

# file_df = spark.read.format("delta").load("/Users/andrewpurchase/Documents/electicity-demand/data/test_data/geonames_clean").createOrReplaceTempView("my_temp_view")
# or
# spark.sql("create or replace temporary view my_temp_view using delta location '/Users/andrewpurchase/Documents/electicity-demand/data/test_data/geonames_clean'")
# spark.sql("select * from my_temp_view").show()

# spark.sql("show create table geonames_clean").show()
