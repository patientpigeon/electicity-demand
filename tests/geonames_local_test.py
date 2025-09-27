from src.utils.config_loader import spark
import os

print(os.getcwd())

df = spark.read.format("delta").load("data/test_data/geonames_clean")

df.show()


df.write.mode("overwrite").format("csv").save("data/files/output.csv")
