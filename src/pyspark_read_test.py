from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySparkTest").getOrCreate()

# Replace with the path to your Parquet directory
parquet_path = (
    "/Users/andrewpurchase/Documents/electicity-demand/data/test_data/test_df_write"
)

# Read the Parquet file(s) into a DataFrame
df = spark.read.parquet(parquet_path)

df.show()

df.createOrReplaceTempView("my_view")

result = spark.sql("""
    SELECT Age, Name, concat('hello', Name, ', how are you')
    FROM my_view
    WHERE Age < 30
""")

result.show()
