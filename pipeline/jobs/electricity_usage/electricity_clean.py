from pipeline.utils import config_loader as cl


def main(spark):
    df = spark.read.format("delta").load(f"{database_path_root}/electricity_extract")


if __name__ == "__main__":
    cl.run_job(main, "Electricity Clean Job")
