import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)

def get_spark_session(app_name: str) -> SparkSession:

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    return spark