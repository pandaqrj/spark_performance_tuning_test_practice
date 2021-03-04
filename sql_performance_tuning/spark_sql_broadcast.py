from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel


def exe_spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("spark sql hint test") \
        .getOrCreate()
    # sc = spark.sparkContext

    spark.read.json("data.json").createOrReplaceTempView("t")


if __name__ == "__main__":
    exe_spark()
