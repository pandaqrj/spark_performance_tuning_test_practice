from pyspark.sql import SparkSession


def exe_spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("sparksql hint test") \
        .getOrCreate()
    sc = spark.sparkContext


if __name__ == "__main__":
    exe_spark()
