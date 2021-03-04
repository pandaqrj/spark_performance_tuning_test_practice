from pyspark.sql import SparkSession


def exe_spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("spark sql hint test") \
        .getOrCreate()
    # sc = spark.sparkContext
    """
        SELECT /*+ COALESCE(3) */ * FROM t
        SELECT /*+ REPARTITION(3) */ * FROM t
        SELECT /*+ REPARTITION(c) */ * FROM t
        SELECT /*+ REPARTITION(3, c) */ * FROM t
        SELECT /*+ REPARTITION_BY_RANGE(c) */ * FROM t
        SELECT /*+ REPARTITION_BY_RANGE(3, c) */ * FROM t
    """


if __name__ == "__main__":
    exe_spark()
