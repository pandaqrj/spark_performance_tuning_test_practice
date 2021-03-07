from pyspark.sql import SparkSession


def exe_spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("sql test1") \
        .getOrCreate()

    spark.read.parquet("spark-warehouse/order_detail").createOrReplaceTempView("oder_detail")

    SQL = """
        SELECT T1.province AS AREA
              ,SUBSTR(T1.date, 0, 7) AS MON
              ,COUNT(1) AS ORDER_COUNT
              ,SUM(T1.amount) AS ORDER_AMOUNT
          FROM oder_detail T1
         WHERE SUBSTR(T1.date, 0, 7) = '2021-01'
      GROUP BY T1.province
              ,SUBSTR(T1.date, 0, 7)
    """
    spark.sql(SQL).show(truncate=False)


if __name__ == '__main__':
    exe_spark()
