from pyspark.sql import SparkSession


def exe_spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("spark sql hint test") \
        .getOrCreate()
    # sc = spark.sparkContext

    spark.read.json("data.json").createOrReplaceTempView("t")

    # 重分区和合并
    """
    SELECT /*+ COALESCE(3) */ * FROM t;
    SELECT /*+ REPARTITION(3) */ * FROM t;
    SELECT /*+ REPARTITION(c1) */ * FROM t;
    SELECT /*+ REPARTITION(3, c1) */ * FROM t;
    SELECT /*+ REPARTITION_BY_RANGE(c1) */ * FROM t;
    SELECT /*+ REPARTITION_BY_RANGE(3, c1) */ * FROM t;
    """

    # 重分区，不会shuffle, 是将同一个节点上的多个分区进行重分区
    # 一般用于减少分区从而减少spark-sql产生的大量小文件
    # 但是在数据量较大的情况下会降低计算速度
    spark.sql(
        """
            SELECT /*+ COALESCE(3) */ 
                   * 
              FROM t
        """
    ).show()

    # 重分区，会shuffle数据以实现负载均衡，一般用于增加分区数以提高并行度
    # 在执行join操作或者cache方法之前调用会产生不错的效果
    # 但是要注意shuffle的开销
    spark.sql(
        """
            SELECT /*+ REPARTITION(3) */ 
                   * 
              FROM t
        """
    ).show()


if __name__ == "__main__":
    exe_spark()
