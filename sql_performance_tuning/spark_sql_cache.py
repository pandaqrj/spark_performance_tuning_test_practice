from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel


def exe_spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("spark sql hint test") \
        .getOrCreate()
    # sc = spark.sparkContext

    df = spark.read.json("data.json")

    """
    临时数据存储
    如果计算会重用某个数据集，应该缓存，否则 spark 每次计算将会从源头开始
    临时数据的存储级别：
        MEMORY_ONLY
        MEMORY_ONLY_2
        MEMORY_ONLY_SER (JAVA SCALA)
        MEMORY_ONLY_SER_2 (JAVA SCALA)
        MEMORY_AND_DISK (默认)
        MEMORY_AND_DISK_2
        MEMORY_AND_DISK_SER (JAVA SCALA)
        MEMORY_AND_DISK_SER_2 (JAVA SCALA)
        DISK_ONLY
        DISK_ONLY_2
        OFF_HEAP
    """
    df.persist(storageLevel=StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("t")

    '''
    # 也可以通过SQL方式缓存
    # `CACHE LAZY TABLE`会直到一个action操作才被触发，而`CACHE TABLE`会即时生效
    # persist()也是lazy的
    spark.sql(
        """
            CACHE LAZY TABLE t
        """
    )
    '''

    # 计算1
    spark.sql(
        """
            SELECT count(1) as count
              FROM t
             WHERE id <= 5
        """
    ).show()

    # 计算2 复用了view t
    spark.sql(
        """
            SELECT count(1) as count
              FROM t
             WHERE id >= 6
        """
    ).show()


if __name__ == "__main__":
    exe_spark()
