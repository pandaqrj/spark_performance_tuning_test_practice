from pyspark.sql import SparkSession


def exe_spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("spark sql hint test") \
        .getOrCreate()
    # sc = spark.sparkContext

    spark.read.json("../data1.json").createOrReplaceTempView("t")

    """
    -- Join Hints for broadcast join
    SELECT /*+ BROADCAST(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;
    SELECT /*+ BROADCASTJOIN (t1) */ * FROM t1 left JOIN t2 ON t1.key = t2.key;
    SELECT /*+ MAPJOIN(t2) */ * FROM t1 right JOIN t2 ON t1.key = t2.key;
    
    -- Join Hints for shuffle sort merge join
    SELECT /*+ SHUFFLE_MERGE(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;
    SELECT /*+ MERGEJOIN(t2) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;
    SELECT /*+ MERGE(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;
    
    -- Join Hints for shuffle hash join
    SELECT /*+ SHUFFLE_HASH(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;
    
    -- Join Hints for shuffle-and-replicate nested loop join
    SELECT /*+ SHUFFLE_REPLICATE_NL(t1) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;
    
    -- 多个hints同时指定时，有以下优先级：
    -- BROADCAST > MERGE > SHUFFLE_HASH > SHUFFLE_REPLICATE_NL
    -- 下面的例子会Warning：org.apache.spark.sql.catalyst.analysis.HintErrorLogger: Hint (strategy=merge)
    -- MERGE会被覆盖，不会生效
    SELECT /*+ BROADCAST(t1), MERGE(t1, t2) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;
    """


if __name__ == "__main__":
    exe_spark()
