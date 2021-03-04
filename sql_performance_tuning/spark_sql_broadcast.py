from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel


def exe_spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("spark sql hint test") \
        .getOrCreate()
    # sc = spark.sparkContext

    spark.read.json("data.json").createOrReplaceTempView("t")

    """
    -- 广播关联Hints:
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
    
    -- When different join strategy hints are specified on both sides of a join, Spark
    -- prioritizes the BROADCAST hint over the MERGE hint over the SHUFFLE_HASH hint
    -- over the SHUFFLE_REPLICATE_NL hint.
    -- Spark will issue Warning in the following example
    -- org.apache.spark.sql.catalyst.analysis.HintErrorLogger: Hint (strategy=merge)
    -- is overridden by another hint and will not take effect.
    SELECT /*+ BROADCAST(t1), MERGE(t1, t2) */ * FROM t1 INNER JOIN t2 ON t1.key = t2.key;
    """


if __name__ == "__main__":
    exe_spark()
