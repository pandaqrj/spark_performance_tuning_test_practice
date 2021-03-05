from pyspark.sql import SparkSession


def exe_spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("spark sql hint test") \
        .getOrCreate()
    # sc = spark.sparkContext

    spark.read.json("../data1.json").createOrReplaceTempView("t1")
    spark.read.json("../data2.json").createOrReplaceTempView("t2")

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

    # 在实际运用中，对小表进行广播，可以增加计算性能
    # t1 订单表， t2 用户维度
    # 对于right outer join 只能广播左表；
    # 对于left outer join，left semi join，left anti join，internal join等只能广播右表；
    # inner join 可以指定广播
    spark.sql("""
        SELECT /*+ MAPJOIN(t2) */
               t2.area
              ,COUNT(DISTINCT t1.id) as order_count
              ,SUM(t1.amount) as order_amount
          FROM t2
          JOIN t1
            on t1.user = t2.user
      GROUP BY t2.area
    """).show()
    """
    执行计划显示，BuildLeft对左边也就是t2进行了广播，否则默认是会对右边进行广播，hint生效
    == Physical Plan ==
    *(4) HashAggregate(keys=[area#22], functions=[count(distinct id#9L)], output=[area#22, order_count#28L])
    +- Exchange hashpartitioning(area#22, 200), true, [id=#77]
       +- *(3) HashAggregate(keys=[area#22], functions=[partial_count(distinct id#9L)], output=[area#22, count#34L])
          +- *(3) HashAggregate(keys=[area#22, id#9L], functions=[], output=[area#22, id#9L])
             +- Exchange hashpartitioning(area#22, id#9L, 200), true, [id=#72]
                +- *(2) HashAggregate(keys=[area#22, id#9L], functions=[], output=[area#22, id#9L])
                   +- *(2) Project [area#22, id#9L]
                      +- *(2) BroadcastHashJoin [user#24], [user#10], Inner, BuildLeft
                         :- BroadcastExchange HashedRelationBroadcastMode(List(input[1, string, true])), [id=#63]
                         :  +- *(1) Project [area#22, user#24]
                         :     +- *(1) Filter isnotnull(user#24)
                         :        +- FileScan json [area#22,user#24] Batched: false, DataFilters: [isnotnull(user#24)], Format: JSON, Location: InMemoryFileIndex[file:/C:/Users/panda/Desktop/WORK/sp/spark_practice/data2.json], PartitionFilters: [], PushedFilters: [IsNotNull(user)], ReadSchema: struct<area:string,user:string>
                         +- *(2) Project [id#9L, user#10]
                            +- *(2) Filter isnotnull(user#10)
                               +- FileScan json [id#9L,user#10] Batched: false, DataFilters: [isnotnull(user#10)], Format: JSON, Location: InMemoryFileIndex[file:/C:/Users/panda/Desktop/WORK/sp/spark_practice/data1.json], PartitionFilters: [], PushedFilters: [IsNotNull(user)], ReadSchema: struct<id:bigint,user:string>
    """


if __name__ == "__main__":
    exe_spark()
