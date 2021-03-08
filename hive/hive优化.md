# HIVE优化

## 执行计划:
EXPLAIN EXTEND {SQL}

## 小表关联大表 - MAP JOIN
设置参数：  
```
默认开启
hive.auto.convert.join=true
hive.mapjoin.smalltable.filesize=25000000
```
这样就会自动优化，不需要在使用hint指定MAP JOIN
```sql
    SELECT /*+ MAPJOIN(T1) */ --最新版本的HIVE自动会选择MAP JOIN的表，不再需要hint指定
           T1.ID
          ,T2.NAME
      FROM T1
 LEFT JOIN T2
        ON T1.ID = T2.ID
```

## 大表关联大表
### 1. NULL值过滤

使用场景：  
1. 非 INNER JOIN
2. 不需要字段为 NULL 的  

```sql
--未过滤空值：
SELECT A.* FROM A LEFT JOIN B ON A.ID = B.ID

--过滤空值：
SELECT A.* FROM ( SELECT * FROM A WHERE A.ID IS NOT NULL ) AS A LEFT JOIN B ON A.ID = B.ID
```

### 2. NULL 值转换

如果结果需要为 NULL 值，则不能进行 NULL 值过滤。
但如果 NULL 值过多，则会在reduce阶段（spark就是shuflle阶段）发送到同一个reducer里面处理，则会造成数据倾斜！
因此在这种情况下则需要进行 NULL 值转换

```sql
-- 未 NULL 值转化：
SELECT A.* FROM A LEFT JOIN B ON A.ID = B.ID

-- NULL 值转化（注意转化的值不能有与原 KEY 值相同的结果）：
SELECT A.* FROM A LEFT JOIN B ON NVL(A.ID, RAND()) = B.ID
```
  
如果使用 Spark3.0以上版本时，则开启
```
spark.sql.adaptive.enabled=true
spark.sql.adaptive.skewJoin.enabled=true（默认开启）
spark.sql.adaptive.skewJoin.skewedPartitionFactor=10（默认）
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB（默认）
```
就会自动对倾斜的 JOIN 进行处理

### 3. 分桶 Sort Merge Buket Join
需要再`MAPJOIN`条件下才能进行SMB JOIN，不过MAPJOIN已经默认开启。不用再指定了
```
hive.auto.convert.join=true (mapjoin默认开启)
hive.optimize.bucketmapjoin = true
hive.optimize.bucketmapjoin.sortedmerge = true
```

然后建立分桶表
```sql
-- 注意：关联字段应该和分桶字段及排序字段一致！
-- 分桶个数最好不超过集群CPU个数。
CREATE TABLE t1 (……) CLUSTERED BY (col_1) SORTED BY (col_1) INTO {buckets_Nums} BUCKETS;
CREATE TABLE t2 (……) CLUSTERED BY (col_1) SORTED BY (col_1) INTO {buckets_Nums} BUCKETS;

   EXPLAIN EXTEND
    SELECT t1.*
      FROM t1
 LEFT JOIN t2
        on t1.col_1 = t2.col_1
    ;
```

如果 SMB JOIN 生效，执行计划中会在 MAP 侧中生成`Sorted Merge Bucket Map Join Operator`
否则还是`Reduce Join`

## GROUP BY 聚合的时候发生数据倾斜
```
1.在MAP侧进行预聚合：
hive.map.aggr=true（默认开启）

2.在MAP侧预聚合的数据条数：
hive.groupby.mapaggr.checkinterval=100000

3.开启倾斜负载均衡：
hive.groupby.skewindata=true（会产生两个MR，所以在数据量小的情况下不划算）
```