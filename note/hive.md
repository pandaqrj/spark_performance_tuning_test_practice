# HIVE优化
## 执行计划:
`EXPLAIN EXTENDED {SQL}`

## 小表关联大表 - `MAPJOIN`
设置参数：  
```sql
SET hive.auto.convert.join=true; --默认true
SET hive.mapjoin.smalltable.filesize=25000000;
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
+ ### 1.NULL值过滤
    使用场景：
    - 非 INNER JOIN
    - 不需要字段为 NULL 的  
    
    ```sql
    --未过滤空值：
    SELECT A.* FROM A LEFT JOIN B ON A.ID = B.ID;
    
    --过滤空值：
    SELECT A.* FROM ( SELECT * FROM A WHERE A.ID IS NOT NULL ) AS A LEFT JOIN B ON A.ID = B.ID;
    ```

+ ### 2. NULL 值转换
    
    如果结果需要为 NULL 值，则不能进行 NULL 值过滤。
    但如果 NULL 值过多，则会在reduce阶段（spark就是shuflle阶段）发送到同一个reducer里面处理，则会造成数据倾斜！
    因此在这种情况下则需要进行 NULL 值转换
    ```sql
    -- 未 NULL 值转化：
    SELECT A.* FROM A LEFT JOIN B ON A.ID = B.ID;
    
    -- NULL 值转化（注意转化的值不能有与原 KEY 值相同的结果）：
    SELECT A.* FROM A LEFT JOIN B ON NVL(A.ID, RAND()) = B.ID;
    ```
    如果使用 Spark3.0以上版本时，则开启
    ```sql
    SET spark.sql.adaptive.enabled=true;
    SET spark.sql.adaptive.skewJoin.enabled=true; -- 默认true
    SET spark.sql.adaptive.skewJoin.skewedPartitionFactor=10; --默认
    SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB; --默认
    ```
    就会自动对倾斜的 JOIN 进行处理

+ ### 3. 分桶 Sort Merge Buket Join
    需要再`MAPJOIN`条件下才能进行SMB JOIN，不过MAPJOIN已经默认开启。不用再指定了
    ```sql
    SET hive.auto.convert.join=true; -- 默认true
    SET hive.optimize.bucketmapjoin=true;
    SET hive.optimize.bucketmapjoin.sortedmerge=true;
    ```
    
    然后建立分桶表
    ```sql
    -- 注意：关联字段应该和分桶字段及排序字段一致！
    -- 分桶个数最好不超过集群CPU个数。
    CREATE TABLE t1 (……) CLUSTERED BY (col_1) SORTED BY (col_1) INTO {buckets_Nums} BUCKETS;
    CREATE TABLE t2 (……) CLUSTERED BY (col_1) SORTED BY (col_1) INTO {buckets_Nums} BUCKETS;
    
       EXPLAIN EXTENDED
        SELECT t1.*
          FROM t1
     LEFT JOIN t2
            on t1.col_1 = t2.col_1
        ;
    ```
    
    如果 SMB JOIN 生效，执行计划中会在 MAP 侧中生成`Sorted Merge Bucket Map Join Operator`
    否则还是`Reduce Join`

## `GROUP BY` 聚合的时候发生数据倾斜

1.在MAP侧进行预聚合：
```sql
SET hive.map.aggr=true;
```

2.在MAP侧预聚合的数据条数：
```sql
SET hive.groupby.mapaggr.checkinterval=100000
```

3.开启倾斜负载均衡：
```sql
SET hive.groupby.skewindata=true;
```
开启后会产生两个MR，所以在数据量小的情况下不划算


## `COUNT(DISTINCT COL)` 去重统计
由于`COUNT(DISTINCT COL)`需要再一个reducer中完成，数据量大的情况下就会导致JOB很难完成。  
一般`COUNT(DISTINCT COL)`会用先`GROUP BY COL`再`COUNT(COL)`的方式来替换。因此需要注意`GROUP BY`的数据倾斜问题。
```sql
-- 直接用 COUNT(DISTINCT COL)
SELECT COUNT(DISTINCT COL) AS C FROM T;

-- 先 GROUP BY COL ，再 COUNT(COL)
SELECT COUNT(COL) AS C FROM (SELECT COL FROM T GROUP BY COL);
```
但是这样会产生两个MR任务，因此在数据量小的情况下并不合算

## 笛卡尔积
应该避免笛卡尔积，当连接条件无效或存在连接条件的时候就会产生笛卡尔积！  
Hive只会用一个Reducer来计算笛卡尔积

# 行列过滤
+ 列过滤: `SELECT`中，应该只拿需要的列，如果有分区，应该尽量使用分区过滤，少用`SELECT *`。
+ 行过滤：当使用外连接中，如果将副表的过滤条件写在`WHERE`后面，那么会先全表关联之后再过滤。
    ```sql
    -- 先关联再过滤
        SELECT T1.ID
              ,T2.NAME 
          FROM T1 
     LEFT JOIN T2 
            ON T1.ID = T2.ID 
         WHERE T1.ETL_DATE = DATE'2020-01-01'
    ;
    
    -- 先过滤再关联
        SELECT T1.ID
              ,T2.NAME 
          FROM (SELECT T1.ID FROM T1 WHERE T1.ETL_DATE = DATE'2020-01-01') T1 
     LEFT JOIN T2
            ON T1.ID = T2.ID
    ;
    ```
  `注意`如果关联字段和过滤字段相同，那么SQL会自动进行谓词下推进行优化。
  但复杂SQL有可能会是的谓词下推失效，因此建议日常写SQL的时候还是进行先过滤再关联。

## 分区

## 分桶

## 合理设置作业数
1. 通常认为一个MAP作业处理一个集群Block大小的数据（默认为128M）最合适。
2. MAP作业数越多越好？  
   如果一个作业处理很多小文件，但是这些小文件都远小于一个Block大小，
   则每个小文件都会被当做一个Block用一个作业来处理，当作业数多起来的时候，
   每一个作业启动和初始化的时间则会远远大于逻辑计算的时间，就会造成资源浪费。
   而且同一时间可执行的作业数是有限的。
3. 每个MAP作业处理一个Block大小就高枕无忧了？  
   不一定，因为如果一行数据的大小是1B，那么128MB就有一亿多行的数据！
   这时候就需要对作业处理的文件大小设置进行缩小。
   
+ ### 复杂文件增加Map数
    增加 map 的方法为：根据`computeSliteSize(Math.max(minSize,Math.min(maxSize,blocksize)))=blocksize=128M`公式，调整 maxSize 最大值。让 maxSize 最大值低于 blocksize 就可以增加 map 的个数。
    ```sql
    -- 设置最大切片值为4MB
    SET mapreduce.input.fileinputformat.split.maxsize=4194304;
    SELECT COUNT(*) AS ROW_NUM FROM T1
    ```
+ ### 小文件的合并
    + 在MAP执行前合并小文件，减少MAP数
        ```sql
        -- 默认
        SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
        ```
    + MR任务结束时合并小文件
        ```sql
        -- MAP-ONLY任务结束时合并小文件，默认true
        SET hive.merge.mapfiles=true;
       
        -- MR任务结束时合并小文件，默认false
        SET hive.merge.mapredfiles=true;
       
        -- 合并文件的大小，默认256MB
        SET hive.merge.size.per.task=268435456;
       
        -- 当输出文件平均值小于此值，则启动一个MR任务进行合并
        SET hive.merge.smallfiles.avgsize=16777216;
        ```
+ ### 合理设置Reduce数
    + #### 调整reduce数量
        + 修改hive配置
            ```sql
            -- 每个reduce处理数据的大小，默认256MB
            SET hive.reducers.bytes.per.reducer=256000000;
            
            --每个任务最大reduce数，默认1009
            SET hive.exec.reducers.max=1009;
            ``` 
            计算recuder数的公式：
        `min(hive.exec.reducers.max, {总数据量}/hive.reducers.bytes.per.reducer)`  
      
        + 在`mapred-default.xml`文件中修改：`SET mapred.join.reduces=15;`（默认为-1）
    
        + 只有`mapred.join.reduces=-1`时修改hive的配置才能生效，否则将以`mapred-default.xml`文件中写死的个数为准。
    + #### reduce个数越多越好？
        + 过多的启动和初始化reduce也会消耗时间和资源。
        + 有多少reduce就会输出文件。

    在设置reduce个数需考虑两个原则：
    1. 处理数据量大的reduce数量要合适。
    2. 单个reduce处理的数据量大小要合适。
    
## 并行执行
在共享集群中设置并发执行可以提高运行速度。
```sql
SET hive.exec.parallel=true; -- 打开任务并行执行
SET hive.exec.parallel.thread.number=16; -- 同一个 sql 允许最大并行度，默认为 8。
```
当然，得是在系统资源比较空闲的时候才有优势，否则，没资源，并行也起不来。