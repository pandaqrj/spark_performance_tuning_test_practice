# 需要优化的原因
1. 性能差
2. 执行时间长
3. SQL语句欠佳
4. 索引失效
5. 服务器参数设置不合理（缓冲、线程数）

# SQL的解析过程：
```SQL
-- 语句：
SELECT DISTINCT ... FROM ... JOIN ... ON ... WHERE GROUP BY HAVING ORDER BY LIMIT

-- 解析过程 FROM -> ON -> JOIN -> WHERE -> GROUP BY -> HAVING -> SELECT DISTINCT -> ORBDER BY -> LIMIT
```
# 优化：
## 索引 index
`索引`是帮助数据库高效获取数据的`数据结构`（树：B+树、Hash树...）。  
主键索引（不能为NULL的唯一索引）  
唯一索引  
单值索引    
符合索引
```SQL
-- 创建
CREATE [UNIQUE] INDEX XXX_INDEX ON XXX_TABLE(COL1[,COL2,COL3...]);
ALTER TABLE XXX_TABLE ADD [UNIQUE] INDEX XXX_INDEX(COL1[,COL2,COL3...]);

-- 删除
DROP INDEX XXX_INDEX ON XXX_TABLE;
```
+ ### 索引的弊端：
    1. 索引本身很大。
    2. 所以不是所有情况都适用：
        - 少量数据
        - 频繁更新的字段
        - 很少筛选的字段
    3. 索引会降低增删改查的效率。

+ ### 索引的优势：
    1. 提高查询效率（降低IO）
    2. 增加排序效率（B+树已经是一个排序的数据结构）

## 执行计划
```SQL
EXPLAIN {SQL}
```
