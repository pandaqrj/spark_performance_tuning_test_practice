from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, ArrayType

url_ol = "jdbc:mysql://10.66.137.124:3306/points_mall?user=dw_read&password=G42IMZ1tbsZ@4OZq"
url_dw = "jdbc:mysql://10.66.147.66:3306/dw?user=bi&password=JwAkQEik9yW$hxJU"


def streaming_run():
    spark = SparkSession \
        .builder \
        .appName("STRUCTURED_STREAMING_TONGDUIBA_BEHAVIOUR_LOG") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "10.104.135.20:9092") \
        .option("subscribe", "points_mall.behaviour_log") \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    schema1 = StructType() \
        .add("data", StringType())

    schema2 = StructType(
        [
            StructField("logId", StringType())
            , StructField("itemCode", StringType())
            , StructField("supplierId", StringType())
            , StructField("appId", StringType())
            , StructField("ext", StringType())
            , StructField("createdTime", StringType())
            , StructField("updatedTime", StringType())
        ]
    )

    schema3 = ArrayType(schema2)

    df.select(from_json("value", schema1).alias("data")).select("data.*") \
        .select(explode(from_json("data", schema3))).select("col.*") \
        .createOrReplaceTempView("behaviour_log")

    spark.read.jdbc(url=url_ol, table="supplier_account") \
        .createOrReplaceTempView("supplier_account")

    spark.read.jdbc(url=url_ol, table="supplier_behaviour_item_code") \
        .createOrReplaceTempView("supplier_behaviour_item_code")

    spark.read.jdbc(url=url_ol, table="app") \
        .createOrReplaceTempView("app")

    spark.read.jdbc(url=url_dw, table="DIM_CRM_UNION_INFO") \
        .createOrReplaceTempView("DIM_CRM_UNION_INFO")

    SQL = f"""
        SELECT M2.supplierId AS SUPPLIER_ID
              ,M4.companyName AS COMPANY_NAME
              ,M4.mobile AS MOBILE
              ,FROM_UNIXTIME(M4.createdAt) AS SUPPLIER_REG_TIME
              ,M5.name AS APP_NAME
              ,M2.itemCode AS ITEM_CODE
              ,M3.item_name AS ITEM_NAME
              ,M2.ext as ITEM_EXT
              ,M2.createdTime AS CREATE_UNIXTIME
              ,FROM_UNIXTIME(M2.createdTime) AS CREATE_TIME
              ,M6.UNION_MANAGER AS MANAGER
              ,M6.UNION_MANAGER_TEAM AS MANAGER_TEAM
          FROM behaviour_log M2
     LEFT JOIN supplier_behaviour_item_code M3
            ON M2.itemCode = M3.item_code
     LEFT JOIN supplier_account M4
            ON M2.supplierId = M4.supplierId
     LEFT JOIN app M5
            ON M2.appId = M5.appId
     LEFT JOIN DIM_CRM_UNION_INFO M6
            ON M2.supplierId = M6.SUPPLIER_ID
    """

    df1 = spark.sql(SQL)

    query = df1 \
        .writeStream \
        .option("checkpointLocation", "/home/quanruijia/dw_shell/py/spark/checkpoint/") \
        .foreachBatch(process_batch) \
        .start()

    # query = words \
    #       .writeStream \
    #       .outputMode("append") \
    #       .format("console") \
    #       .start()

    query.awaitTermination()


def process_batch(batch_df, batch_id):
    # batch_df.show()
    batch_df.write.jdbc(url=url_dw, table="dw.DM_STREAM_TONGDUIBA_USER_BEHAVIOR_REPORT", mode="append")


if __name__ == '__main__':
    streaming_run()
