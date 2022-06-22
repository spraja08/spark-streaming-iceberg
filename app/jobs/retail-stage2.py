from pyspark.sql.functions import col, expr, window, lit
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.functions import rand

def _create_data(spark, config):
    rate_df = spark.readStream.format("rate") \
        .option("rowsPerSecond", 1) \
        .load()
    raw_df = rate_df.withColumn("customer_id", (rand(seed=42) * 10).cast("int")) \
        .withColumn("product_id", (rand(seed=42) * 10).cast("int")) \
        .withColumn("store_id", (rand(seed=42) * 10).cast("int")) \
        .withColumn("quantity", (rand(seed=42) * 10).cast("int")) \
        .withColumnRenamed("value", "id")
    return(raw_df.withColumn("unit_price", (raw_df["product_id"] + 1).cast("float")))

def _transform_data(df):
    return( df
            .withWatermark("timestamp", "3 minutes")
            .groupBy( \
                window(col("timestamp"), "30 minutes"), 
                col("product_id")) 
            .agg(_sum("quantity").alias("total_quantity")) 
            .withColumn( "insight_type", lit("product") ) 
            .withColumn( "store_id", lit(-1) )) 

def _load_data(config, aggregated_stream):
    result = aggregated_stream.select("product_id", "store_id", "insight_type",
                            aggregated_stream.window.start.alias("start"),
                            aggregated_stream.window.end.alias("end"),
                            "total_quantity") 
    result.writeStream \
        .outputMode("complete") \
        .format("iceberg") \
        .option("path", "my_catalog.my_ns.my_retail_agg_table") \
        .option("checkpointLocation", "retail-agg-loc-iceberg-prod-1") \
        .trigger(processingTime='5 minutes') \
        .option("fanout-enabled", "true") \
        .start() \
        .awaitTermination()

def run_job(spark, config):
    spark.sql("""CREATE TABLE IF NOT EXISTS my_catalog.my_ns.my_retail_agg_table ( 
                                       product_id int,
                                       store_id int,
                                       insight_type string,
                                       start timestamp, 
                                       end timestamp,
                                       total_quantity long)
            USING iceberg
            PARTITIONED BY (product_id, store_id, start)           
            LOCATION 's3://iceberg-bucket/my/key/prefix/my_ns.db/my_retail_agg_table'
            TBLPROPERTIES( 
            'serializationLib' = 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            'inputFormat' = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'outputFormat' = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'format-version' = '2')""")
    _load_data(config, _transform_data(_create_data(spark, config)))
