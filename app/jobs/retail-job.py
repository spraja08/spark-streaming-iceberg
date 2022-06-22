from pyspark.sql.functions import col, expr, window, lit
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.functions import rand

def add_retail_columns(rate_df):
    raw_df = rate_df.withColumn("customer_id", (rand(seed=42) * 10).cast("int")) \
            .withColumn("product_id", (rand(seed=42) * 10).cast("int")) \
            .withColumn("store_id", (rand(seed=42) * 10).cast("int")) \
            .withColumn("quantity", (rand(seed=42) * 10).cast("int")) \
            .withColumnRenamed("value", "id")
    return(raw_df.withColumn("unit_price", (raw_df["product_id"] + 1).cast("float")))

def _create_data(spark):
    rate_df = spark.readStream.format("rate") \
        .option("rowsPerSecond", 1) \
        .load()
    return add_retail_columns(rate_df)

def _transform_product_data(df, windowPeriod):
    return( df
            .withWatermark("timestamp", "3 minutes")
            .groupBy( \
                window(col("timestamp"), windowPeriod), 
                col("product_id")) 
            .agg(_sum("quantity").alias("total_quantity")) 
            .withColumn( "insight_type", lit("product") ) 
            .withColumn( "store_id", lit(-1) )) 

def _transform_store_data(df, windowPeriod):
    return( df 
            .withWatermark("timestamp", "3 minutes") 
            .groupBy( 
                window(col("timestamp"), windowPeriod), 
                col("store_id")) 
            .agg(_sum("quantity").alias("total_quantity")) 
            .withColumn( "insight_type", lit("store") ) 
            .withColumn( "product_id", lit(-1) ) )

def _load_data(config, aggregated_stream, table, triggerInterval):
    result = aggregated_stream.select("product_id", "store_id", "insight_type",
                            aggregated_stream.window.start.alias("start"),
                            aggregated_stream.window.end.alias("end"),
                            "total_quantity") 
    result.writeStream \
        .outputMode("complete") \
        .format("iceberg") \
        .option("path", table) \
        .option("checkpointLocation", table + "-10") \
        .trigger(processingTime=triggerInterval) \
        .option("fanout-enabled", "true") \
        .start() 

def _create_tables(spark):
    spark.sql("""CREATE TABLE IF NOT EXISTS my_catalog.my_ns.my_product_agg_table ( 
                                       product_id int,
                                       store_id int,
                                       insight_type string,
                                       start timestamp, 
                                       end timestamp,
                                       total_quantity long)
            USING iceberg
            PARTITIONED BY (product_id, start)           
            LOCATION 's3://iceberg-bucket/my/key/prefix/my_ns.db/my_product_agg_table'
            TBLPROPERTIES( 
            'serializationLib' = 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            'inputFormat' = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'outputFormat' = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'format-version' = '2')""")
    
    spark.sql("""CREATE TABLE IF NOT EXISTS my_catalog.my_ns.my_store_agg_table ( 
                                       product_id int,
                                       store_id int,
                                       insight_type string,
                                       start timestamp, 
                                       end timestamp,
                                       total_quantity long)
            USING iceberg
            PARTITIONED BY (store_id, start)           
            LOCATION 's3://iceberg-bucket/my/key/prefix/my_ns.db/my_store_agg_table'
            TBLPROPERTIES( 
            'serializationLib' = 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            'inputFormat' = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'outputFormat' = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'format-version' = '2')""")   

    spark.sql("""CREATE TABLE IF NOT EXISTS my_catalog.my_ns.my_retail_agg_table ( 
                                       product_id int,
                                       store_id int,
                                       insight_type string,
                                       start timestamp, 
                                       end timestamp,
                                       total_quantity long)
            USING iceberg
            PARTITIONED BY (start)           
            LOCATION 's3://iceberg-bucket/my/key/prefix/my_ns.db/my_retail_agg_table'
            TBLPROPERTIES( 
            'serializationLib' = 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            'inputFormat' = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'outputFormat' = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'format-version' = '2')""")  

def _unify_data(spark, config):
    store = spark.readStream.format("iceberg").load("my_catalog.my_ns.my_store_agg_table")
    product = spark.readStream.format("iceberg").load("my_catalog.my_ns.my_product_agg_table")
    retail = store.union(product)
    retail.writeStream \
        .outputMode("append") \
        .format("iceberg") \
        .option("path", "my_catalog.my_ns.my_retail_agg_table") \
        .option("checkpointLocation", "my_catalog.my_ns.my_retail_agg_table-10" ) \
        .trigger(processingTime='5 minutes') \
        .option("fanout-enabled", "true") \
        .start() 

def run_job(spark, config):
    _create_tables(spark)
    retail_stream = _create_data(spark)
    _load_data(config, _transform_store_data(retail_stream, "5 minutes"), "my_catalog.my_ns.my_store_agg_table", "1 minutes")
    _load_data(config, _transform_product_data(retail_stream, "5 minutes"), "my_catalog.my_ns.my_product_agg_table", "1 minutes")
    #_unify_data(spark, config)
    spark.streams.awaitAnyTermination()