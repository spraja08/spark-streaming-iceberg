from pyspark.sql.functions import col, expr, window
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamWriter

def _extract_data(spark, config):
    schema = StructType([StructField("id", IntegerType()),
                       StructField("InvoiceNo", IntegerType()),
                       StructField("StockCode", IntegerType()),
                       StructField("Description", StringType()),
                       StructField("Quantity", FloatType()),
                       StructField("InvoiceDate", StringType()),
                       StructField("UnitPrice", FloatType()),
                       StructField("CustomerID", StringType()),
                       StructField("Country", StringType()),
                       StructField("InvoiceTimestamp", TimestampType())])
    
    return (spark.readStream
        .option("maxFilesPerTrigger", "5")
        .option("header", "true")
        .schema(schema)
        .format("csv")
        .option("path", config.get('source_data_path'))
        .load())


def _transform_data(raw_df):
    transformed_stream = raw_df.select(
        col("CustomerID"), 
        col("StockCode"), 
        col("InvoiceTimestamp"),
        col("Quantity"),
        col("UnitPrice"))
    return (transformed_stream 
            .withWatermark("InvoiceTimestamp", "3 minutes") 
            .groupBy(  
                    window( col("InvoiceTimestamp"), "10 minutes"), 
                    col( "StockCode" ) ) 
            .agg( _sum( "Quantity" ).alias( "total_sales")))


def _load_data(config, aggregated_stream):
    result = aggregated_stream.select("StockCode", 
                aggregated_stream.window.start.cast("string").alias("start"), 
                aggregated_stream.window.end.cast("string").alias("end"), 
                "total_sales")

    result.writeStream \
            .outputMode("append") \
            .format("iceberg") \
            .option("path", "my_catalog.my_ns.my_retail_agg_table" ) \
            .option("checkpointLocation", "checkpoint-loc-iceberg-xyz7") \
            .trigger(processingTime='1 minutes') \
            .start() \
            .awaitTermination()
                  
def run_job(spark, config):
    spark.sql("""CREATE TABLE IF NOT EXISTS my_catalog.my_ns.my_retail_agg_table ( 
                                       StockCode int,
                                       start string, 
                                       end string,
                                       total_sales double)
            USING iceberg
            LOCATION 's3://iceberg-bucket/my/key/prefix/my_ns.db/my_retail_agg_table'
            TBLPROPERTIES( 
            'serializationLib' = 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            'inputFormat' = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'outputFormat' = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat')""")
    _load_data(config, _transform_data(_extract_data(spark, config)))
