from pyspark.sql.functions import col, expr, window
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.functions import rand

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

def _create_data(spark, config):
    return( spark.readStream.format( "rate" ) \
            .option( "rowsPerSecond", 1 ) \
            .load() )

def _transform_data(raw_df):
    df = raw_df.withColumn( "customer_id", ( rand( seed=42 ) * 10 ).cast( "int" ) ) \
            .withColumn( "product_id", ( rand( seed=42 ) * 10 ).cast( "int" ) ) \
            .withColumn( "store_id", ( rand( seed=42 ) * 10 ).cast( "int" ) ) \
            .withColumn( "quantity", ( rand( seed=42 ) * 10 ).cast( "int" ) ) \
            .withColumnRenamed( "value", "id" ) 
    return( df.withColumn( "unit_price", ( df[ "product_id" ] + 1 ).cast( "float" ) ) )

def _load_data(config, stream):
    stream.writeStream \
            .outputMode("append") \
            .format("iceberg") \
            .option("path", "my_catalog.my_ns.my_retail_raw_evets" ) \
            .option("checkpointLocation", "checkpoint-loc-iceberg-raw2") \
            .trigger(processingTime='3 minutes') \
            .option( "fanout-enabled", "true" ) \
            .start() \
            .awaitTermination()
                  
def run_job(spark, config):
    spark.sql("""CREATE TABLE IF NOT EXISTS my_catalog.my_ns.my_retail_raw_evets ( 
                                       timestamp timestamp,
                                       id long, 
                                       customer_id int,
                                       product_id int,
                                       store_id int,
                                       quantity int,
                                       unit_price float )
            USING iceberg
            PARTITIONED BY (product_id)
            LOCATION 's3://iceberg-bucket/my/key/prefix/my_ns.db/my_retail_raw_evets'
            TBLPROPERTIES( 
            'serializationLib' = 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            'inputFormat' = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'outputFormat' = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat')""")
    _load_data(config, _transform_data(_create_data(spark, config)))
