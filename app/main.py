import json
import importlib
import argparse
from pyspark.sql import SparkSession


def _parse_arguments():
    """ Parse arguments provided by spark-submit commend"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True)
    parser.add_argument("--app_name", required=True)
    parser.add_argument("--source_data_path", required=True)
    parser.add_argument("--output_data_path", required=True)
    return parser.parse_args()


def main():
    """ Main function excecuted by spark-submit command"""
    args = _parse_arguments()

    #with open("./config.json", "r") as config_file: 
    #    config = json.load(config_file)

    config_set = {"app_name": args.app_name, "source_data_path": args.source_data_path, "output_data_path":args.output_data_path}
    config = json.loads(json.dumps(config_set))

    spark = SparkSession.builder.appName(config.get("app_name")). \
        config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog" ). \
        config("spark.sql.catalog.my_catalog.warehouse", "s3://iceberg-bucket/my/key/prefix/" ). \
        config("spark.sql.catalog.my_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog" ). \
        config("spark.sql.catalog.my_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO" ). \
        config("spark.sql.catalog.my_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager" ). \
        config("spark.sql.catalog.my_catalog.lock.table", "myGlueLockTable" ). \
        getOrCreate()
    job_module = importlib.import_module(f"jobs.{args.job}")
    job_module.run_job(spark, config)

if __name__ == "__main__":
    main()
