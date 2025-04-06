from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

from pyspark.sql.functions import col


def getSpark():
    NESSIE_URI = "http://localhost:19120/api/v1"
    WAREHOUSE = "s3a://warehouse/"

    AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    AWS_S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT")

    spark = (
        SparkSession.builder.appName("MinIO + Iceberg + Nessie")
        .config("spark.jars.packages", ",".join([
            'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,'
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,'
            'org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.67.0,'
            'software.amazon.awssdk:bundle:2.17.178,'
            'software.amazon.awssdk:url-connection-client:2.17.178'
        ]))
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", "http://localhost:19120/api/v1")  # Nessie API
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

        # MinIO-specific settings
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")  # Required for MinIO

        # Required for Iceberg AWS client to work with MinIO
        .config("spark.sql.catalog.nessie.s3.endpoint", AWS_S3_ENDPOINT)
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.sql.catalog.nessie.s3.access-key-id", AWS_ACCESS_KEY)
        .config("spark.sql.catalog.nessie.s3.secret-access-key", AWS_SECRET_KEY)

        .getOrCreate()
    )
    return spark


if __name__ == "__main__":
    spark = getSpark()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("SHOW TABLES IN nessie").show(truncate=False)

    # Read the Iceberg table
    df = spark.read.table("nessie.weather")

    # Show a few rows
    df.where(col("location_name")=="New Delhi").show(truncate=False)
