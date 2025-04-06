import os
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructField, StructType, DoubleType, IntegerType, StringType, LongType
from pyspark import SparkConf
from pyspark.sql import SparkSession


def getSpark():
    NESSIE_URI = "http://localhost:19120/api/v1"
    WAREHOUSE = "s3a://warehouse/"

    AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    AWS_S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT")

    conf = (
        SparkConf()
        .setAppName('app_name')
        .set('spark.jars.packages',
             'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,'
             'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,'
             'org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.67.0,'
             'software.amazon.awssdk:bundle:2.17.178,'
             'software.amazon.awssdk:url-connection-client:2.17.178')
        .set('spark.sql.extensions',
             'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,'
             'org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', NESSIE_URI)
        .set('spark.sql.catalog.nessie.ref', 'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .set('spark.sql.catalog.nessie.s3.endpoint', AWS_S3_ENDPOINT)
        .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .set('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY)
        .set('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY)
        .set('spark.hadoop.fs.s3a.endpoint', AWS_S3_ENDPOINT)
    )
    spark_sess = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark_sess


if __name__ == "__main__":
    CHECKPOINT = "/tmp/spark/checkpoints/weather"
    TABLE = "nessie.weather"
    QUERY = """
                CREATE TABLE IF NOT EXISTS {table} (
                    location_name STRING,
                    location_region STRING,
                    location_country STRING,
                    location_lat DOUBLE,
                    location_lon DOUBLE,
                    location_tz_id STRING,
                    location_localtime_epoch BIGINT,
                    location_localtime STRING,
                    current_last_updated_epoch BIGINT,
                    current_last_updated STRING,
                    current_temp_c DOUBLE,
                    current_temp_f DOUBLE,
                    current_is_day INT,
                    current_condition_text STRING,
                    current_condition_icon STRING,
                    current_condition_code INT,
                    current_wind_mph DOUBLE,
                    current_wind_kph DOUBLE,
                    current_wind_degree INT,
                    current_wind_dir STRING,
                    current_pressure_mb DOUBLE,
                    current_pressure_in DOUBLE,
                    current_precip_mm DOUBLE,
                    current_precip_in DOUBLE,
                    current_humidity INT,
                    current_cloud INT,
                    current_feelslike_c DOUBLE,
                    current_feelslike_f DOUBLE,
                    current_windchill_c DOUBLE,
                    current_windchill_f DOUBLE,
                    current_heatindex_c DOUBLE,
                    current_heatindex_f DOUBLE,
                    current_dewpoint_c DOUBLE,
                    current_dewpoint_f DOUBLE,
                    current_vis_km DOUBLE,
                    current_vis_miles DOUBLE,
                    current_uv DOUBLE,
                    current_gust_mph DOUBLE,
                    current_gust_kph DOUBLE,
                    current_air_quality_co DOUBLE,
                    current_air_quality_no2 DOUBLE,
                    current_air_quality_o3 DOUBLE,
                    current_air_quality_so2 DOUBLE,
                    current_air_quality_pm2_5 DOUBLE,
                    current_air_quality_pm10 DOUBLE,
                    current_air_quality_us_epa_index INT,
                    current_air_quality_gb_defra_index INT,
                    timestamp STRING
                ) USING iceberg
            """.format(table=TABLE)

    spark = getSpark()
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("location_name", StringType(), True),
        StructField("location_region", StringType(), True),
        StructField("location_country", StringType(), True),
        StructField("location_lat", DoubleType(), True),
        StructField("location_lon", DoubleType(), True),
        StructField("location_tz_id", StringType(), True),
        StructField("location_localtime_epoch", LongType(), True),
        StructField("location_localtime", StringType(), True),
        StructField("current_last_updated_epoch", LongType(), True),
        StructField("current_last_updated", StringType(), True),
        StructField("current_temp_c", DoubleType(), True),
        StructField("current_temp_f", DoubleType(), True),
        StructField("current_is_day", IntegerType(), True),
        StructField("current_condition_text", StringType(), True),
        StructField("current_condition_icon", StringType(), True),
        StructField("current_condition_code", IntegerType(), True),
        StructField("current_wind_mph", DoubleType(), True),
        StructField("current_wind_kph", DoubleType(), True),
        StructField("current_wind_degree", IntegerType(), True),
        StructField("current_wind_dir", StringType(), True),
        StructField("current_pressure_mb", DoubleType(), True),
        StructField("current_pressure_in", DoubleType(), True),
        StructField("current_precip_mm", DoubleType(), True),
        StructField("current_precip_in", DoubleType(), True),
        StructField("current_humidity", IntegerType(), True),
        StructField("current_cloud", IntegerType(), True),
        StructField("current_feelslike_c", DoubleType(), True),
        StructField("current_feelslike_f", DoubleType(), True),
        StructField("current_windchill_c", DoubleType(), True),
        StructField("current_windchill_f", DoubleType(), True),
        StructField("current_heatindex_c", DoubleType(), True),
        StructField("current_heatindex_f", DoubleType(), True),
        StructField("current_dewpoint_c", DoubleType(), True),
        StructField("current_dewpoint_f", DoubleType(), True),
        StructField("current_vis_km", DoubleType(), True),
        StructField("current_vis_miles", DoubleType(), True),
        StructField("current_uv", DoubleType(), True),
        StructField("current_gust_mph", DoubleType(), True),
        StructField("current_gust_kph", DoubleType(), True),
        StructField("current_air_quality_co", DoubleType(), True),
        StructField("current_air_quality_no2", DoubleType(), True),
        StructField("current_air_quality_o3", DoubleType(), True),
        StructField("current_air_quality_so2", DoubleType(), True),
        StructField("current_air_quality_pm2_5", DoubleType(), True),
        StructField("current_air_quality_pm10", DoubleType(), True),
        StructField("current_air_quality_us_epa_index", IntegerType(), True),
        StructField("current_air_quality_gb_defra_index", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe", "weather") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = kafka_df.selectExpr(
        "CAST(value AS STRING) AS value",
        "timestamp AS event_time"  # Use timestamp directly from the JSON data
    ).withColumn("parsed_json", from_json(col("value"), schema)) \
        .select(
        col("parsed_json.location_name").alias("location_name"),
        col("parsed_json.location_region").alias("location_region"),
        col("parsed_json.location_country").alias("location_country"),
        col("parsed_json.location_lat").alias("location_lat"),
        col("parsed_json.location_lon").alias("location_lon"),
        col("parsed_json.location_tz_id").alias("location_tz_id"),
        col("parsed_json.location_localtime_epoch").alias("location_localtime_epoch"),
        col("parsed_json.location_localtime").alias("location_localtime"),
        col("parsed_json.current_last_updated_epoch").alias("current_last_updated_epoch"),
        col("parsed_json.current_last_updated").alias("current_last_updated"),
        col("parsed_json.current_temp_c").alias("current_temp_c"),
        col("parsed_json.current_temp_f").alias("current_temp_f"),
        col("parsed_json.current_is_day").alias("current_is_day"),
        col("parsed_json.current_condition_text").alias("current_condition_text"),
        col("parsed_json.current_condition_icon").alias("current_condition_icon"),
        col("parsed_json.current_condition_code").alias("current_condition_code"),
        col("parsed_json.current_wind_mph").alias("current_wind_mph"),
        col("parsed_json.current_wind_kph").alias("current_wind_kph"),
        col("parsed_json.current_wind_degree").alias("current_wind_degree"),
        col("parsed_json.current_wind_dir").alias("current_wind_dir"),
        col("parsed_json.current_pressure_mb").alias("current_pressure_mb"),
        col("parsed_json.current_pressure_in").alias("current_pressure_in"),
        col("parsed_json.current_precip_mm").alias("current_precip_mm"),
        col("parsed_json.current_precip_in").alias("current_precip_in"),
        col("parsed_json.current_humidity").alias("current_humidity"),
        col("parsed_json.current_cloud").alias("current_cloud"),
        col("parsed_json.current_feelslike_c").alias("current_feelslike_c"),
        col("parsed_json.current_feelslike_f").alias("current_feelslike_f"),
        col("parsed_json.current_windchill_c").alias("current_windchill_c"),
        col("parsed_json.current_windchill_f").alias("current_windchill_f"),
        col("parsed_json.current_heatindex_c").alias("current_heatindex_c"),
        col("parsed_json.current_heatindex_f").alias("current_heatindex_f"),
        col("parsed_json.current_dewpoint_c").alias("current_dewpoint_c"),
        col("parsed_json.current_dewpoint_f").alias("current_dewpoint_f"),
        col("parsed_json.current_vis_km").alias("current_vis_km"),
        col("parsed_json.current_vis_miles").alias("current_vis_miles"),
        col("parsed_json.current_uv").alias("current_uv"),
        col("parsed_json.current_gust_mph").alias("current_gust_mph"),
        col("parsed_json.current_gust_kph").alias("current_gust_kph"),
        col("parsed_json.current_air_quality_co").alias("current_air_quality_co"),
        col("parsed_json.current_air_quality_no2").alias("current_air_quality_no2"),
        col("parsed_json.current_air_quality_o3").alias("current_air_quality_o3"),
        col("parsed_json.current_air_quality_so2").alias("current_air_quality_so2"),
        col("parsed_json.current_air_quality_pm2_5").alias("current_air_quality_pm2_5"),
        col("parsed_json.current_air_quality_pm10").alias("current_air_quality_pm10"),
        col("parsed_json.current_air_quality_us_epa_index").alias("current_air_quality_us_epa_index"),
        col("parsed_json.current_air_quality_gb_defra_index").alias("current_air_quality_gb_defra_index"),
    )

    spark.sql(QUERY)

    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("iceberg") \
        .option("checkpointLocation", CHECKPOINT) \
        .toTable(TABLE)

    query.awaitTermination()
