from pyspark.sql.functions import col, from_json, min, max, avg, window
from pyspark.sql.types import StructField, StructType, DoubleType, TimestampType
from pyspark import SparkConf
from pyspark.sql import SparkSession


def getSpark(warehouse, uri):
    conf = (
        SparkConf()
        .setAppName('KAFKA-ICEBERG-DATA-LAKE')
        # packages
        .set('spark.jars.packages',
             'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4')
        .set('spark.sql.extensions',
             'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    )
    spark_sess = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark_sess


if __name__ == "__main__":
    WAREHOUSE = "nessie"
    URI = "http://rest:8181"
    CHECKPOINT = "/tmp/spark/checkpoints/crypto_metrics"
    TABLE = "rest.crypto_metrics"
    QUERY = """
            CREATE TABLE IF NOT EXISTS {table} (
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                max_BTC_EUR DOUBLE,
                min_BTC_EUR DOUBLE,
                avg_BTC_EUR DOUBLE,
                max_BTC_INR DOUBLE,
                min_BTC_INR DOUBLE,
                avg_BTC_INR DOUBLE,
                max_BTC_USD DOUBLE,
                min_BTC_USD DOUBLE,
                avg_BTC_USD DOUBLE,
                max_ETH_EUR DOUBLE,
                min_ETH_EUR DOUBLE,
                avg_ETH_EUR DOUBLE,
                max_ETH_INR DOUBLE,
                min_ETH_INR DOUBLE,
                avg_ETH_INR DOUBLE,
                max_ETH_USD DOUBLE,
                min_ETH_USD DOUBLE,
                avg_ETH_USD DOUBLE
            ) USING iceberg
        """.format(table=TABLE)

    spark = getSpark(WAREHOUSE, URI)
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("BTC_EUR", DoubleType(), True),
        StructField("BTC_INR", DoubleType(), True),
        StructField("BTC_USD", DoubleType(), True),
        StructField("ETH_EUR", DoubleType(), True),
        StructField("ETH_INR", DoubleType(), True),
        StructField("ETH_USD", DoubleType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.1.225:9092") \
        .option("subscribe", "crypto") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = kafka_df.selectExpr(
        "CAST(value AS STRING) AS value",
        "timestamp"
    ).withColumn("parsed_json", from_json(col("value"), schema)) \
        .select(
        col("parsed_json.BTC_EUR").alias("BTC_EUR"),
        col("parsed_json.BTC_INR").alias("BTC_INR"),
        col("parsed_json.BTC_USD").alias("BTC_USD"),
        col("parsed_json.ETH_EUR").alias("ETH_EUR"),
        col("parsed_json.ETH_INR").alias("ETH_INR"),
        col("parsed_json.ETH_USD").alias("ETH_USD"),
        col("parsed_json.timestamp").alias("event_time"),
        col("timestamp").alias("processing_time")
    )

    windowed_df = parsed_df \
        .withWatermark("event_time", "1 minute") \
        .groupBy(window(col("event_time"), "1 minute", "1 minute")) \
        .agg(
        max("BTC_EUR").alias("max_BTC_EUR"),
        min("BTC_EUR").alias("min_BTC_EUR"),
        avg("BTC_EUR").alias("avg_BTC_EUR"),
        max("BTC_INR").alias("max_BTC_INR"),
        min("BTC_INR").alias("min_BTC_INR"),
        avg("BTC_INR").alias("avg_BTC_INR"),
        max("BTC_USD").alias("max_BTC_USD"),
        min("BTC_USD").alias("min_BTC_USD"),
        avg("BTC_USD").alias("avg_BTC_USD"),
        max("ETH_EUR").alias("max_ETH_EUR"),
        min("ETH_EUR").alias("min_ETH_EUR"),
        avg("ETH_EUR").alias("avg_ETH_EUR"),
        max("ETH_INR").alias("max_ETH_INR"),
        min("ETH_INR").alias("min_ETH_INR"),
        avg("ETH_INR").alias("avg_ETH_INR"),
        max("ETH_USD").alias("max_ETH_USD"),
        min("ETH_USD").alias("min_ETH_USD"),
        avg("ETH_USD").alias("avg_ETH_USD")
    ).withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window") \
        .select("window_start",
                "window_end",
                "max_BTC_EUR",
                "min_BTC_EUR",
                "avg_BTC_EUR",
                "max_BTC_INR",
                "min_BTC_INR",
                "avg_BTC_INR",
                "max_BTC_USD",
                "min_BTC_USD",
                "avg_BTC_USD",
                "max_ETH_EUR",
                "min_ETH_EUR",
                "avg_ETH_EUR",
                "max_ETH_INR",
                "min_ETH_INR",
                "avg_ETH_INR",
                "max_ETH_USD",
                "min_ETH_USD",
                "avg_ETH_USD")

    spark.sql(QUERY)

    query = windowed_df.writeStream \
        .outputMode("append") \
        .format("iceberg") \
        .option("checkpointLocation", CHECKPOINT) \
        .toTable(TABLE)

    query.awaitTermination()
