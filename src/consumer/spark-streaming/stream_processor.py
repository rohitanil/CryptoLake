from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructField, StructType, DoubleType, StringType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Kafka-Iceberg-Stream-Processor").getOrCreate()
    schema = StructType([
        StructField("BTC_EUR", DoubleType(), True),
        StructField("BTC_INR", DoubleType(), True),
        StructField("BTC_USD", DoubleType(), True),
        StructField("ETH_EUR", DoubleType(), True),
        StructField("ETH_INR", DoubleType(), True),
        StructField("ETH_USD", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.1.16:9092") \
        .option("subscribe", "crypto") \
        .option("startingOffsets", "earliest") \
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

    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()
