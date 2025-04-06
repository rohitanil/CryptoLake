from pyspark.sql import SparkSession
from anthropic import Anthropic
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
import os


def get_prompt(user_query, table, columns):
    prompt = f"""
    You are a Text-to-SQL conversion assistant.
    
    Given:
    - A user query: "{user_query}"
    - A table named: `{table}`
    - Columns in the table: {", ".join(columns)}
    
    Generate a valid **Spark SQL** query that answers the user query.
    
    Only output the **SQL query**. Do not include any explanation or formatting.
    """
    return prompt.strip()


def get_sql_query(table_name, columns):
    api_key = os.getenv("ANTHROPIC_KEY")
    client = Anthropic(api_key=api_key)
    response = client.messages.create(
        model="claude-3-5-sonnet-latest",
        max_tokens=100,
        temperature=0,
        messages=[{"role": "user", "content": get_prompt(input("Enter your query: "), table_name, columns)}]
    )
    return response.content[0].text.strip()


def getSpark():
    NESSIE_URI = "http://localhost:19120/api/v1"
    WAREHOUSE = "s3a://warehouse/"

    AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    AWS_S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT")

    spark_sess = (
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
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)  # Nessie API
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

        # MinIO-specific settings
        .config("spark.hadoop.fs.s3a.endpoint", AWS_S3_ENDPOINT)
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
    return spark_sess




if __name__ == "__main__":
    spark = getSpark()
    spark.sparkContext.setLogLevel("ERROR")
    cols = """location_name STRING,
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
                    timestamp STRING"""
    query = get_sql_query("weather", cols)
    print(f'Query Generated: {query}')

    # Read the Iceberg table
    df = spark.read.table("nessie.weather")
    df.createOrReplaceTempView("weather")

    result_df = spark.sql(query)
    # Show the results of the query
    result_df.show()
