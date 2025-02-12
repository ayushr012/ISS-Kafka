import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder.master("local[*]").appName("KafkaSparkDemo") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.2.1") \
        .getOrCreate()

    # Define Kafka Stream
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "testtopic") \
        .option("startingOffsets", "latest") \
        .load()

    # Define JSON Schema
    schema = StructType([
        StructField("message", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("iss_position", StructType([
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True)
        ]))
    ])

    # Parse JSON Messages
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data"))

    # Flatten JSON Fields
    flattened_df = parsed_df.select(
        col("data.iss_position.latitude"),
        col("data.iss_position.longitude"),
        col("data.message"),
        col("data.timestamp")
    )

    # Write to Console (for Debugging)
    query = flattened_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Save to CSV
    query_csv = flattened_df.writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("path", "testing") \
        .option("checkpointLocation", "checkpoint/") \
        .start()

    query.awaitTermination()
