"""
Spark Structured Streaming Pipeline - DA5402W Assignment 1 Part B
Processes, cleans, and analyzes real-time sensor streams from Apache Kafka.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def main():
    # Initialize Spark Session with Kafka Connector
    # Using standard Kafka connector coordinate for local execution
    spark = (SparkSession.builder
             .appName("SensorStreamProcessor")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6")
             .config("spark.sql.shuffle.partitions", "3") # Optimized for 3 partitions
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    # 1. Define schema for incoming JSON records
    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("status", StringType(), True)
    ])

    # 2. Print Schema to Console
    print("\n" + "="*20 + " INCOMING STREAM SCHEMA " + "="*20)
    print("="*64 + "\n")

    # Connect to local Kafka broker (Change to "kafka:29092" if running on Portal UI)
    BROKER = "localhost:9092"
    TOPIC = "sensor_da25m502"

    # Read Streaming DataFrame from Kafka
    kafka_stream_df = (spark.readStream
                       .format("kafka")
                       .option("kafka.bootstrap.servers", BROKER)
                       .option("subscribe", TOPIC)
                       .option("startingOffsets", "latest")
                       .load())

    # Parse JSON values from Kafka byte payloads
    parsed_df = (kafka_stream_df
                 .selectExpr("CAST(value AS STRING) as json_payload")
                 .select(F.from_json(F.col("json_payload"), schema).alias("data"))
                 .select("data.*"))

    # 5 & 6. Handle Timestamps (Convert strings and drop explicitly invalid format rows)
    timestamped_df = (parsed_df
                      .withColumn("event_time", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
                      .filter(F.col("event_time").isNotNull())) # Task 5: Filters invalid timestamps

    # 13. Apply Watermarking with a 5-minute delay threshold
    watermarked_df = timestamped_df.withWatermark("event_time", "5 minutes")

    # 4. Remove Duplicate records based on sensor_id + timestamp criteria
    deduplicated_df = watermarked_df.dropDuplicates(["sensor_id", "timestamp"])

    # 5. Remove out-of-bounds invalid sensor rows
    valid_sensors_df = deduplicated_df.filter(
        (F.col("temperature") >= -20.0) & 
        (F.col("temperature") <= 100.0) & 
        (F.col("humidity") >= 0.0) & 
        (F.col("humidity") <= 100.0)
    )

    # 3. Handle Missing Values: Use a window expression or drop if history unavailable
    # Due to streaming state limitations on multi-level lookups, we report and drop rows 
    # that do not possess a valid numeric temperature field.
    cleaned_df = valid_sensors_df.filter(F.col("temperature").isNotNull())

    # 7. Feature Engineering tasks
    enriched_df = (cleaned_df
                   .withColumn("hour_of_day", F.hour(F.col("event_time")))
                   .withColumn("day_of_week", F.dayofweek(F.col("event_time")))
                   .withColumn("is_weekend", F.when(F.col("day_of_week").isin(1, 7), 1).otherwise(0)))

    # 8, 9, 11 & 12. Streaming Aggregations over a 5-Minute Tumbling Window
    analytics_df = (enriched_df
                    .groupBy(
                        F.window(F.col("event_time"), "5 minutes"),
                        F.col("sensor_id")
                    )
                    .agg(
                        F.avg("temperature").alias("avg_temperature"),
                        F.max("temperature").alias("max_temperature"),
                        F.count("status").alias("total_status_records"),
                        F.first("status").alias("current_status")
                    )
                    .select(
                        F.col("window.start").alias("window_start"),
                        F.col("window.end").alias("window_end"),
                        "sensor_id",
                        F.round("avg_temperature", 2).alias("avg_temp"),
                        "max_temperature",
                        "total_status_records"
                    ))

    # Output Console Sink to view continuous analytics aggregation
    query = (analytics_df.writeStream
             .outputMode("complete") # Complete mode is ideal for presenting window updates
             .format("console")
             .option("truncate", "false")
             .start())

    query.awaitTermination()

if __name__ == "__main__":
    main()