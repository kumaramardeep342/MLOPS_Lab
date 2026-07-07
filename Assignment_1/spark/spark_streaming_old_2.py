"""
Spark Structured Streaming Pipeline - DA5402W Assignment 1 Part B
Processes, cleans, and analyzes real-time sensor streams from Apache Kafka.

Covers Tasks 1-14 including file-logging  report generation.
"""
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.streaming import StreamingQueryListener

# ==============================================================================
# TASK 14: PERFORMANCE METRICS CAPTURE
# ==============================================================================
class ReportMetricsListener(StreamingQueryListener):
    """
    Listens to the live Spark streaming context engine and automatically dumps 
    operational performance analytics to JSON for the final report submission.
    """
    def onQueryStarted(self, event):
        pass
    def onQueryTerminated(self, event):
        pass
    def onQueryProgress(self, event):
        progress = event.progress
        metrics = {
            "batch_id": progress.batchId,
            "input_rate_rps": progress.inputRowsPerSecond,
            "processing_rate_rps": progress.processedRowsPerSecond,
            "batch_duration_ms": progress.durationMs.get("triggerExecution", 0)
        }
        os.makedirs("reports", exist_ok=True)
        with open("reports/streaming_performance_metrics.json", "w") as f:
            json.dump(metrics, f, indent=4)

def main():
    # Initialize Spark Session with the Kafka Connector bundle
    spark = (SparkSession.builder
             .appName("SensorStreamProcessor")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6")
             .config("spark.sql.shuffle.partitions", "3") # Optimized for your 3 topic partitions
             .getOrCreate())

    # Set log level to reduce console clutter while processing streams
    spark.sparkContext.setLogLevel("WARN")
    
    # Register the performance metrics tracking hook
    spark.streams.addListener(ReportMetricsListener())

    # ==============================================================================
    # TASK 1: DEFINE INCOMING DATA SCHEMA
    # ==============================================================================
    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("status", StringType(), True)
    ])

    # ==============================================================================
    # TASK 2: PRINT SCHEMA VERIFICATION
    # ==============================================================================
    # Verified schema is displayed cleanly via the DataFrame pipeline below.

    BROKER = "localhost:9092"
    TOPIC = "sensor_da25m502"

    # Connect to the local Docker-hosted Kafka Broker Cluster channel
    kafka_stream_df = (spark.readStream
                       .format("kafka")
                       .option("kafka.bootstrap.servers", BROKER)
                       .option("subscribe", TOPIC)
                       .option("startingOffsets", "latest")
                       .load())

    # Extract payloads and cast binary values to usable string rows
    parsed_df = (kafka_stream_df
                 .selectExpr("CAST(value AS STRING) as json_payload")
                 .select(F.from_json(F.col("json_payload"), schema).alias("data"))
                 .select("data.*"))

    # Print structural data map tree cleanly to terminal console log
    print("\n" + "="*20 + " TASK 2: VERIFIED INCOMING SCHEMA " + "="*20)
    parsed_df.printSchema()
    print("="*74 + "\n")

    # ==============================================================================
    # TASK 5 & 6: TIMESTAMP PARSING & CORRUPT COLUMN DATA FILTERS
    # ==============================================================================
    # Parses strings and instantly filters explicitly malformed timestamp records out
    timestamped_df = (parsed_df
                      .withColumn("event_time", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
                      .filter(F.col("event_time").isNotNull()))

    # ==============================================================================
    # TASK 13: WATERMARK DEFINITION (5-Minute Latency Buffer Threshold)
    # ==============================================================================
    watermarked_df = timestamped_df.withWatermark("event_time", "5 minutes")

    # ==============================================================================
    # TASK 4: DE-DUPLICATION (Identity Boundary Verification via Key Fields)
    # ==============================================================================
    deduplicated_df = watermarked_df.dropDuplicates(["sensor_id", "timestamp"])

    # ==============================================================================
    # TASK 5: TEMPERATURE & HUMIDITY BOUNDARY CHECKS
    # ==============================================================================
    valid_sensors_df = deduplicated_df.filter(
        (F.col("temperature") >= -20.0) & (F.col("temperature") <= 100.0) &
        (F.col("humidity") >= 0.0) & (F.col("humidity") <= 100.0)
    )

    # ==============================================================================
    # TASK 3: MISSING STRATEGY NULL-VALUE HANDLING
    # ==============================================================================
    cleaned_df = valid_sensors_df.filter(F.col("temperature").isNotNull())

    # ==============================================================================
    # TASK 7: FEATURE ENGINEERING (Temporal Property Derivations)
    # ==============================================================================
    enriched_df = (cleaned_df
                   .withColumn("hour_of_day", F.hour(F.col("event_time")))
                   .withColumn("day_of_week", F.dayofweek(F.col("event_time")))
                   .withColumn("is_weekend", F.when(F.col("day_of_week").isin(1, 7), 1).otherwise(0)))

    # ==============================================================================
    # TASK 8, 9, 11 & 12: SLIDING/TUMBLING WINDOWED TIME AGGREGATIONS
    # ==============================================================================
    analytics_df = (enriched_df
                    .groupBy(F.window(F.col("event_time"), "5 minutes"), F.col("sensor_id"))
                    .agg(
                        F.avg("temperature").alias("avg_temp"),       # Task 8: Avg Temp
                        F.max("temperature").alias("max_temperature"),# Task 9: Max Temp
                        F.count("status").alias("total_status_records") # Task 11: Total Counts
                    )
                    .select(
                        F.col("window.start").cast("string").alias("window_start"),
                        F.col("window.end").cast("string").alias("window_end"),
                        "sensor_id",
                        F.round("avg_temp", 2).alias("avg_temp"),
                        "max_temperature",
                        "total_status_records"
                    ))

    # ==============================================================================
    # TASK 10: CONSOLE SINK (Validation and Visual Inspection Monitor)
    # ==============================================================================
    query_console = (analytics_df.writeStream
                     .outputMode("complete")
                     .format("console")
                     .option("truncate", "false")
                     .start())

    # ==============================================================================
    # REPORT GENERATION SINK: LOGS EXTRACTOR FOR ANALYTICS PLOTS
    # ==============================================================================
    # Automatically saves micro-batch changes out into structured physical CSV logs
    query_csv = (analytics_df.writeStream
                 .outputMode("append")
                 .format("csv")
                 .option("path", "reports/analytics_csv")
                 .option("checkpointLocation", "reports/checkpoint")
                 .start())

    query_console.awaitTermination()

if __name__ == "__main__":
    main()