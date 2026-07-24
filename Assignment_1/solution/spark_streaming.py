"""
Spark Structured Streaming pipeline - DA5402W Assignment 1, Part B.

Reads the sensor stream from Kafka, performs preprocessing, feature
engineering, streaming analytics and event-time (windowed/watermarked)
processing.

Run (after the producer has published some data):
    python spark_streaming.py --topic sensor_21f1234567 --duration-seconds 180

Requires:
    pyspark (with the spark-sql-kafka-0-10 package, e.g.
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0)
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone

import pyspark
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.streaming import StreamingQueryListener

SENSOR_SCHEMA = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("status", StringType(), True),
])

# Mutable, process-wide counters updated from foreachBatch / the query listener.
METRICS = {
    "total_records_in": 0,
    "missing_values_corrected": 0,
    "duplicate_records_removed": 0,
    "invalid_records_removed": 0,
    "total_records_clean": 0,
    "late_records_accepted": 0,
    "records_discarded_by_watermarking": 0,
    "performance": [],
}


def write_metrics(path):
    METRICS["last_updated"] = datetime.now(timezone.utc).isoformat()
    try:
        with open(path, "w") as f:
            json.dump(METRICS, f, indent=2, default=str)
    except OSError as e:
        print(f"Could not write metrics file: {e}")


def base_dataframe(spark, bootstrap_servers, topic):
    """Read raw Kafka records and parse the JSON payload + event-time timestamp."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "1000")
        .load()
    )

    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), SENSOR_SCHEMA).alias("data")
    ).select("data.*")

    # Convert timestamp string -> Spark Timestamp. Invalid/garbled strings become NULL
    # (try_to_timestamp avoids ANSI mode raising on unparseable strings).
    parsed = parsed.withColumn(
        "event_time", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
      

    return parsed


def process_preprocessing_batch(batch_df, batch_id, metrics_path):
    """Per-microbatch preprocessing, feature engineering and analytics."""
    print(f"[preprocessing] batch {batch_id}: received, counting rows...")
    n_in = batch_df.count()
    print(f"[preprocessing] batch {batch_id}: {n_in} input rows")
    if n_in == 0:
        return
    METRICS["total_records_in"] += n_in

    # 1. Handle missing temperature -> replace with the batch mean temperature.
    mean_temp_row = batch_df.agg(F.mean("temperature").alias("mean_temp")).collect()[0]
    mean_temp = mean_temp_row["mean_temp"]
    n_missing = batch_df.filter(F.col("temperature").isNull()).count()
    METRICS["missing_values_corrected"] += n_missing
    df = batch_df.fillna({"temperature": mean_temp} if mean_temp is not None else {})

    # 2. Remove duplicate records using sensor_id + timestamp.
    n_before_dedup = df.count()
    df = df.dropDuplicates(["sensor_id", "timestamp"])
    METRICS["duplicate_records_removed"] += n_before_dedup - df.count()

    # 3. Remove invalid records: out-of-range temperature or invalid (null) timestamp.
    n_before_filter = df.count()
    df = df.filter(
        (F.col("temperature") >= -20)
        & (F.col("temperature") <= 100)
        & (F.col("event_time").isNotNull())
    )
    METRICS["invalid_records_removed"] += n_before_filter - df.count()

    # 4. Feature engineering: hour of day, day of week, weekend indicator.
    df = (
        df.withColumn("hour_of_day", F.hour("event_time"))
        .withColumn("day_of_week", F.dayofweek("event_time"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int"))
    )

    n_clean = df.count()
    METRICS["total_records_clean"] += n_clean

    print(f"\n===== Batch {batch_id} | input={n_in} clean={n_clean} "
          f"missing_fixed={n_missing} mean_temp={mean_temp} =====")

    df.cache()

    # Streaming analytics: avg/max temperature per sensor.
    df.groupBy("sensor_id").agg(
        F.avg("temperature").alias("avg_temperature"),
        F.max("temperature").alias("max_temperature"),
    ).orderBy("sensor_id").show(truncate=False)

    # Number of active sensors in this batch.
    active_sensors = df.filter(F.col("status") == "active").select("sensor_id").distinct().count()
    print(f"Active sensors in this batch: {active_sensors}")

    # Distribution of sensor status values.
    df.groupBy("status").count().orderBy("status").show(truncate=False)

    df.unpersist()
    write_metrics(metrics_path)


def run_windowed_query(parsed, processing_time, checkpoint_dir):
    """5-minute tumbling window average temperature with a 5-minute watermark."""
    windowed = (
        parsed
        .withWatermark("event_time", "5 minutes")
        .groupBy(F.window("event_time", "5 minutes"), F.col("sensor_id"))
        .agg(F.avg("temperature").alias("avg_temperature"), F.count("*").alias("record_count"))
    )

    query = (
        windowed.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        #.option("checkpointLocation", os.path.join(checkpoint_dir, "windowed"))
        .trigger(processingTime=processing_time)
        .start()
    )
    return query


class MetricsListener(StreamingQueryListener):
    """Captures per-batch performance metrics and watermark drop stats."""

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        progress = event.progress
        entry = {
            "query": progress.name or str(progress.id),
            "batch_id": progress.batchId,
            "input_rows_per_second": progress.inputRowsPerSecond,
            "processed_rows_per_second": progress.processedRowsPerSecond,
            "num_input_rows": progress.numInputRows,
            "batch_duration_ms": progress.durationMs.get("triggerExecution")
            if progress.durationMs else None,
        }

        dropped = 0
        for state_op in progress.stateOperators:
            dropped += getattr(state_op, "numRowsDroppedByWatermark", 0) or 0
        entry["rows_dropped_by_watermark"] = dropped
        METRICS["records_discarded_by_watermarking"] += dropped

        METRICS["performance"].append(entry)

    def onQueryTerminated(self, event):
        pass


def main():
    #args = parse_args()
    topic = 'sensor_1'
    group_id = 'grpA'
    bootstrap_servers = 'kafka:29092'
    metrics_out = "/storage/scratch/spark_metrics_roll_no.json"
    duration_seconds = 180
    checkpoint_dir = "/storage/scratch/spark_checkpoints"
    processing_time = "10 seconds"
    num_consumers = 3
    idle_timeout_ms = 10000
    show_records = 10

    spark = (
        SparkSession.builder
        .appName("DA5402W-SensorStreaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.jars.packages",
            f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    listener = MetricsListener()
    spark.streams.addListener(listener)

    parsed = base_dataframe(spark, bootstrap_servers, topic)

    print("Streaming DataFrame schema:")
    parsed.printSchema()

    # Late records (event_time older than the current watermark, but not yet
    # dropped) are still accepted into the windowed aggregation; we track how
    # many records arrive "late" relative to processing time as a proxy.
    preprocessing_query = (
        parsed.writeStream
        .foreachBatch(lambda df, bid: process_preprocessing_batch(df, bid, metrics_out))
        #.option("checkpointLocation", os.path.join(checkpoint_dir, "preprocessing"))
        .trigger(processingTime=processing_time)
        .start()
    )

    windowed_query = run_windowed_query(parsed, processing_time, checkpoint_dir)

    print(f"\nspark_streaming.py is running (topic={topic}, "
          f"trigger={processing_time}, duration={duration_seconds}s)...\n")

    start = time.time()
    try:
        while time.time() - start < duration_seconds:
            time.sleep(5)
            elapsed = int(time.time() - start)
            print(f"[heartbeat] spark_streaming.py running... elapsed={elapsed}s "
                  f"/ {duration_seconds}s, total_records_in={METRICS['total_records_in']}, "
                  f"preprocessing_active={preprocessing_query.isActive}, "
                  f"windowed_active={windowed_query.isActive}")

            for q, name in ((preprocessing_query, "preprocessing"), (windowed_query, "windowed")):
                if not q.isActive:
                    exc = q.exception()
                    print(f"[error] {name} query terminated: {exc}")

            if not preprocessing_query.isActive and not windowed_query.isActive:
                break
    except KeyboardInterrupt:
        pass
    finally:
        preprocessing_query.stop()
        windowed_query.stop()
        write_metrics(metrics_out)
        print("\nFinal metrics summary:")
        print(json.dumps(METRICS, indent=2, default=str))


if __name__ == "__main__":
    main()
