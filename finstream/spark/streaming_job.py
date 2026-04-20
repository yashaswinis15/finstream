import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, BooleanType, LongType, IntegerType
)

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("FinStream-Streaming")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# ----------------------------
# Spark Session
# ----------------------------
spark = (
    SparkSession.builder
    .appName("FinStream-Streaming")
    .master("local[2]")
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.0.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# Paths
# ----------------------------
BASE_PATH = "/tmp/finstream/delta"

BRONZE_TXN = f"{BASE_PATH}/bronze/transactions"
SILVER_TXN = f"{BASE_PATH}/silver/transactions"

CHECKPOINT_BRONZE = f"{BASE_PATH}/checkpoints/bronze"
CHECKPOINT_SILVER = f"{BASE_PATH}/checkpoints/silver"

# ----------------------------
# Schema (FLAT — matches Debezium unwrap)
# ----------------------------
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", LongType()),
    StructField("account_id", IntegerType()),
    StructField("txn_type", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant", StringType()),
    StructField("merchant_category", StringType()),
    StructField("category", StringType()),
    StructField("status", StringType()),
    StructField("risk_score", DoubleType()),
    StructField("ip_address", StringType()),
    StructField("device_type", StringType()),
    StructField("location_city", StringType()),
    StructField("location_country", StringType()),
    StructField("is_international", BooleanType()),
    StructField("created_at", StringType()),

    # Debezium metadata fields (from unwrap)
    StructField("__op", StringType()),
    StructField("__source_ts_ms", LongType())
])

# ----------------------------
# Kafka → Bronze
# ----------------------------
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", "finstream.public.transactions")
    .option("startingOffsets", "earliest")  # safer for reproducibility
    .option("failOnDataLoss", "false")
    .load()
)

parsed_stream = (
    raw_stream
    .select(
        F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("data")
    )
    .select("data.*")
)

def write_bronze(df, epoch_id):
    (
        df
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("event_date", F.to_date("created_at"))  # event-time partition
        .write
        .format("delta")
        .mode("append")
        .partitionBy("event_date")
        .save(BRONZE_TXN)
    )

bronze_query = (
    parsed_stream.writeStream
    .foreachBatch(write_bronze)
    .option("checkpointLocation", CHECKPOINT_BRONZE)
    .trigger(processingTime="10 seconds")
    .start()
)

# ----------------------------
# Bronze → Silver
# ----------------------------
bronze_stream = (
    spark.readStream
    .format("delta")
    .load(BRONZE_TXN)
)

def write_silver(df, epoch_id):

    df = df.withColumn("txn_timestamp", F.to_timestamp("created_at"))

    silver_df = (
        df
        # Handle late data
        .withWatermark("txn_timestamp", "10 minutes")

        # Deduplication (CDC replay safety)
        .dropDuplicates(["transaction_id", "__source_ts_ms"])

        # Remove deletes (simplified handling)
        .filter(F.col("__op") != "d")

        # Basic data quality
        .filter(F.col("amount") > 0)
        .fillna({"risk_score": 0.0})

        # Feature engineering
        .withColumn(
            "risk_tier",
            F.when(F.col("risk_score") >= 0.90, "critical")
             .when(F.col("risk_score") >= 0.70, "high")
             .when(F.col("risk_score") >= 0.40, "medium")
             .otherwise("low")
        )
        .withColumn("is_large_transaction", F.col("amount") >= 5000)
        .withColumn("txn_hour", F.hour("txn_timestamp"))
        .withColumn("txn_date", F.to_date("txn_timestamp"))
        .withColumn(
            "is_after_hours",
            (F.col("txn_hour") < 6) | (F.col("txn_hour") >= 23)
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    (
        silver_df
        .write
        .format("delta")
        .mode("append")
        .partitionBy("txn_date")
        .save(SILVER_TXN)
    )

silver_query = (
    bronze_stream.writeStream
    .foreachBatch(write_silver)
    .option("checkpointLocation", CHECKPOINT_SILVER)
    .trigger(processingTime="15 seconds")
    .start()
)

# ----------------------------
# Run
# ----------------------------
log.info("FinStream streaming pipeline running...")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    log.info("Stopping streams...")
    bronze_query.stop()
    silver_query.stop()
    spark.stop()