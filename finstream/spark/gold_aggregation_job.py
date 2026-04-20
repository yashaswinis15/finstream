import logging
from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("FinStream-Gold")

# ----------------------------
# Spark Session
# ----------------------------
builder = (
    SparkSession.builder
    .appName("FinStream-Gold")
    .master("local[2]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

BASE_PATH = "/tmp/finstream/delta"

SILVER_TXN = f"{BASE_PATH}/silver/transactions"
GOLD_FRAUD = f"{BASE_PATH}/gold/fraud_summary"
GOLD_VOLUME = f"{BASE_PATH}/gold/volume_by_minute"
GOLD_ACCT = f"{BASE_PATH}/gold/account_risk_profile"

CHECKPOINT = "/tmp/finstream/checkpoints/gold_all"


# ----------------------------
# CORE LOGIC
# ----------------------------
def process_batch(df, epoch_id):

    df = df.withColumn(
        "minute_window",
        F.date_trunc("minute", F.col("txn_timestamp"))
    )

    # ----------------------------
    # GOLD 1: Fraud summary
    # ----------------------------
    fraud = (
        df.groupBy("minute_window", "risk_tier")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("risk_score").alias("avg_risk_score"),
            F.count(F.when(F.col("status") == "flagged", 1)).alias("flagged_count")
        )
        .withColumn(
            "fraud_rate_pct",
            F.round(F.col("flagged_count") / F.col("transaction_count") * 100, 2)
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    fraud.write.format("delta").mode("append").save(GOLD_FRAUD)

    # ----------------------------
    # GOLD 2: Volume
    # ----------------------------
    volume = (
        df.groupBy("minute_window", "txn_type")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_volume")
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    volume.write.format("delta").mode("append").save(GOLD_VOLUME)

    # ----------------------------
    # GOLD 3: Account risk
    # ----------------------------
    acct = (
        df.groupBy("account_id")
        .agg(
            F.count("*").alias("total_txns"),
            F.avg("risk_score").alias("avg_risk_score"),
            F.max("txn_timestamp").alias("last_txn")
        )
    )

    # Ensure table exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{GOLD_ACCT}`
        (account_id BIGINT, total_txns BIGINT, avg_risk_score DOUBLE, last_txn TIMESTAMP)
        USING DELTA
    """)

    acct.createOrReplaceTempView("updates")

    spark.sql(f"""
        MERGE INTO delta.`{GOLD_ACCT}` t
        USING updates s
        ON t.account_id = s.account_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    log.info(f"[epoch {epoch_id}] processed batch")


# ----------------------------
# STREAM
# ----------------------------
silver_stream = (
    spark.readStream
    .format("delta")
    .load(SILVER_TXN)
)

query = (
    silver_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)

log.info("Gold job running...")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    query.stop()
    spark.stop()