from pyspark.sql import SparkSession, functions as F, Window
from delta.tables import DeltaTable

# ----------------------------
# Spark Session
# ----------------------------
spark = (
    SparkSession.builder
    .appName("FinStream-Silver-Batch")
    .master("local[2]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

BRONZE_PATH = "/tmp/finstream/delta/bronze/transactions"
SILVER_PATH = "/tmp/finstream/delta/silver/transactions"

# ----------------------------
# Read Bronze (optional filter for performance)
# ----------------------------
df = spark.read.format("delta").load(BRONZE_PATH)

# ----------------------------
# Keep valid CDC ops
# ----------------------------
df = df.filter(F.col("__op").isin("c", "u", "d"))

# ----------------------------
# Deduplicate latest record
# ----------------------------
window_spec = Window.partitionBy("transaction_id") \
    .orderBy(F.col("__source_ts_ms").desc())

df = (
    df
    .withColumn("rn", F.row_number().over(window_spec))
    .filter("rn = 1")
    .drop("rn")
)

# ----------------------------
# Data quality
# ----------------------------
df = (
    df
    .withColumn("txn_timestamp", F.to_timestamp("created_at"))
    .filter(F.col("txn_timestamp").isNotNull())
    .fillna({
        "risk_score": 0.0,
        "amount": 0.0
    })
)

# ----------------------------
# Enrichment
# ----------------------------
silver_df = (
    df
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
    .withColumn("is_weekend", F.dayofweek("txn_timestamp").isin([1, 7]))
    .withColumn("is_after_hours",
        (F.col("txn_hour") < 6) | (F.col("txn_hour") >= 23)
    )
    .withColumn("processed_at", F.current_timestamp())
)

# ----------------------------
# Merge into Silver
# ----------------------------
if DeltaTable.isDeltaTable(spark, SILVER_PATH):

    delta_table = DeltaTable.forPath(spark, SILVER_PATH)

    (
        delta_table.alias("t")
        .merge(
            silver_df.alias("s"),
            "t.transaction_id = s.transaction_id"
        )
        .whenMatchedDelete(condition="s.__op = 'd'")
        .whenMatchedUpdateAll(condition="s.__op != 'd'")
        .whenNotMatchedInsertAll(condition="s.__op != 'd'")
        .execute()
    )

else:
    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("txn_date")
        .save(SILVER_PATH)
    )

print("Silver batch backfill complete")