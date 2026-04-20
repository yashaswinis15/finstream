from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip
import os

BRONZE_PATH = os.getenv(
    "BRONZE_PATH",
    "/tmp/finstream/delta/bronze/transactions"
)

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("Debug-Bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

df = spark.read.format("delta").load(BRONZE_PATH)

print("\n=== SAMPLE ===")
df.show(10, truncate=False)

print("\n=== SCHEMA ===")
df.printSchema()

# ----------------------------
# Focus on recent data
# ----------------------------
recent_df = df.orderBy(F.col("__source_ts_ms").desc()).limit(1000)

print("\n=== CDC DISTRIBUTION (sample) ===")
recent_df.groupBy("__op").count().show()

# ----------------------------
# CDC sanity
# ----------------------------
delete_sample = recent_df.filter("__op = 'd'").count()
print(f"Delete events (sample): {delete_sample}")

# ----------------------------
# Freshness
# ----------------------------
if "created_at" in df.columns:
    latest = df.selectExpr("max(to_timestamp(created_at)) as latest").first()["latest"]
    print(f"Latest event timestamp: {latest}")

# ----------------------------
# Data quality
# ----------------------------
null_ids = recent_df.filter("transaction_id IS NULL").count()
print(f"Null transaction_id (sample): {null_ids}")

neg_amounts = recent_df.filter("amount < 0").count()
print(f"Negative amounts (sample): {neg_amounts}")

print("\n✅ Bronze debug complete")