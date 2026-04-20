from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip
import os

SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "/tmp/finstream/delta/silver/transactions"
)

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("Debug-Silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

df = spark.read.format("delta").load(SILVER_PATH)

print("\n=== SAMPLE ===")
df.show(10, truncate=False)

print("\n=== SCHEMA ===")
df.printSchema()

# ----------------------------
# Focus on recent data
# ----------------------------
recent_df = df.orderBy(F.col("txn_timestamp").desc()).limit(1000)

# ----------------------------
# Duplicate check (sample)
# ----------------------------
dupes = (
    recent_df.groupBy("transaction_id")
    .count()
    .filter("count > 1")
    .count()
)
print(f"\nDuplicate transaction_ids (sample): {dupes}")

# ----------------------------
# CDC correctness
# ----------------------------
if "__op" in df.columns:
    deletes = recent_df.filter("__op = 'd'").count()
    print(f"Delete rows (sample): {deletes}")

# ----------------------------
# Data quality
# ----------------------------
null_risk = recent_df.filter("risk_score IS NULL").count()
print(f"Null risk_score (sample): {null_risk}")

# ----------------------------
# Business validation
# ----------------------------
print("\n=== Risk Tier Distribution (sample) ===")
recent_df.groupBy("risk_tier").count().show()

print("\n=== Large Transaction Ratio (sample) ===")
recent_df.select(
    (F.sum(F.col("is_large_transaction").cast("int")) / F.count("*")).alias("ratio")
).show()

# ----------------------------
# Freshness
# ----------------------------
latest = df.selectExpr("max(txn_timestamp) as latest").first()["latest"]
print(f"\nLatest transaction timestamp: {latest}")

print("\n✅ Silver debug complete")