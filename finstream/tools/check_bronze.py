from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os

BRONZE_PATH = os.getenv(
    "BRONZE_PATH",
    "/tmp/finstream/delta/bronze/transactions"
)

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("CheckBronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

try:
    df = spark.read.format("delta").load(BRONZE_PATH)
except Exception as e:
    print(f"❌ Failed to read Bronze table: {e}")
    exit(1)

print("\n===== SCHEMA =====")
df.printSchema()

print("\n===== SAMPLE DATA =====")
df.show(5, truncate=False)

print("\n===== QUICK CHECKS =====")

# Sample-based checks instead of full scan
sample_df = df.limit(1000)

# CDC columns
required_cols = ["__op", "__source_ts_ms"]
missing = [c for c in required_cols if c not in df.columns]

print(f"CDC columns present: {not missing}")
if missing:
    print(f"⚠ Missing: {missing}")

# Null check
null_ids = sample_df.filter("transaction_id IS NULL").count()
print(f"Null transaction_id (sample): {null_ids}")

# Negative amounts
neg_amounts = sample_df.filter("amount < 0").count()
print(f"Negative amounts (sample): {neg_amounts}")

# Freshness (safe aggregation)
if "created_at" in df.columns:
    latest = df.selectExpr("max(to_timestamp(created_at)) as latest").first()["latest"]
    print(f"Latest event timestamp: {latest}")

print("\n✅ Bronze check complete")