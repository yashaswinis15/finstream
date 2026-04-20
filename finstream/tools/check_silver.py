from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os

SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "/tmp/finstream/delta/silver/transactions"
)

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("CheckSilver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

try:
    df = spark.read.format("delta").load(SILVER_PATH)
except Exception as e:
    print(f"❌ Failed to read Silver table: {e}")
    exit(1)

print("\n===== SAMPLE DATA =====")
df.show(5, truncate=False)

print("\n===== SCHEMA =====")
df.printSchema()

print("\n===== QUICK CHECKS =====")

# Sample instead of full scan
sample_df = df.limit(1000)

# Duplicate check (sample-based)
dupes = (
    sample_df.groupBy("transaction_id")
    .count()
    .filter("count > 1")
    .count()
)
print(f"Duplicate transaction_ids (sample): {dupes}")

# Delete check
if "__op" in df.columns:
    deletes = sample_df.filter("__op = 'd'").count()
    print(f"Deletes present (sample): {deletes}")

# Null checks
null_risk = sample_df.filter("risk_score IS NULL").count()
print(f"Null risk_score (sample): {null_risk}")

# Freshness
if "txn_timestamp" in df.columns:
    latest = df.selectExpr("max(txn_timestamp) as latest").first()["latest"]
    print(f"Latest transaction timestamp: {latest}")

print("\n✅ Silver check complete")