# FinStream — Real-Time Fraud Detection & Analytics Pipeline

A production-style data engineering project that ingests, processes, and analyzes financial transactions in near real time using CDC and streaming architecture.

This system simulates how fintech platforms detect fraud, maintain data consistency, and recover from failures under realistic constraints.

---

## Architecture

PostgreSQL (source database)  
↓ WAL-based CDC  
Debezium (captures row-level changes)  
↓  
Kafka (durable event stream)  
↓  
Spark Structured Streaming  
↓  
Delta Lake (Bronze → Silver → Gold)  
↓  
dbt (analytical transformations)  
↓  
PostgreSQL (serving layer)  
↓  
Grafana (dashboards) + Slack (alerts)

---

## What This Project Demonstrates

- Change Data Capture using PostgreSQL WAL (no polling)
- Near real-time streaming pipeline with low-latency processing
- Medallion architecture (Bronze, Silver, Gold layers)
- Fault-tolerant processing using:
  - Kafka offset management
  - Spark checkpointing
  - Delta Lake transaction log
- Schema evolution handling with minimal disruption
- Recovery from Kafka and CDC failures
- Data quality validation using dbt tests
- Alerting based on transaction risk scoring

---

## Tech Stack

- PostgreSQL (source + serving layer)
- Debezium (CDC engine)
- Apache Kafka (event streaming)
- Apache Spark Structured Streaming
- Delta Lake (ACID lakehouse storage)
- dbt (transformations + testing)
- Grafana (visualization)
- Python (alert engine)
- Docker (local orchestration)

---

## How the Pipeline Works

1. Transactions are written to PostgreSQL  
2. Debezium captures changes via WAL  
3. Events are streamed into Kafka topics  
4. Spark processes Kafka streams:
   - Bronze: raw CDC events  
   - Silver: cleaned, deduplicated, enriched data  
5. Gold layer computes business metrics  
6. dbt builds analytical models in PostgreSQL  
7. Grafana dashboards visualize metrics  
8. Alert engine triggers notifications for high-risk activity  

---

## Gold Layer (Business Aggregations)

The Gold layer is computed separately to isolate business logic from ingestion.

Outputs include:

- **fraud_summary** — distribution of transactions by risk tier over time  
- **volume_by_minute** — transaction volume trends for operational monitoring  
- **account_risk_profile** — aggregated risk indicators per account  

**Why separate Gold from Silver?**
- Enables recomputation without reprocessing raw data  
- Keeps ingestion pipeline focused on correctness  
- Allows flexible business logic iteration  

---

## Schema Evolution

This pipeline supports schema changes with minimal disruption.

Example:
- Adding new columns (e.g., `device_fingerprint`)

Behavior:
- Debezium captures schema changes from WAL  
- Kafka propagates updated records  
- Spark ingests using schema merging  
- Delta Lake stores updated schema  

Note:
- Compatibility depends on change type (additive vs breaking)

---

## Failure Recovery

Tested scenarios:

### Kafka Restart
- Kafka stopped and restarted  
- Debezium retains changes via WAL  
- Spark resumes from checkpoint  

### Debezium Restart
- Connector restarted  
- Missed events replayed from WAL  

### Observations
- No data loss observed during testing  
- Duplicate events handled via deduplication logic  

---

## Data Correctness Guarantees

- Kafka + Debezium provide **at-least-once delivery**
- Spark + Delta Lake enable **idempotent processing**
- Deduplication ensures consistent downstream results  

Result:
- System achieves **effectively-once behavior under tested conditions**

---

## Grafana Dashboard

- URL: http://localhost:3000  

Displays:
- Transaction volume trends  
- Fraud risk distribution  
- Pipeline activity  

---

## Alert Engine

- Polls PostgreSQL every 30 seconds  
- Triggers alerts for:
  - High-risk transactions  
  - Suspicious high-value activity  

---

## Design Decisions

**Why Debezium?**  
Captures row-level changes without adding load to the source database.

**Why Delta Lake?**  
Provides ACID guarantees and supports streaming + batch unification.

**Why Medallion Architecture?**  
Separates ingestion, cleaning, and business logic for maintainability.

**Why deduplication in Spark?**  
Handles duplicate events caused by CDC replay or retries.

---

## Limitations

- Local deployment (not distributed cluster)  
- Single Kafka broker (no replication)  
- Alerting is polling-based  
- Limited security configuration  

---

## Future Improvements

- Cloud deployment (AWS / GCP / Azure)  
- Event-driven alerting  
- Multi-broker Kafka setup  
- Observability (metrics, tracing)  
- CI/CD for pipeline and dbt  

---

## Why This Project Matters

This project reflects real-world data engineering challenges:

- Streaming + CDC integration  
- Fault-tolerant data pipelines  
- Schema evolution handling  
- Data quality validation  

It demonstrates not just building pipelines—but understanding their behavior under failure and change.