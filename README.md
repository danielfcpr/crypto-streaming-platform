# Crypto Streaming Lakehouse
## Near Real-Time Analytics Platform

A **near real-time crypto analytics platform** built end-to-end using **Kafka, Spark, AWS (S3, Athena and Glue), Airflow, and Power BI**.

This project demonstrates how to design, build, and operate a **modern data lakehouse** capable of ingesting streaming data, enforcing data quality, and serving analytics-ready datasets with **minute-level freshness**.

**Portfolio objective**: showcase real-world **Data Engineering** skills — architecture, data modeling, orchestration, and operational trade-offs.

---

## What This Project Demonstrates

- Streaming ingestion with **Apache Kafka**
- **Bronze / Silver / Gold** lakehouse architecture
- Near real-time analytics (1-minute granularity)
- Data quality gates and freshness checks
- Partitioned data modeling for analytics
- Orchestration with **Apache Airflow**
- BI consumption with **Power BI**
- Fully containerized local environment using **Docker Compose**

---

## Architecture Overview

**High-level data flow**
```plaintext
CoinGecko API
↓
Kafka (Producer)
↓
Kafka Connect (S3 Sink)
↓
S3 Bronze (raw JSON)
↓
Spark (Bronze → Silver)
↓
Spark (Silver → Gold)
↓
Athena (external tables & views)
↓
Power BI (dashboards)
```
This architecture mirrors **production-grade lakehouse patterns** used in cloud environments.

---
## Technology Stack

| Layer                | Technology              |
|----------------------|-------------------------|
| Ingestion            | Kafka, Kafka Connect    |
| Streaming Source     | CoinGecko API           |
| Storage              | Amazon S3               |
| Processing           | Apache Spark            |
| Query Engine         | Amazon Athena           |
| Schemas & partitions | Amazon Glue             |
| Orchestration        | Apache Airflow          |
| Visualization        | Power BI                |
| Infrastructure       | Docker & Docker Compose |

---

## Data Modeling — Bronze / Silver / Gold

### 🥉 Bronze — Raw Events

- Raw JSON messages from Kafka
- One file per flush interval
- No transformations
- Used for traceability and reprocessing

---

### 🥈 Silver — Clean & Structured

Two datasets are produced.

#### 1. History (append-only)

- Deduplicated records
- Typed columns
- One row per coin per minute

#### 2. Latest Snapshot

- One row per coin
- Always reflects the most recent value

**Data quality checks**

- Required columns present
- No nulls on critical fields
- Prices greater than zero
- Freshness threshold enforced

---

### 🥇 Gold — Analytics Ready

#### gold_quotes_1m

- Minute-level fact table
- Used for intraday analysis
- Partitioned by `dt`

#### gold_quotes_daily

- Daily aggregates per coin
- Average, minimum, maximum, and last price
- Maximum market cap and volume

#### gold_quotes_latest

- One row per coin
- Used for real-time KPIs

---

## Data Quality and Freshness

Spark jobs include **explicit data quality gates**:

- Schema validation
- Null checks
- Value constraints
- Freshness checks based on event time

Freshness metrics are exposed via **Athena views** and visualized in **Power BI**.

---

## Near Real-Time Strategy

- Data ingested every **60 seconds**
- Gold layer refreshed on demand or via Airflow
- Power BI refreshed every **1–5 minutes**

This provides a practical balance between **latency, cost, and complexity**.

---

## Power BI Dashboards

### Page 1 — Current Snapshot

- Latest price
- Market capitalization
- 24-hour volume
- Data freshness KPI (color-coded)


```plaintext
screenshots/
├── powerbi-current-snapshot.png
```
---

### Page 2 — Daily Snapshot

- Daily average, minimum, and maximum prices
- Market cap and volume trends
- Historical comparison per coin

```plaintext
screenshots/
├── powerbi-daily-overview.png
```

---

### Page 3 — Intraday Trends (1m)

- Price evolution over time
- Coin-level filtering

```plaintext
screenshots/
├── powerbi-intraday-trends.png
```

---

## Orchestration with Apache Airflow

Airflow controls the **data movement phase** of the pipeline.

Execution order:
```plaintext
check_s3_sink_running
↓
spark_bronze_to_silver
↓
spark_silver_to_gold
↓
athena_repair
```
Ingestion runs continuously, while transformations are explicitly orchestrated.

---
## Running the Project Locally

### Start core services
```bash
make up
```
### Register the Kafka → S3 Sink connector
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @connectors/s3-sink-crypto-raw.json
```
### Let data ingest for a few minutes

### Trigger the Airflow DAG

- Open Airflow UI at `http://localhost:8081`
- Trigger `crypto_lakehouse_bronze_silver_gold`

### Refresh Power BI

### Stopping the Platform
Stop containers without removing state:
```bash
make stop
```
Full reset including volumes:
```bash
make clean
```
---
## Design Decisions and Trade-offs

- Kafka + S3 Sink chosen for simplicity and robustness
- AWS Athena used for serverless analytics
- Append and overwrite-by-partition strategy for near real-time updates
- Manual DAG triggers during development for transparency
- Docker Compose for local reproducibility

---

## What This Project Proves

- End-to-end data pipeline ownership
- Strong understanding of streaming and batch hybrid systems
- Solid data modeling practices
- Production-oriented mindset (data quality, freshness, orchestration)
- Ability to deliver analytics-ready datasets to BI tools

---

## Possible Next Improvements

- Incremental Spark processing with checkpoints
- Schema Registry integration
- CI/CD for Spark jobs
- Cloud deployment (ECS, EKS, or MWAA)

---

## Author

**Daniel Calvo Pérez**  
Data Engineer / Machine Learning Engineer

