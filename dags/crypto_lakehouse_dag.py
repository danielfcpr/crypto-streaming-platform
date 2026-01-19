from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "daniel",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="crypto_lakehouse_bronze_silver_gold",
    default_args=DEFAULT_ARGS,
    description="Bronze->Silver->Gold (Spark on Docker Compose)",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # manual trigger for now (senior choice while debugging)
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "lakehouse", "spark"],
) as dag:
    check_s3_sink_running = BashOperator(
        task_id="check_s3_sink_running",
        bash_command=r"""
        set -euo pipefail
        curl -sf http://kafka-connect:8083/connectors/s3-sink-crypto-quotes-raw/status \
          | grep -q '"state":"RUNNING"'
        """,
    )

    spark_bronze_to_silver = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command="""
        set -euo pipefail
        cd /opt/project
        docker compose run --rm --no-deps spark-silver
        """,
    )

    spark_silver_to_gold = BashOperator(
        task_id="spark_silver_to_gold",
        bash_command="""
        set -euo pipefail
        cd /opt/project
        docker compose run --rm --no-deps spark-gold
        """,
    )

    athena_repair = BashOperator(
        task_id="athena_repair",
        bash_command="""
        set -euo pipefail
        cd /opt/project
        docker compose run --rm --no-deps athena-repair
        """,
    )

    # chaining
    check_s3_sink_running >> spark_bronze_to_silver >> spark_silver_to_gold >> athena_repair