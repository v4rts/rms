from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)

CH_CONN_ID = "RMS_postgreSQL"
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
MART_SQL_DIR = "SQL"

MART_TABLES = {
    "build_mart_leads": "mart_leads.sql",
    "build_mart_stages": "mart_stages.sql",
    "build_period_value": "period_value.sql",
    "build_mart_deals": "mart_deals.sql",
}

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="BITRIX_MART_LAYER_LOAD",
    default_args=default_args,
    start_date=datetime.today() - timedelta(days=1),
    catchup=False,
    max_active_tasks=2,
    tags=["bitrix", "mart", "RMS", "ROSAO"],
    template_searchpath=[DAG_DIR],
) as dag:

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    build_tasks = []
    for task_id, sql_file in MART_TABLES.items():
        build = SQLExecuteQueryOperator(
            task_id=task_id,
            conn_id=CH_CONN_ID,
            sql=f"{MART_SQL_DIR}/{sql_file}",
            split_statements=True,
        )
        build_tasks.append(build)

    start >> build_tasks >> finish
