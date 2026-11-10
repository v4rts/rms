from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger(__name__)

CH_CONN_ID = "RMS_postgreSQL"
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
MART_SQL_DIR = "SQL"

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="BITRIX_HIST_ATTRS_LOAD",
    default_args=default_args,
    start_date=datetime.today() - timedelta(days=1),
    catchup=False,
    schedule="0 3 * * *",
    max_active_tasks=2,
    tags=["bitrix", "history of attributes", "RMS", "ROSAO"],
    template_searchpath=[DAG_DIR],
) as dag:

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    trigger_mart = TriggerDagRunOperator(
        task_id="trigger_mart_layer",
        trigger_dag_id="BITRIX_MART_LAYER_LOAD",
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    build_hist = SQLExecuteQueryOperator(
        task_id="build_hist_entity_attributes",
        conn_id=CH_CONN_ID,
        sql=f"{MART_SQL_DIR}/hist_entity_attributes.sql",
        split_statements=True,
    )

    start >> build_hist >> finish >> trigger_mart

