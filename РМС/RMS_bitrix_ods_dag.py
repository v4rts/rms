from datetime import datetime, timedelta
import logging
import os

from dotenv import load_dotenv

from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from RMS.python.ods_extract import task_extract
from RMS.python.ods_transform import task_transform
from RMS.python.ods_load import task_load
from RMS.constants import WEBHOOK_CONFIG

logger = logging.getLogger(__name__)

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path=os.path.join(DAG_DIR, ".env"), override=True)

BITRIX_WEBHOOK = os.getenv("BITRIX_WEBHOOK_URL")
PG_CONN_ID = "RMS_postgreSQL"

# Каталог для промежуточных файлов (json/parquet). По умолчанию как раньше;
# на сервере без /tmp задайте BITRIX_TEMP_DIR в .env или в окружении Airflow.
TEMP_DATA_DIR = os.getenv("BITRIX_TEMP_DIR", "/tmp/airflow_bitrix/")

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="BITRIX_ODS_LAYER_LOAD",
    default_args=default_args,
    # Airflow 3: schedule_interval заменён на schedule.
    # Время в UTC (03:00 UTC = 06:00 МСК), как и раньше.
    schedule="0 3 * * *",
    # Фиксированная дата: динамический datetime.today() в Airflow 3 не рекомендуется.
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_tasks=2,
    tags=["bitrix", "ods", "RMS", "ROSAO"],
    template_searchpath=[DAG_DIR],
) as dag:

    start = EmptyOperator(task_id="start")

    # PostgresOperator удалён в новом провайдере -> SQLExecuteQueryOperator (conn_id).
    drop_tables = SQLExecuteQueryOperator(
        task_id="sql_drop_tables",
        conn_id=PG_CONN_ID,
        sql="SQL/ods_drop.sql",
    )

    create_dim_bstatus_norm = SQLExecuteQueryOperator(
        task_id="create_dim_bstatus_norm",
        conn_id=PG_CONN_ID,
        sql="SQL/dim_business_status_norm.sql",
    )

    finish = EmptyOperator(task_id="finish")

    trigger_mart = TriggerDagRunOperator(
        task_id="trigger_mart_layer",
        trigger_dag_id="BITRIX_HIST_ATTRS_LOAD",
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    extract_tasks = {}

    for config in WEBHOOK_CONFIG:
        target_table = config["target_table"]
        table_name = target_table.split(".")[-1]
        extract_path = os.path.join(TEMP_DATA_DIR, f"{table_name}_raw.json")
        transform_path = os.path.join(TEMP_DATA_DIR, f"{table_name}_processed.parquet")

        extract = PythonOperator(
            task_id=f"extract_{table_name}",
            python_callable=task_extract,
            op_args=[BITRIX_WEBHOOK, target_table, extract_path],
        )

        transform = PythonOperator(
            task_id=f"transform_{table_name}",
            python_callable=task_transform,
            op_args=[extract_path, transform_path, target_table],
        )

        load = PythonOperator(
            task_id=f"load_{table_name}",
            python_callable=task_load,
            op_args=[transform_path, table_name, PG_CONN_ID],
        )

        extract_tasks[target_table] = extract

        (
            start
            >> extract
            >> transform
            >> drop_tables
            >> create_dim_bstatus_norm
            >> load
            >> finish
        )

    for config in WEBHOOK_CONFIG:
        target_table = config["target_table"]
        parent_table = config.get("parent_table")

        if parent_table and parent_table in extract_tasks:
            extract_tasks[parent_table] >> extract_tasks[target_table]

    finish >> trigger_mart
