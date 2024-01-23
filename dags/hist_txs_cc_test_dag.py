from datetime import datetime

from airflow import DAG
from stellar_etl_airflow import macros
from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_gcs_to_bq_task import build_gcs_to_bq_task
from stellar_etl_airflow.build_time_test_task import build_time_task
from stellar_etl_airflow.default import get_default_dag_args

dag = DAG(
    "hist_txs_cc_performance_test_dag",
    default_args=get_default_dag_args(),
    max_active_runs=1,
    start_date=datetime(2023, 10, 31, 11, 0, 0),
    catchup=False,
    schedule_interval="@hourly",
    params={
        "alias": "cc",
    },
    user_defined_macros={
        "subtract_data_interval": macros.subtract_data_interval,
        "batch_run_date_as_datetime_string": macros.batch_run_date_as_datetime_string,
    },
)

internal_project = "test-hubble-319619"
internal_dataset = "hist_txs_xdr_lucas_santos"
use_testnet = False
use_futurenet = False

time_task = build_time_task(dag, use_testnet=True, use_futurenet=use_futurenet)

tx_export_task = build_export_task(
    dag,
    "archive",
    "export_transactions",
    "transactions.txt",
    use_testnet=use_testnet,
    use_futurenet=use_futurenet,
    use_gcs=True,
    resource_cfg="cc",
)

send_txs_to_bq_task = build_gcs_to_bq_task(
    dag,
    tx_export_task.task_id,
    internal_project,
    internal_dataset,
    "history_transactions",
    "",
    partition=True,
    cluster=True,
)

time_task >> tx_export_task >> send_txs_to_bq_task
