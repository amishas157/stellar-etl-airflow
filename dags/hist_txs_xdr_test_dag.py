from ast import literal_eval
from datetime import datetime

from airflow import DAG
from airflow.models.variable import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from stellar_etl_airflow import macros
from stellar_etl_airflow.build_batch_stats import build_batch_stats
from stellar_etl_airflow.build_dbt_test_task import build_dbt_task, dbt_task
from stellar_etl_airflow.build_export_test_task import build_export_task
from stellar_etl_airflow.build_time_test_task import build_time_task
from stellar_etl_airflow.default import get_default_dag_args

with DAG(
    "hist_txs_xdr_performance_test_dag",
    default_args=get_default_dag_args(),
    max_active_runs=1,
    start_date=datetime(2024, 1, 26, 0, 0, 0),
    catchup=False,
    schedule_interval="@hourly",
    params={
        "alias": "cc",
    },
    user_defined_macros={
        "subtract_data_interval": macros.subtract_data_interval,
        "batch_run_date_as_datetime_string": macros.batch_run_date_as_datetime_string,
    },
) as dag:
    table = "hist_txs_xdr"
    internal_project = "{{ var.value.bq_project }}"
    internal_dataset = "{{ var.value.bq_dataset }}"
    use_testnet = literal_eval(Variable.get("use_testnet"))
    use_futurenet = literal_eval(Variable.get("use_futurenet"))
    bucket_name = Variable.get("gcs_exported_data_bucket_name")
    time_task = build_time_task(
        dag, use_testnet=use_testnet, use_futurenet=use_futurenet
    )

    write_tx_stats = build_batch_stats(dag, "export_ledger_transaction")

    tx_export_task = build_export_task(
        dag,
        "archive",
        "export_ledger_transaction",
        "ledger_transactions.txt",
        use_testnet=use_testnet,
        use_futurenet=use_futurenet,
        use_gcs=True,
        resource_cfg="cc",
    )

    copy_single_file = GCSToGCSOperator(
        task_id="send_to_lake",
        source_bucket=bucket_name,
        source_object=[
            "{{ task_instance.xcom_pull(task_ids='"
            + tx_export_task.task_id
            + '\')["output"] }}'
        ],
        destination_bucket="ledger_transaction_data_lake",
        destination_object="prod/ledger_transactions.txt",  # If not supplied the source_object value will be used
        exact_match=True,
    )

    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        table_resource={
            "tableReference": {
                "projectId": internal_project,
                "datasetId": internal_dataset,
                "tableId": "xdr_external_table_prod",
            },
            "schema": {
                "fields": [
                    {"mode": "NULLABLE", "name": "ledger_sequence", "type": "INTEGER"},
                    {"mode": "NULLABLE", "name": "tx_envelope", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "tx_fee_meta", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "tx_ledger_history", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "tx_meta", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "tx_result", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "batch_id", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "batch_run_date", "type": "DATETIME"},
                    {
                        "mode": "NULLABLE",
                        "name": "batch_insert_ts",
                        "type": "TIMESTAMP",
                    },
                    {"mode": "NULLABLE", "name": "closed_at", "type": "TIMESTAMP"},
                ]
            },
            "externalDataConfiguration": {
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "sourceUris": [
                    "gs://us-central1-hubble-2-d948d67b-bucket/dag-exported/scheduled__2024-01-26T13:00:00+00:00/49991480-49991782-ledger_transactions.txt"
                ],
                "compression": "NONE",
                "jsonOptions": {"encoding": "UTF-8"},
            },
        },
    )

    dbt_stg_hist_txs = dbt_task(
        dag, "stg_history_ledger_transaction", command_type="run"
    )

    dbt_hist_txs = dbt_task(dag, "hist_txs", command_type="run")

    (
        time_task
        >> write_tx_stats
        >> tx_export_task
        >> copy_single_file
        >> create_external_table
        >> dbt_stg_hist_txs
        >> dbt_hist_txs
    )

    # scanned bytes bq job
    # task duration
    # resources utilized
    # task que roda precisa de uma máquina com tamanho x e fica y tempo rodando n
    # vai poder

    # usar o regsitry privado
    # techindicum/stellar-dbt
