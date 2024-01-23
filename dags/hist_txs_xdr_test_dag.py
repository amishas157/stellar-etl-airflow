from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
)
from stellar_etl_airflow import macros
from stellar_etl_airflow.build_bq_insert_job_task import (
    file_to_string,
    get_query_filepath,
)
from stellar_etl_airflow.build_dbt_test_task import build_dbt_task
from stellar_etl_airflow.default import get_default_dag_args

with DAG(
    "hist_txs_xdr_performance_test_dag",
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
) as dag:
    table = "hist_txs_xdr"
    internal_project = "test-hubble-319619"
    internal_dataset = "hist_txs_xdr_lucas_santos"
    use_testnet = False
    use_futurenet = False

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
                    {"mode": "NULLABLE", "name": "sponsor", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "closed_at", "type": "TIMESTAMP"},
                ]
            },
            "externalDataConfiguration": {
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "sourceUris": ["gs://ledger_transaction_data_lake/prod/*.txt"],
                "compression": "NONE",
                "jsonOptions": {"encoding": "UTF-8"},
            },
        },
    )

    # replace by dbt model
    query_path = get_query_filepath("hist_txs_lucas_santos")
    query = file_to_string(query_path)
    configuration = {
        "query": {
            "query": query,
            "destinationTable": {
                "projectId": internal_project,
                "datasetId": internal_dataset,
                "tableId": table,
            },
            "useLegacySql": False,
        }
    }
    configuration["query"]["writeDisposition"] = "WRITE_TRUNCATE"

    create_hist_txs = BigQueryInsertJobOperator(
        task_id=f"create_table",
        configuration=configuration,
    )

    create_external_table >> create_hist_txs
    # replace by dbt model

    dbt_hist_txs = build_dbt_task(
        dag,
        "hist_txs_xdr",
        command_type="run",
        resource_cfg="cc",
        project="test",
    )

    # scanned bytes bq job
    # task duration
    # resources utilized
    # task que roda precisa de uma máquina com tamanho x e fica y tempo rodando n
    # vai poder

    # usar o regsitry privado
    # techindicum/stellar-dbt
