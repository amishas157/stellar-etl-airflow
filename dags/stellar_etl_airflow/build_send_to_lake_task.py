"""
This file contains functions for creating Airflow tasks to send the from Google Cloud Storage into data lake.
"""
import json

from airflow.models import Variable


def send_data_to_lake(source_object, data_lake_bucket_name):
    from google.cloud import storage

    bucket_name = Variable.get("gcs_exported_data_bucket_name")

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_object)
    with open("r") as file:
        linhas = file.readlines()

    bucket = storage_client.get_bucket(data_lake_bucket_name)
    start = {"ledger_sequence": 0}
    for linha in linhas:
        dados = json.loads(linha)
        blob = bucket.blob(f'{dados["ledger_sequence"]}.txt')
        ledger_sequence = dados["ledger_sequence"]
        if ledger_sequence == start["ledger_sequence"]:
            with blob.open(f"{ledger_sequence}.txt", "a") as file:
                file.write(linha)
        else:
            with blob.open(f"{ledger_sequence}.txt", "w") as file:
                file.write(linha)
        start = dados
