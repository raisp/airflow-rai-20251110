import os
from datetime import datetime

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.decorators import task

input_path_1 = "/opt/airflow/data/STT1.csv"
input_path_2 = "/opt/airflow/data/STT2.csv"
output_path = "/opt/airflow/data/STT_AGGREGATED.csv"

with DAG(
    dag_id="aggregate_stt_data",
    start_date=datetime(2025, 11, 9),
    schedule=None,
) as dag:

    @task
    def read_csv_to_df(path: str) -> pd.DataFrame:
        if not os.path.exists(path):
            raise FileNotFoundError(f"{path} not found on worker")
        df = pd.read_csv(path)

        return df

    @task
    def aggregate_json(df_1: pd.DataFrame, df_2: pd.DataFrame):
        combined = pd.concat([df_1, df_2], ignore_index=True)
        deduped = combined.drop_duplicates(subset=["number"], keep="last").reset_index(
            drop=True
        )

        deduped["debit"] = deduped.loc[deduped["client_type"] == "C", "amount"]
        deduped["credit"] = deduped.loc[deduped["client_type"] == "V", "amount"]

        aggregated = deduped.groupby(["date", "client_code"], as_index=False).agg(
            {"debit": "sum", "credit": "sum"}
        )

        aggregated["credit"] = aggregated["credit"].replace(0, np.nan).astype("Int64")
        aggregated["debit"] = aggregated["debit"].replace(0, np.nan).astype("Int64")
        aggregated.to_csv(output_path, index=False)

    # wiring
    df_1 = read_csv_to_df(input_path_1)
    df_2 = read_csv_to_df(input_path_2)
    aggregated = aggregate_json(df_1, df_2)

    df_1 >> aggregated
    df_2 >> aggregated
