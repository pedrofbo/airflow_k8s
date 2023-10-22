from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=datetime(2023, 10, 21),
    catchup=False
)
def something_with_pandas():
    @task
    def extract():
        data = [
            {"a": 1, "b": "a", "c": True},
            {"a": 2, "b": "b", "c": True},
            {"a": 3, "b": "c", "c": False},
        ]
        return data

    @task
    def transform(data: list[dict]):
        df = pd.DataFrame(data)
        df = pd.concat([
            df,
            pd.DataFrame([{"a": 4, "b": "d", "c": True}])
        ])
        df["d"] = df["a"]**2 + 10 * df["c"]

        return df

    @task
    def load(data: pd.DataFrame):
        print(f"\n{data}")

    data = extract()
    transformed_data = transform(data)
    load(transformed_data)


something_with_pandas()
