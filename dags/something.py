"""
DAG that does something... :)
"""
from copy import deepcopy
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=datetime(2023, 10, 21),
    catchup=False,
    doc_md=__doc__
)
def something():
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
        new_data = deepcopy(data)
        new_data.append({"a": 4, "b": "d", "c": True})
        for entry in new_data:
            entry["d"] = entry["a"]**2 + 10 * entry["c"]

        return new_data

    @task
    def load(data: list[dict]):
        for entry in data:
            print(entry)

    data = extract()
    transformed_data = transform(data)
    load(transformed_data)


something()
