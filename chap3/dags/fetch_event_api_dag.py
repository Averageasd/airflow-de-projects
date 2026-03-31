from pathlib import Path

import pandas as pd
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _calculate_stats(input_path, output_path):
    
    # read data from json. group data by keys date, user
    # and write the data into a different directory
    events = pd.read_json(input_path)
    stats = events.groupby(["timestamp", "user"]).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)
    
with DAG(
    dag_id="unscheduled_fetch_events",
    schedule=None
):
    # fetch 
    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
            "mkdir -p /data && "
            "curl -o /data/events.json http://events-api:8081/events/latest"
        )
    )
    
    calculate_stats = PythonOperator(
        task_id="calculate_stats",
        python_callable=_calculate_stats,
        op_kwargs = {
            "input_path": "/data/events.json",
            "output_path": "/data/stats.csv",
        }
    )
    
fetch_events >> calculate_stats