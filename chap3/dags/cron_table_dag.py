from pathlib import Path

import pandas as pd
import pendulum
import pandas as pd
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.timetables.trigger import DeltaTriggerTimetable

def _calculate_stats(input_path, output_path):
    
    # read data from json. group data by keys date, user
    # and write the data into a different directory
    events = pd.read_json(input_path)
    stats = events.groupby(["timestamp", "user"]).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)
    
with DAG(
    
    # every 2 days from 01/01/2026 to 01/05/2026 run the pipeline
    dag_id="cron_fetch_events",
    start_date=pendulum.datetime(year=2026, month=1, day=1),
    end_date=pendulum.datetime(year=2026, month=1, day=5),
    schedule=DeltaTriggerTimetable(pendulum.duration(days=2)),
    catchup=True,
):
    # fetch 
    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
            "mkdir -p /data && "
            "curl -o /data/{{ logical_date | ds }}.json http://events-api:8081/events/latest"
        )
    )
    
    calculate_stats = PythonOperator(
        task_id="calculate_stats",
        python_callable=_calculate_stats,
        op_kwargs = {
            "input_path": "/data/events/{{logical_date | ds}}.json",
            "output_path": "/data/stats/{{logical_date | ds}}.csv",
        }
    )
    
fetch_events >> calculate_stats