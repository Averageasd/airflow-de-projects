from pathlib import Path

import pandas as pd
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset

events_dataset_1 = Asset("file:///data/events_01")
events_dataset_2 = Asset("file:///data/events_2")

# macro function. referenced inside op_kwargs
# get first asset for a corresponding asset list
def _get_event(triggering_asset_events, uri):
    print(triggering_asset_events[Asset(uri)][0])
    return triggering_asset_events[Asset(uri)][0]


def _calculate_stats(input_paths, output_path):
    """Calculates event statistics."""
    # join json files from both input paths 
    events = pd.concat(
        pd.read_json(input_path, convert_dates=["timestamp"], lines=True) for input_path in input_paths
    )

    stats = (
        events.assign(date=lambda df: df["timestamp"].dt.date).groupby(["date", "user"]).size().reset_index()
    )

    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)


# we can use AssetOrTimeSchedule to combine both time interval and event-based trigger (when asset is updated)
with DAG(
    dag_id="04c_multi_consumer",
    schedule=[events_dataset_1, events_dataset_2],
    start_date=pendulum.yesterday(),
    user_defined_macros={"get_event": _get_event},
):
    calculate_stats = PythonOperator(
        task_id="calculate_stats",
        python_callable=_calculate_stats,
        op_kwargs={
            "input_paths": [
                "/data/events_01/{{ get_event(triggering_asset_events, 'file:///data/events_01').extra.date }}.json",
                "/data/events_2/{{ get_event(triggering_asset_events, 'file:///data/events_2').extra.date }}.json",
            ],
            "output_path": "/data/stats/{{ get_event(triggering_asset_events, 'file:///data/events_01').extra.date }}.csv",
        },
    )