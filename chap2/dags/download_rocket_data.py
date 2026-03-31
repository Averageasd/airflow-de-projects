import json
import pathlib

import pendulum
import requests
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from requests.exceptions import ConnectionError, MissingSchema



def _get_pictures():             #1
    # Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    # Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except ConnectionError:
                raise ConnectionError(f"Could not connect to {image_url}.")


with DAG(                                           #2
    dag_id="01_download_rocket_launches",           #3
    start_date=pendulum.today('UTC').add(days=-14), #4
    schedule=None,                                  #5
):

    download_launches = BashOperator(               #6
        task_id="download_launches",                #7
        bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    )

    get_pictures = PythonOperator(               #8
        task_id="get_pictures", #8
        python_callable=_get_pictures,            #8
    )

    notify = BashOperator(
        task_id="notify",
        bash_command='echo "There are $(ls /tmp/images/ | wc -l) images."',
    )

download_launches >> get_pictures >> notify