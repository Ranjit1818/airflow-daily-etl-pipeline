import json
import pendulum
import requests

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from datetime import datetime


# ---------------- DAG ---------------- #


dag = DAG(
    dag_id="App_incremental_load",
    start_date=datetime(2026, 7, 1),
    schedule=CronDataIntervalTimetable(
        cron="0 0 * * *",
        timezone=pendulum.timezone("Asia/Kolkata"),
    ),
    catchup=True, #catch the previous date to run this
)


# ---------------- TASK ---------------- #

def fetch_api_data(**context):
    # Get templated values
    url = context["templates_dict"]["url"]
    output_path = context["templates_dict"]["output_path"]

    # Execution date
    start_date = context["ds"]
    end_date = context["ds"]

    print(f"Start Date : {start_date}")
    print(f"End Date   : {end_date}")

    payload = {
        "start_date": start_date,
        "end_date": end_date,
    }

    try:
        response = requests.post(
            url,
            json=payload,
            auth=("admin", "manish"),   # HTTP Basic Authentication
            timeout=30,
        )

        print(f"Status Code : {response.status_code}")

        # Raise exception for 4xx/5xx responses
        response.raise_for_status()

        data = response.json()

        with open(output_path, "w") as file:
            json.dump(data, file, indent=4)

        print(f"Data saved successfully to: {output_path}")

    except requests.exceptions.RequestException as e:
        print(f"API Request Failed: {e}")
        raise


# ---------------- OPERATOR ---------------- #

pull_api_data = PythonOperator(
    task_id="pull_api_data",
    python_callable=fetch_api_data,
    templates_dict={
        "url": "http://fastapi-app:5000/getAll",
        "output_path": "/opt/airflow/output_files/dag_result_{{ ds }}.json",
    },
    dag=dag,
)