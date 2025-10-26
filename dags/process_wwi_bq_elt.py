from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

import os
import json

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2025, 1, 1),
    "dbt_cloud_conn_id": os.environ.get('DBT_CLOUD_CONN_ID'),
    "account_id": os.environ.get('DBT_ACCOUNT_ID')
}

path = os.getcwd()

f = open('{0}/dags/process_wwi_bq_elt.json'.format(path),)
config = json.load(f)
f.close()

current_date = datetime.now()

def get_cutoff_date():
    if config["cutoff_date"] == "":
        return current_date
    else:
        return datetime.strptime(config["cutoff_date"], "%Y-%m-%d %H:%M:%S")
    
with DAG(
    dag_id='process_wwi_bq_elt',
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=60)
) as dag:
    load_wwi_bq = []
    load_tables = []
    i = 0
    
    for group_table in config["load_tables"]:
        i = i + 1
        load_tables = load_tables + group_table

        load_wwi_bq.append(
            PapermillOperator(
                task_id=f"load_wwi_bq_w{str(i)}",
                input_nb="{0}/notebooks/load_wwi_bq.ipynb".format(path),
                output_nb=f"{path}/notebooks/outputs/load_wwi_bq_w{str(i)}_{current_date.strftime("%Y-%m-%d %H:%M:%S")}_output.ipynb",
                parameters={
                    "newCutoff": get_cutoff_date().strftime("%Y-%m-%d %H:%M:%S"),
                    "fromNotebook": False,
                    "source": config["source"],
                    "destination": config["destination"],
                    "tables": group_table,
                    "sparkMaster": config["sparkMaster"],
                    "retriesMax": int(config["retriesMax"])
                }
            )
        )

    load_wwi_bq_test = PapermillOperator(
        task_id="load_wwi_bq_test",
        input_nb="{0}/notebooks/load_wwi_bq_test.ipynb".format(path),
        output_nb="{0}/notebooks/outputs/load_wwi_bq_test_{1}_output.ipynb".format(
            path, 
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ),
        parameters={
            "newCutoff": get_cutoff_date().strftime("%Y-%m-%d %H:%M:%S"),
            "fromNotebook": False,
            "source": config["source"],
            "destination": config["destination"],
            "tables": load_tables,
            "sparkMaster": config["sparkMaster"],
            "retriesMax": int(config["retriesMax"])
        }
    )

    set_cutoff_date_bq = PapermillOperator(
        task_id="set_cutoff_date_bq",
        input_nb="{0}/notebooks/set_cutoff_date_bq.ipynb".format(path),
        output_nb="{0}/notebooks/outputs/set_cutoff_date_bq_{1}_output.ipynb".format(
            path, 
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ),
        parameters={
            "newCutoff": get_cutoff_date().strftime("%Y-%m-%d %H:%M:%S"),
            "fromNotebook": False,
            "warehouse": config["warehouse"],
            "sparkMaster": config["sparkMaster"],
            "retriesMax": int(config["retriesMax"])
        }
    )

    warehouse_wwi_bq = DbtCloudRunJobOperator(
        task_id="warehouse_wwi_bq",
        job_id=os.environ.get('DBT_JOB_ID'),
        check_interval=10,
        timeout=3000,
        retry_from_failure=True,
        additional_run_config={
            "vars": {
                "cutoff_date": get_cutoff_date().strftime("%Y-%m-%d %H:%M:%S")
            }
        }
    )
    
    load_wwi_bq >> load_wwi_bq_test >> set_cutoff_date_bq >> warehouse_wwi_bq
    # set_cutoff_date_bq >> warehouse_wwi_bq