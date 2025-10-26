from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator

import os
import json

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2025, 1, 1)
}

path = os.getcwd()

f = open('{0}/dags/process_wwi_elt.json'.format(path),)
config = json.load(f)
f.close()

current_date = datetime.now()

def get_cutoff_date():
    if config["cutoff_date"] == "":
        return current_date
    else:
        return datetime.strptime(config["cutoff_date"], "%Y-%m-%d %H:%M:%S")
    
with DAG(
    dag_id='process_wwi_elt',
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=60)
) as dag:
    load_wwi = []
    load_tables = []
    i = 0
    
    for group_table in config["load_tables"]:
        i = i + 1
        load_tables = load_tables + group_table

        load_wwi.append(
            PapermillOperator(
                task_id=f"load_wwi_w{str(i)}",
                input_nb="{0}/notebooks/load_wwi.ipynb".format(path),
                output_nb=f"{path}/notebooks/outputs/load_wwi_w{str(i)}_{current_date.strftime("%Y-%m-%d %H:%M:%S")}_output.ipynb",
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

    load_wwi_test = PapermillOperator(
        task_id="load_wwi_test",
        input_nb="{0}/notebooks/load_wwi_test.ipynb".format(path),
        output_nb="{0}/notebooks/outputs/load_wwi_test_{1}_output.ipynb".format(
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

    wh_dimension_wwi = PapermillOperator(
        task_id=f"wh_dimension_wwi",
        input_nb="{0}/notebooks/warehouse_wwi.ipynb".format(path),
        output_nb=f"{path}/notebooks/outputs/wh_dimension_wwi_{current_date.strftime("%Y-%m-%d %H:%M:%S")}_output.ipynb",
        parameters={
            "newCutoff": get_cutoff_date().strftime("%Y-%m-%d %H:%M:%S"),
            "fromNotebook": False,
            "destination": config["destination"],
            "tables": config["wh_dimension_tables"],
            "sparkMaster": config["sparkMaster"],
            "retriesMax": int(config["retriesMax"])
        }
    )

    wh_dimension_wwi_test = PapermillOperator(
        task_id=f"wh_dimension_wwi_test",
        input_nb="{0}/notebooks/warehouse_wwi_test.ipynb".format(path),
        output_nb=f"{path}/notebooks/outputs/wh_dimension_wwi_test_{current_date.strftime("%Y-%m-%d %H:%M:%S")}_output.ipynb",
        parameters={
            "newCutoff": get_cutoff_date().strftime("%Y-%m-%d %H:%M:%S"),
            "fromNotebook": False,
            "destination": config["destination"],
            "tables": config["wh_dimension_tables"],
            "sparkMaster": config["sparkMaster"],
            "retriesMax": int(config["retriesMax"]),
            "wh_process": "wh_dimension_wwi",
            "purge_existing": "1"
        }
    )
    
    wh_fact_wwi = PapermillOperator(
        task_id=f"wh_fact_wwi",
        input_nb="{0}/notebooks/warehouse_wwi.ipynb".format(path),
        output_nb=f"{path}/notebooks/outputs/wh_fact_wwi_{current_date.strftime("%Y-%m-%d %H:%M:%S")}_output.ipynb",
        parameters={
            "newCutoff": get_cutoff_date().strftime("%Y-%m-%d %H:%M:%S"),
            "fromNotebook": False,
            "destination": config["destination"],
            "tables": config["wh_fact_tables"],
            "sparkMaster": config["sparkMaster"],
            "retriesMax": int(config["retriesMax"])
        }
    )

    wh_fact_wwi_test = PapermillOperator(
        task_id=f"wh_fact_wwi_test",
        input_nb="{0}/notebooks/warehouse_wwi_test.ipynb".format(path),
        output_nb=f"{path}/notebooks/outputs/wh_fact_wwi_test_{current_date.strftime("%Y-%m-%d %H:%M:%S")}_output.ipynb",
        parameters={
            "newCutoff": get_cutoff_date().strftime("%Y-%m-%d %H:%M:%S"),
            "fromNotebook": False,
            "destination": config["destination"],
            "tables": config["wh_dimension_tables"],
            "sparkMaster": config["sparkMaster"],
            "retriesMax": int(config["retriesMax"]),
            "wh_process": "wh_fact_wwi",
            "purge_existing": "1"
        }
    )
    
    load_wwi >> load_wwi_test >> wh_dimension_wwi >> wh_dimension_wwi_test >> wh_fact_wwi >> wh_fact_wwi_test