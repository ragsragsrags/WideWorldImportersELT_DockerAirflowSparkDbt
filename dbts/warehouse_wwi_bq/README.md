# WideWorldImporters Extract-Load-Transform (ELT) in Dockerized Airflow, Spark and dbt

This is an (E)xtract - (L)oad - (T)ransform of Microsoft's sample database WideWorldImporters.  This will be using Microsoft's sample WidelWorldImporters database as baseline. There are 2 workflows:
   + process_wwi_elt - This will extract and load data to another mssql database using Airflow and Spark.  Transformation is done mostly in the stored procedures called by Spark.
   + <img width="1138" height="192" alt="image" src="https://github.com/user-attachments/assets/34399c95-7c47-4113-934c-6751100fe39c" />
   + process_wwi_bq_elt - This will extract and load data to a BigQuery cloud using Airfflow and Spark.  Transformation is done by dbt.
   + <img width="1139" height="267" alt="image" src="https://github.com/user-attachments/assets/647cf0f2-8c79-48b2-bb0c-260b7bf7a825" />

Prerequisites:
   + Python (I'm using 3.10)
   + Docker Desktop (https://www.docker.com/get-started/)
   + SSMS (https://learn.microsoft.com/en-us/ssms/install/install)
   + Sql Server 2022 Developer (https://www.microsoft.com/en-us/sql-server/sql-server-downloads)
      - When installing, configure for SQL Server Authentication
      - Restore WideWorldImporters backup (https://github.com/Microsoft/sql-server-samples/releases/tag/wide-world-importers-v1.0)
   + Visual Studio Code (https://code.visualstudio.com/download)
   + Visual Studio Code Extensions
     - Docker
     - Jupyter
     - Python
   + At least 8gb memory available and 20gb available disk space
   + Dbt cloud account (could use the free trial)
   + BigQuery Account (could use the free trial)

These are the relevant documents:
   + Docker - https://docs.docker.com/get-started/
   + Apache Airflow in Docker - https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
   + Apache Spark - https://spark.apache.org/docs/latest/sql-getting-started.html
   + dbt
      - dbt operator: https://airflow.apache.org/docs/apache-airflow-providers-dbt-cloud/3.2.3/index.html
      - bigquery setup: https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup
      - dbt for visual studio: https://docs.getdbt.com/docs/install-dbt-extension (if you want to try visual studio)
      - airflow and dbt: https://docs.getdbt.com/guides/airflow-and-dbt-cloud?step=9
         + steps 7 - 9
   + Papermill
      - https://airflow.apache.org/docs/apache-airflow-providers-papermill/stable/operators.html
   + BigQuery
      - https://github.com/GoogleCloudDataproc/spark-bigquery-connector
   
For process_wwi_elt workflow, These are the dags:
   + load_wwi_w1 - loads batch 1 tables
   + load_wwi_w2 - loads batch 2 tables
   + load_wwi_test - validate loaded data
   + wh_dimension_wwi - warehouse dimension tables
   + wh_dimension_wwi_test - validate warehoused dimension data
   + wh_fact_wwi - warehouse fact tables
   + wh_fact_wwi_test - validate warehoused fact data

For process_wwi_bq_elt workflow, These are the dags:
   + load_wwi_bq_w1 - loads batch 1 tables
   + load_wwi_bq_w2 - loads batch 2 tables
   + load_wwi_bq_test - validate loaded data
   + set_cutoff_date_bq - sets the warehouse cutoff date
   + warehouse_wwi_bq - warehouse data with dbt
     
Some instructions:
1. Open Visual Studio to the folder you saved the files
2. Open a notebook in notebooks folder
3. Create virtual environment for Python (this could take a little bit)
    - Select Kernel and Create Python Environment
    <img width="975" height="199" alt="image" src="https://github.com/user-attachments/assets/26911ee9-9966-4534-9ba3-8871a20ffa96" />
4. Once virtual environment is created, open new terminal in Terminal -> New Terminal.  It should look something like this:
   <img width="479" height="88" alt="image" src="https://github.com/user-attachments/assets/687dd463-0125-438d-b2c1-f331709a0d6d" />
5. Install pyspark in the virtual environment:
   pip install pyspark==3.5.7
6. Docker initialization:
    - Initialize docker: docker compose up airflow-init
    - Build at least once: docker compose up -d --build
      <img width="490" height="220" alt="image" src="https://github.com/user-attachments/assets/bbaa3ce2-fe15-45bd-a270-1fb0403105b7" />

    - Succeeding build you can use just (unless you updated Dockerfile or requirements.txt): docker compose up -d
7. Create the warehouse database found here in mssql: scripts/WideWorldImporters.sql
8. You can update the cutoff_date in json configuration here:
    + process_wwi_elt: dags/process_wwi_elt.json
    + process_wwi_bq_elt: dags/process_wwi_bq_elt.json
   Should be at least "2013-01-01 00:00:00" and increase it every run.
   If you want to use the current date then make it blank ""
10. When testing in notebook you can update files here:
    + process_wwi_elt:
       - load: notebooks/load_wwi.json and notebooks/load_wwi.ipynb
       - test load: notebooks/load_wwi_test.json and notebooks/load_wwi_test.ipynb
       - warehouse: notebooks/warehouse_wwi.json and notebooks/warehouse_wwi.ipynb
       - test warehouse: notebooks/warehouse_wwi_test.json and notebooks/warehouse_wwi_test.ipynb 
    + process_wwi_bq_elt:
       - load: notebooks/load_wwi_bq.json and notebooks/load_wwi_bq.ipynb
       - test load: notebooks/load_wwi_bq_test.json and notebooks/load_wwi_bq_test.ipynb
       - set cutoff date: notebooks/set_cutoff_date_bq.json and notebooks/set_cutoff_date_bq.ipynb
12. Add your google cloud service account token here in: resources/credentials
      https://developers.google.com/workspace/guides/create-credentials
13. I'm using the dbtCloudOperator, so you needed to have the files saved in the repository (like github, gitlab etc..) and it needed to be built successfully also in the cloud so you could create a job that will be called by the airflow.  Another note, you need files pushed to the main branch because the build will get its files from the main branch.  In my case, I have it setup in the cloud in github and also have it in the local files through Github desktop so I could develop and test locally.  If you want to test dbt locally:
    + Install dbt for Visual Studio: https://docs.getdbt.com/docs/install-dbt-extension
    + Get the dbt credentials and save it to C:\Users\user_name\.dbt\
      <img width="1302" height="569" alt="image" src="https://github.com/user-attachments/assets/8b584e41-2b9a-4b8c-86cf-796f2482ae75" /
    + See steps 7 to 9: airflow and dbt: https://docs.getdbt.com/guides/airflow-and-dbt-cloud
    + Update this configurations in using docker compose from steps above:
      <img width="461" height="325" alt="image" src="https://github.com/user-attachments/assets/db09d9b7-3d83-4838-ac32-43c6e42d2246" />
      You can see it from the url of the job you created
      
These are the url:
- Airflow Url (this could take time load on first load): http://localhost:8080/ 
- Spark Url: http://localhost:8088/

You should see these pages:
- Airflow (login: airflow/airflow) - this could take a while to load maybe 2-3 minutes in first build
<img width="872" height="443" alt="image" src="https://github.com/user-attachments/assets/51b070f8-14cf-4556-8878-a1cd02779ea6" />

- Spark
<img width="958" height="602" alt="image" src="https://github.com/user-attachments/assets/a6a1e94c-cc35-410a-a747-53956befdf5a" />

I've used retries because I've only around 8gb avaialable memory and sometimes it stops.  If you have more memory then 1 retry should be okay.



