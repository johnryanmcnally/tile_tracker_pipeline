from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# from airflow.utils.dates import days_ago 

# Initialize DAG
dag = DAG(
    dag_id = 'tile_dag',
    default_args = {'start_date': None},
    schedule = '0 23 * * *',
    catchup=False
)

extract_and_process_task = BashOperator(
    task_id = 'extract_and_process',
    bash_command = "python /opt/data_handling/extract_and_process.py",
    dag = dag
)

hdbscan_cluster_task = BashOperator(
    task_id = 'hdbscan_cluster',
    bash_command = "python /opt/data_handling/hdbscan_cluster.py",
    dag = dag
)

reverse_geocode_task = BashOperator(
    task_id = 'reverse_geocode',
    bash_command = "python /opt/data_handling/reverse_geocode.py",
    dag = dag
)

retrieve_weather_task = BashOperator(
    task_id = 'retrieve_weather',
    bash_command = "python /opt/data_handling/retrieve_weather.py",
    dag = dag
)

load_to_postgres_task = BashOperator(
    task_id = 'load_to_postgres',
    bash_command='python /opt/data_handling/postgres_load.py',
    dag = dag
)

# Assign dependencies
# Step 1 to Step 2
extract_and_process_task >> hdbscan_cluster_task

# Step 2 to both 3a and 3b (Forking)
hdbscan_cluster_task >> [reverse_geocode_task, retrieve_weather_task]

# Both 3a and 3b to Step 4 (Joining)
[reverse_geocode_task, retrieve_weather_task] >> load_to_postgres_task