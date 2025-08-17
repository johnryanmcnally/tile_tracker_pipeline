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

# Step 1
extract_tile_data_task = BashOperator(
    task_id = 'extract_tile_data',
    bash_command = "python /opt/data_handling/extract_tile_data.py",
    dag = dag
)

# Step 2
feature_engineering_task = BashOperator(
    task_id = 'feature_engineering',
    bash_command = "python /opt/data_handling/feature_engineering.py",
    dag = dag
)

# Step 3a
reverse_geocode_task = BashOperator(
    task_id = 'reverse_geocode',
    bash_command = "python /opt/data_handling/reverse_geocode.py",
    dag = dag
)

# Step 3b
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
extract_tile_data_task >> feature_engineering_task

# Step 2 to both 3a and 3b
feature_engineering_task >> [reverse_geocode_task, retrieve_weather_task]

# Both 3a and 3b to Step 4
[reverse_geocode_task, retrieve_weather_task] >> load_to_postgres_task