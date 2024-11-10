from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define default_args for your DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 9),
    'retries': 1,
}

# Define your DAG
dag = DAG(
    'openbrewerydb_dag',  # Name of the DAG
    default_args=default_args,
    description='Medallion Architecture of OpenBreweryDB API',
    schedule_interval=None,  # Change to a cron expression or timedelta for scheduling
)

# List of layers (bronze, silver, gold)
layers = ['bronze', 'silver', 'gold']

# Loop through the layers and dynamically create tasks
spark_submit_tasks = {}
for layer in layers:
    task_id = f'spark_submit_{layer}_job'
    application_path = f'/opt/airflow/dags/scripts/{layer}_layer.py'  # Path to the script for each layer
    jar_paths = (
        '/opt/airflow/dags/scripts/jars/hadoop-azure-3.4.1.jar,'
        '/opt/airflow/dags/scripts/jars/azure-storage-8.6.6.jar,'
        '/opt/airflow/dags/scripts/jars/jetty-server-9.4.45.v20220203.jar,'
        '/opt/airflow/dags/scripts/jars/jetty-util-9.4.45.v20220203.jar,'
        '/opt/airflow/dags/scripts/jars/jetty-util-ajax-9.4.45.v20220203.jar'
    )
    
    # Create the bash command to run spark-submit with your provided command
    bash_command = f"""
    spark-submit \
        --master spark://spark-master:7077 \
        --jars {jar_paths} \
        {application_path}
    """
    
    # Use BashOperator to execute the command
    spark_submit_tasks[layer] = BashOperator(
        task_id=task_id,
        bash_command=bash_command,  # Run the exact spark-submit command
        dag=dag,
    )

# Set dynamic task dependencies: bronze -> silver -> gold
spark_submit_tasks['bronze'] >> spark_submit_tasks['silver'] >> spark_submit_tasks['gold']
