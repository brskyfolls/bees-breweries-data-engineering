from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.utils.email import send_email
import os

# Define the HTML template globally
email_template = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Email Template</title>
</head>
<body style="margin: 0; padding: 0; font-family: Arial, sans-serif; background-color: #f4f4f4;">
  <table role="presentation" style="width: 100%; border-collapse: collapse; background-color: #f4f4f4;">
    <tr>
      <td align="center" style="padding: 20px 0;">
        <!-- Logo Section -->
        <table role="presentation" style="width: 600px; background-color: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1);">
          <tr>
            <td align="center" style="padding: 20px; background-color: #FFD700; color: #ffffff;">
              <h1 style="margin: 0; font-size: 24px;"> DAG {task_instance}</h1>
            </td>
          </tr>
          <!-- Body Section -->
          <tr>
            <td style="padding: 20px; color: #333333;">
              <h2 style="font-size: 20px; margin-bottom: 10px;">Dear Engineer,</h2>
              <p style="font-size: 16px; line-height: 1.5; margin-bottom: 20px;">
                We are notifying you that a task in your Airflow pipeline failed. Please find the details below:
              </p>
              
              <p style="font-size: 16px; line-height: 1.5; margin-bottom: 20px;">
                <strong>Task ID:</strong> {task_id}<br>
                <strong>Error Message:</strong> {exception}<br>
                <strong>Execution Date:</strong> {execution_date}<br>
                <strong>Retry Attempt:</strong> {try_number}<br>
              </p>

              <p style="font-size: 16px; line-height: 1.5; margin-bottom: 20px;">
                Please review the logs and take the necessary actions to resolve the issue. The task has failed and may require your attention to fix any underlying issues.
              </p>
              
              <p style="font-size: 16px; line-height: 1.5;">
                If you have any questions or need assistance, feel free to contact us.
              </p>
            </td>
          </tr>
          <!-- Button Section -->
          <tr>
            <td align="center" style="padding: 20px;">
              <a href="{log_url}" style="background-color: #FFD700; color: #ffffff; text-decoration: none; padding: 10px 20px; border-radius: 5px; font-size: 16px;">
                Learn More
              </a>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>
"""

# Function to send the email with HTML content on failure
def failure_callback(context):
    # Define the email subject
    subject = f"DAG {context['task_instance'].dag_id} Task Failed: {context['task_instance'].task_id}"
    
    # Get the error details from the context
    task_instance = context['task_instance'].dag_id
    task_id = context['task_instance'].task_id
    exception = context.get('exception', 'No exception message')
    execution_date = context['execution_date']
    try_number = context['ti'].try_number
    log_url = context['task_instance'].log_url

    # Replace the placeholders in the email template
    global email_template

    # Dynamically format the email template
    email_content = email_template.format(
        task_instance=str(task_instance),
        task_id=str(task_id),
        exception=str(exception),
        execution_date=str(execution_date),
        try_number=str(try_number),
        log_url=str(log_url)
    )

    # Send email with HTML template
    send_email(
        to=os.getenv('EMAIL_TO_SEND_ALERT', 'brskyfolls@gmail.com'),
        subject=subject,
        html_content=email_content
    )

# Define default_args for your DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 9),
    'retries': 0,
}

# Define your DAG
dag = DAG(
    'openbrewerydb_dag',  # Name of the DAG
    default_args=default_args,
    description='Medallion Architecture of OpenBreweryDB API',
    schedule_interval=' 0 16 * * *',  # Change to a cron expression or timedelta for scheduling
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
        on_failure_callback=failure_callback,
        dag=dag,
    )

# Set dynamic task dependencies: bronze -> silver -> gold
spark_submit_tasks['bronze'] >> spark_submit_tasks['silver'] >> spark_submit_tasks['gold']
