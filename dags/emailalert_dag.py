from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email

# Default arguments
default_args = {
    'owner': 'airflow',
    'email_on_failure': False,  # We'll handle failures with a custom callback
    'retries': 0,
}

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
        to="brskyfolls@gmail.com",
        subject=subject,
        html_content=email_content
    )

# Define a sample task that will fail
def sample_task():
    raise ValueError("This task is meant to fail for testing purposes")

# Define the DAG
with DAG(
    'html_email_template_on_failure',
    default_args=default_args,
    description='DAG to test sending HTML email on task failure',
    schedule_interval='@once',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task that is intended to fail
    failing_task = PythonOperator(
        task_id='failing_task',
        python_callable=sample_task,
        on_failure_callback=failure_callback  # Attach the failure callback
    )

failing_task
