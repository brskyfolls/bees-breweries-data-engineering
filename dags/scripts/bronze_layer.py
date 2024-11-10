import os
import logging
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from upload_script import upload_files
from delete_script import delete_folder, delete_folder_from_blob_storage

# Configure logging for better traceability
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration for storage and file paths
ACCOUNT_NAME = os.getenv('AZURE_ACCOUNT_NAME', 'default_account_name')
ACCOUNT_KEY = os.getenv('AZURE_STORAGE_ACCOUNT_KEY', 'default_account_key')
CONTAINER_NAME = os.getenv('CONTAINER_BRONZE', 'bronze-inbev-test')

LOCAL_TEMP_PATH = "/opt/airflow/dags/temp/breweries"
FOLDER_PATH = "openbrewerydb"
API_URL = "https://api.openbrewerydb.org/breweries"

# Initialize Spark session
def init_spark():
    """ Initialize and configure the Spark session. """
    logging.info("Initializing Spark session.")
    return SparkSession.builder \
        .appName("Brewery Data Pipeline") \
        .master("local[*]") \
        .getOrCreate()

# Define schema for the brewery data
def define_schema():
    """ Define the schema for the brewery data. """
    return StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("address_1", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("street", StringType(), True)
    ])

# Fetch brewery data from API with pagination
def fetch_brewery_data():
    """ Fetch and process brewery data from the API. """
    logging.info("Fetching brewery data from the API.")
    all_data = []
    page = 0

    while True:
        response = requests.get(API_URL, params={'page': page, 'per_page': 50})
        
        if response.status_code == 200:
            data = response.json()
            if not data:  # Exit if no data is returned
                break

            # Handle longitude and latitude conversion
            for entry in data:
                entry['longitude'] = safe_float(entry.get('longitude'))
                entry['latitude'] = safe_float(entry.get('latitude'))
            
            all_data.extend(data)
            page += 1
        else:
            logging.error(f"Error fetching data: {response.status_code}")
            break

    logging.info(f"Fetched {len(all_data)} records.")
    return all_data

# Safe float conversion to handle invalid values
def safe_float(value):
    """ Safely convert a value to float, returning None if invalid. """
    try:
        return float(value) if value else None
    except (ValueError, TypeError):
        return None

# Write DataFrame to parquet format
def write_to_parquet(df):
    """ Write the DataFrame to the local directory in Parquet format. """
    logging.info(f"Writing data to {LOCAL_TEMP_PATH}.")
    os.makedirs(os.path.dirname(LOCAL_TEMP_PATH), exist_ok=True)
    df.coalesce(1).write.mode("append").parquet(LOCAL_TEMP_PATH)

# Upload the parquet files to Azure Blob Storage
def upload_to_storage():
    """ Upload the Parquet files to Azure Blob Storage. """
    logging.info("Uploading data to Azure Blob Storage.")
    upload_files(
        directory_path=LOCAL_TEMP_PATH,
        account_name=ACCOUNT_NAME,
        storage_account_key=ACCOUNT_KEY,
        container_name=CONTAINER_NAME,
        folder_in_container=FOLDER_PATH
    )

# Clean up temporary files
def cleanup():
    """ Clean up temporary files. """
    logging.info("Cleaning up temporary files.")
    delete_folder(LOCAL_TEMP_PATH)

# Main pipeline execution
def run_pipeline():
    """ Main execution flow of the pipeline. """
    logging.info("Pipeline execution started.")
    
    # Initialize Spark session
    spark = init_spark()
    
    # Define schema and fetch brewery data
    brewery_data = fetch_brewery_data()
    brewery_df = spark.createDataFrame(brewery_data, schema=define_schema())
    
    # Show the DataFrame (Optional: preview the data)
    brewery_df.show(truncate=False)

    delete_folder_from_blob_storage(
        account_name=ACCOUNT_NAME,
        account_key=ACCOUNT_KEY,
        container_name=CONTAINER_NAME,
        folder_name=FOLDER_PATH
    )

    # Write data to Parquet
    write_to_parquet(brewery_df)

    # Upload to Azure Blob Storage
    upload_to_storage()

    # Clean up temporary files
    cleanup()

    # Stop the Spark session
    spark.stop()

    logging.info("Pipeline execution completed.")

# Execute the pipeline
if __name__ == "__main__":
    run_pipeline()
