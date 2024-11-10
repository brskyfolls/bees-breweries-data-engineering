import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from read_script import read_parquet_from_azure
from upload_script import upload_files
from delete_script import delete_folder, delete_folder_from_blob_storage

# Configure logging for better debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants for Azure storage
CONTAINER_NAME = "bronze-inbev-test"
FOLDER_PATH = "openbrewerydb"
STORAGE_ACCOUNT_NAME = os.getenv('AZURE_ACCOUNT_NAME', 'default_account_name')
ACCOUNT_KEY = os.getenv('AZURE_STORAGE_ACCOUNT_KEY', 'default_account_key')
CONTAINER_NAME = os.getenv('CONTAINER_BRONZE', 'bronze-inbev-test')


# Constants for output paths
OUTPUT_DIR = "/opt/airflow/dags/temp/breweries"
SILVER_CONTAINER_NAME = os.getenv('CONTAINER_SILVER', 'silver-inbev-test')
SILVER_FOLDER_PATH = "openbrewerydb"


# Function to clean data and replace nulls with default values
def clean_data(df: 'DataFrame'):
    """
    Cleans the input DataFrame by replacing null values based on data type.
    """
    NULL_VALUE_MAP = {
        "string": "TBD",
        "double": -1
    }

    for column_name in df.columns:
        column_type = df.schema[column_name].dataType.typeName()
        if column_type in NULL_VALUE_MAP:
            null_value = NULL_VALUE_MAP[column_type]
            df = df.withColumn(
                column_name,
                when(col(column_name).isNull(), lit(null_value)).otherwise(col(column_name))
            )
    return df

# Function to write the DataFrame to the local file system
def write_to_local(df: 'DataFrame', output_path: str, spark):
    """
    Writes the DataFrame to the local file system, partitioned by 'brewery_type'.
    """
    try:
        logger.info(f"Writing data to {output_path}...")
        # Set Spark configurations to disable unnecessary metadata
        spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        spark.conf.set("parquet.enable.summary-metadata", "false")
        
        df.coalesce(1).write.mode("append").partitionBy("brewery_type").parquet(output_path)
        logger.info(f"Data successfully written to {output_path}")
    except Exception as e:
        logger.error(f"Error writing data to local: {e}")
        raise

# Function to upload data to Azure Blob Storage
def upload_to_azure(local_path: str, storage_account_name: str, account_key: str, container_name: str, folder_in_container: str):
    """
    Uploads files from the local path to Azure Blob Storage.
    """
    try:
        logger.info(f"Uploading data from {local_path} to Azure Blob Storage...")
        upload_files(
            directory_path=local_path,
            account_name=storage_account_name,
            storage_account_key=account_key,
            container_name=container_name,
            folder_in_container=folder_in_container
        )
        logger.info(f"Data successfully uploaded to {container_name}/{folder_in_container}")
    except Exception as e:
        logger.error(f"Error uploading data to Azure: {e}")
        raise

# Main processing logic
def main():
    try:
        # Read data from Azure
        logger.info("Reading data from Azure...")
        brewery_df, spark = read_parquet_from_azure(
            storage_account_name=STORAGE_ACCOUNT_NAME,
            container_name=CONTAINER_NAME,
            folder_path=FOLDER_PATH,
            account_key=ACCOUNT_KEY
        )

        # Clean data by replacing nulls
        logger.info("Cleaning data...")
        df_cleaned = clean_data(brewery_df)

        # Select relevant columns for the Silver Layer
        logger.info("Selecting relevant columns for the Silver Layer...")
        df_silver = df_cleaned.select(
            "id", "name", "brewery_type", "address_1", "address_2", "address_3", "city", 
            "state_province", "postal_code", "country", "longitude", "latitude", "phone", 
            "website_url", "state", "street"
        )

        # Show the cleaned Silver layer data
        df_silver.show(truncate=False)

        # Ensure the output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        delete_folder_from_blob_storage(
            account_name=STORAGE_ACCOUNT_NAME,
            account_key=ACCOUNT_KEY,
            container_name=SILVER_CONTAINER_NAME,
            folder_name=SILVER_FOLDER_PATH
        )

        # Write the cleaned data to the local file system
        write_to_local(df_silver, OUTPUT_DIR, spark)

        # Upload the data to Azure
        upload_to_azure(OUTPUT_DIR, STORAGE_ACCOUNT_NAME, ACCOUNT_KEY, SILVER_CONTAINER_NAME, SILVER_FOLDER_PATH)

        # Delete the local temporary files
        logger.info("Deleting local temporary files...")
        delete_folder(OUTPUT_DIR)
        logger.info("Local temporary files deleted.")

    except Exception as e:
        logger.error(f"Error during data processing: {e}")
    finally:
        # Stop the Spark session
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()
