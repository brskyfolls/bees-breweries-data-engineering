import os
import logging
from pyspark.sql.functions import count
from read_script import read_parquet_from_azure
from upload_script import upload_files
from delete_script import delete_folder_from_blob_storage, delete_folder

# Set up logging for better traceability
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants for Azure Storage
STORAGE_ACCOUNT_NAME = os.getenv('AZURE_ACCOUNT_NAME', 'default_account_name')
CONTAINER_NAME = os.getenv('CONTAINER_SILVER', 'silver-inbev-test')
FOLDER_PATH = "openbrewerydb/"
ACCOUNT_KEY = os.getenv('AZURE_STORAGE_ACCOUNT_KEY', 'default_account_key')
OUTPUT_PATH = "/opt/airflow/dags/temp/breweries"
GOLD_CONTAINER_NAME = os.getenv('CONTAINER_GOLD', 'gold-inbev-test')

# Helper function to read data from Azure
def read_brewery_data_from_azure():
    logger.info("Reading data from Azure...")
    return read_parquet_from_azure(STORAGE_ACCOUNT_NAME, CONTAINER_NAME, FOLDER_PATH, ACCOUNT_KEY)

# Helper function to aggregate breweries by type, city, and state
def aggregate_breweries(brewery_df):
    logger.info("Aggregating breweries data...")
    return brewery_df.groupBy("brewery_type", "city", "state_province").agg(
        count("id").alias("brewery_count")
    )

# Helper function to save DataFrame to local directory
def save_dataframe_to_local(df, name):
    local_path = f"/opt/airflow/dags/temp/breweries/{name}"
    logger.info(f"Saving {name} DataFrame to local path: {local_path}...")
    os.makedirs(local_path, exist_ok=True)
    df.coalesce(1).write.mode("append").parquet(local_path)

# Helper function to upload files to Azure Blob Storage
def upload_to_blob_storage(local_path, name):
    logger.info(f"Uploading {name} DataFrame to Azure Blob Storage...")
    upload_files(
        directory_path=local_path,
        account_name=STORAGE_ACCOUNT_NAME,
        storage_account_key=ACCOUNT_KEY,
        container_name="gold-inbev-test",
        folder_in_container=f"openbrewerydb/{name}"
    )

# Helper function to delete local files and folder
def cleanup_local_files():
    local_output_path = "/opt/airflow/dags/temp/breweries"
    logger.info(f"Cleaning up local files at {local_output_path}...")
    delete_folder(local_output_path)

# Main function to process and save brewery data
def main():
    try:
        # Step 1: Read brewery data from Azure
        brewery_df, spark = read_brewery_data_from_azure()

        # Step 2: Show initial DataFrame preview
        # brewery_df.show(brewery_df.count(), truncate=False)

        # Step 3: Aggregate breweries data
        aggregated_breweries = aggregate_breweries(brewery_df)

        # Step 4: Show aggregated DataFrame preview
        # aggregated_breweries.show(aggregated_breweries.count(), truncate=False)

        # Step 5: Save and upload DataFrames
        dataframes = {
            "aggregated_breweries": aggregated_breweries,
        }

        for name, df in dataframes.items():
            save_dataframe_to_local(df, name)

            # Delete the existing folder from Azure Blob Storage before uploading
            delete_folder_from_blob_storage(
                account_name=STORAGE_ACCOUNT_NAME,
                account_key=ACCOUNT_KEY,
                container_name=GOLD_CONTAINER_NAME,
                folder_name=f"{FOLDER_PATH}{name}"
            )

            upload_to_blob_storage(f"/opt/airflow/dags/temp/breweries/{name}", name)

        # Cleanup local files after upload
        cleanup_local_files()

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    finally:
        # Stop the Spark session
        spark.stop()
        logger.info("Spark session stopped.")

# Run the script
if __name__ == "__main__":
    main()
