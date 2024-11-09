from pyspark.sql import SparkSession
from pyspark.sql.functions import col, filter

def read_parquet_from_azure(storage_account_name, container_name, folder_path, account_key):
    """
    Reads all Parquet files from a specified Azure Blob Storage container into a PySpark DataFrame.

    :param storage_account_name: Azure storage account name.
    :param container_name: Azure Blob Storage container name.
    :param folder_path: Path to the folder containing the Parquet files.
    :param account_key: Account key for Azure Storage.
    :return: PySpark DataFrame containing the data from the Parquet files.
    """
    # Build the Spark session with Azure Blob Storage credentials
    spark = SparkSession.builder \
        .appName("AzureBlobToPySpark") \
        .master("local[*]") \
        .config(f"spark.hadoop.fs.azure.account.key.{storage_account_name}.blob.core.windows.net", account_key) \
        .getOrCreate()

    # Define the file URL to access all Parquet files in the folder
    file_url = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{folder_path}/*.parquet"

    # Load all Parquet files from the folder into a DataFrame
    df = spark.read.parquet(file_url)

    return df, spark

# Example usage
# if __name__ == "__main__":
#     storage_account_name = "blobinbevtestbr"
#     container_name = "bronze-inbev-test"
#     folder_path = "openbrewerydb/date=2024-11-08"  # Path to the folder containing the Parquet files
#     account_key = "S5Tfc1aNUgfAshhi0H9dXx3EPGRdcquLbhPjonlrJ93NullOdkFl9C+Xt7tBDEcQK54/UzC51sjX+AStgv88ZA=="

#     # Call the function to read data
#     df, spark = read_parquet_from_azure(storage_account_name, container_name, folder_path, account_key)

#     df.show()
#     # local_output_path = "file:///opt/airflow/dags/temp/breweries"
#     # Write the DataFrame to the local directory
#     # df.write.mode("append").parquet(local_output_path)
#     null_latitude_df = df.filter(col("latitude").isNull())

#     # Show the filtered DataFrame
#     null_latitude_df.show()
#     # Display the DataFrame
#     # df.show(truncate=False)
