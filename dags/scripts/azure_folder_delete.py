import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Azure Blob Storage account details
account_name = "blobinbevtestbr"
storage_account_key = "S5Tfc1aNUgfAshhi0H9dXx3EPGRdcquLbhPjonlrJ93NullOdkFl9C+Xt7tBDEcQK54/UzC51sjX+AStgv88ZA=="
container_name = "bronze-inbev-test"

# Path to the local JARs in the master container
spark_jars_path = "/opt/airflow/dags/scripts/jars/hadoop-azure-3.4.1.jar,/opt/airflow/dags/scripts/jars/azure-storage-8.6.6.jar,/opt/airflow/dags/scripts/jars/jetty-server-9.4.45.v20220203.jar,/opt/airflow/dags/scripts/jars/jetty-util-9.4.45.v20220203.jar,/opt/airflow/dags/scripts/jars/jetty-util-ajax-9.4.45.v20220203.jar"
# spark-submit   --master spark://spark-master:7077   --jars /opt/airflow/dags/scripts/jars/hadoop-azure-3.4.1.jar,/opt/airflow/dags/scripts/jars/azure-storage-8.6.6.jar,/opt/airflow/dags/scripts/jars/jetty-server-9.4.45.v20220203.jar,/opt/airflow/dags/scripts/jars/jetty-util-9.4.45.v20220203.jar,/opt/airflow/dags/scripts/jars/jetty-util-ajax-9.4.45.v20220203.jar   /opt/airflow/dags/scripts/azure_script.py
# spark-submit   --master spark://spark-master:7077 /opt/airflow/dags/scripts/azure_script.py
# Initialize Spark session with the local JARs added
spark = (
    SparkSession.builder.appName("Brewery Data Pipeline")
    .config("spark.jars", spark_jars_path)  # Specify the local JAR files
    .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    .config(f"spark.hadoop.fs.azure.account.key.{account_name}.blob.core.windows.net", storage_account_key)
    .getOrCreate()
)

# Set Azure Blob Storage authentication details
spark.sparkContext._jsc.hadoopConfiguration().set(
    f"fs.azure.account.key.{account_name}.blob.core.windows.net", storage_account_key
)
spark.sparkContext._jsc.hadoopConfiguration().set(
    "spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem"
)
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
# Define the schema based on the API response structure
# brewery_schema = StructType([
#     StructField("id", StringType(), True),
#     StructField("name", StringType(), True),
#     StructField("brewery_type", StringType(), True),
#     StructField("address_1", StringType(), True),
#     StructField("address_2", StringType(), True),
#     StructField("address_3", StringType(), True),
#     StructField("city", StringType(), True),
#     StructField("state_province", StringType(), True),
#     StructField("postal_code", StringType(), True),
#     StructField("country", StringType(), True),
#     StructField("longitude", DoubleType(), True),
#     StructField("latitude", DoubleType(), True),
#     StructField("phone", StringType(), True),
#     StructField("website_url", StringType(), True),
#     StructField("state", StringType(), True),
#     StructField("street", StringType(), True)
# ])

# # Function to fetch data from the Open Brewery DB API and format it
# def fetch_brewery_data():
#     url = "https://api.openbrewerydb.org/breweries"
#     response = requests.get(url)
    
#     if response.status_code == 200:
#         data = response.json()
        
#         # Convert longitude and latitude fields to float, handling invalid entries
#         for entry in data:
#             entry['longitude'] = float(entry['longitude']) if entry.get('longitude') else None
#             entry['latitude'] = float(entry['latitude']) if entry.get('latitude') else None
        
#         return data
#     else:
#         print(f"Error fetching data: {response.status_code}")
#         return []

# # Fetch and format data from the API
# brewery_data = fetch_brewery_data()

# # Convert the formatted data into a DataFrame with the specified schema
# brewery_df = spark.createDataFrame(brewery_data, schema=brewery_schema)

# # Show the DataFrame
# brewery_df.show(truncate=False)

# # Write the DataFrame to Azure Blob Storage as CSV
# # file_path = f"wasbs://{container_name}@{account_name}.blob.core.windows.net/temp_output2.parquet"
# # brewery_df.coalesce(1).write \
# #     .mode('append') \
# #     .parquet(file_path) \
    
# spark.sparkContext._jsc.hadoopConfiguration().set(
#     f"fs.azure.account.key.{account_name}.blob.core.windows.net", storage_account_key
# )

# # Delete the folder in Azure Blob Storage
# def delete_folder(path: str):
#     # Initialize the Hadoop filesystem
#     fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
#         spark._jsc.hadoopConfiguration()
#     )
    
#     # Convert the folder path to a Hadoop Path
#     folder_path = spark._jvm.org.apache.hadoop.fs.Path(path)
#     fs.delete(folder_path, True) 
#     # Make sure the path starts with 'wasbs://' scheme for Azure
#     if path.startswith('wasbs://'):
#         # Delete the folder if it exists
#         if fs.exists(folder_path):
#             # Use the 'delete' method to delete the folder recursively
#             fs.delete(folder_path, True)  # 'True' for recursive delete
#             print(f"Folder '{path}' deleted successfully.")
#         else:
#             print(f"Folder '{path}' does not exist.")
#     else:
#         print("The provided path is not an Azure Blob Storage path.")

# folder_to_delete = f"wasbs:///{container_name}@{account_name}.blob.core.windows.net/temp_output2.parquet"
# # Call the function to delete the specified folder
# delete_folder(folder_to_delete)

from azure.storage.blob import BlobServiceClient, ContainerClient
import os

# Azure Blob Storage account details
account_name = "blobinbevtestbr"
account_key = "S5Tfc1aNUgfAshhi0H9dXx3EPGRdcquLbhPjonlrJ93NullOdkFl9C+Xt7tBDEcQK54/UzC51sjX+AStgv88ZA=="
container_name = "bronze-inbev-test"
folder_to_delete = "breweries"  # The folder/blob to delete

# Create a BlobServiceClient to interact with the Blob storage
connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get the container client
container_client = blob_service_client.get_container_client(container_name)

def delete_folder_from_blob_storage(folder_name):
    """
    Deletes all files (blobs) within a specified folder in the blob storage container.

    :param folder_name: The name of the folder to delete files from.
    """
    # List blobs in the specified folder
    blobs_to_delete = container_client.list_blobs(name_starts_with=folder_name)
    
    # Collect blobs in a list and sort them in descending order to delete nested blobs first
    blobs_list = sorted([blob.name for blob in blobs_to_delete], reverse=True)
    
    # Delete each blob
    for blob_name in blobs_list:
        print(f"Deleting blob: {blob_name}")
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.delete_blob()

    print(f"All blobs in folder '{folder_name}' have been deleted.")

# Call the function to delete the folder
delete_folder_from_blob_storage(folder_to_delete)


# Stop the Spark session
spark.stop()
