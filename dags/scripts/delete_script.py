import shutil
import os
from azure.storage.blob import BlobServiceClient

def delete_folder(folder_path):
    """Deletes a folder and all its contents."""
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        try:
            shutil.rmtree(folder_path)  # Recursively deletes the folder and its contents
            print(f"Folder '{folder_path}' and its contents have been deleted.")
        except Exception as e:
            print(f"Error deleting folder {folder_path}: {e}")
    else:
        print(f"Folder '{folder_path}' does not exist or is not a directory.")

def delete_folder_from_blob_storage(account_name, account_key, container_name, folder_name):
    """
    Deletes all files (blobs) within a specified folder in an Azure Blob Storage container.

    :param account_name: Azure Blob Storage account name.
    :param account_key: Azure Blob Storage account key.
    :param container_name: The name of the container.
    :param folder_name: The name of the folder to delete files from.
    """
    # Create the connection string
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"

    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get the container client
    container_client = blob_service_client.get_container_client(container_name)

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

# Example usage
# delete_folder('/opt/airflow/dags/temp/breweries')
# delete_folder_from_blob_storage(
#     account_name="blobinbevtestbr",
#     account_key="S5Tfc1aNUgfAshhi0H9dXx3EPGRdcquLbhPjonlrJ93NullOdkFl9C+Xt7tBDEcQK54/UzC51sjX+AStgv88ZA==",
#     container_name="bronze-inbev-test",
#     folder_name="breweries"
# )