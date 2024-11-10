from azure.storage.blob import BlobServiceClient
import os

def upload_files(directory_path, account_name, storage_account_key, container_name, folder_in_container, prefix="part"):
    """
    Uploads all files from a specified directory and its first-level subdirectories
    that start with a given prefix (default is "part") to a specified folder within Azure Blob Storage.
    
    Parameters:
    - directory_path (str): Path to the local directory containing files to upload.
    - account_name (str): Azure Blob Storage account name.
    - storage_account_key (str): Azure Blob Storage account key.
    - container_name (str): Name of the Azure Blob Storage container.
    - folder_in_container (str): Folder in the container where files will be uploaded.
    - prefix (str): Prefix that file names should start with. Default is "part".
    """
    
    # Build the connection string
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
    
    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    # Get or create the container
    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        container_client.create_container()
    
    # Traverse the directory path up to a depth of 2
    for root, _, files in os.walk(directory_path):
        # Check if we are in depth 2
        if root[len(directory_path):].count(os.sep) < 2:
            for file_name in files:
                if file_name.startswith(prefix):
                    file_path = os.path.join(root, file_name)
                    
                    # Define the blob path with the specified folder and relative path
                    relative_path = os.path.relpath(file_path, directory_path)
                    blob_path = f"{folder_in_container}/{relative_path}"
                    
                    # # Create a BlobClient for each file
                    blob_client = container_client.get_blob_client(blob_path)
                    
                    # Upload the file
                    with open(file_path, "rb") as data:
                        blob_client.upload_blob(data, overwrite=True)
                    
                    print(f"Uploaded '{file_path}' to '{blob_path}' in Azure Blob Storage.")

# Usage example:
# upload_files(
#     directory_path="/opt/airflow/dags/temp/breweries",
#     account_name="blobinbevtestbr",
#     storage_account_key="S5Tfc1aNUgfAshhi0H9dXx3EPGRdcquLbhPjonlrJ93NullOdkFl9C+Xt7tBDEcQK54/UzC51sjX+AStgv88ZA==",
#     container_name="bronze-inbev-test",
#     folder_in_container="date=2024-11-08"  # Specify the folder here
# )
