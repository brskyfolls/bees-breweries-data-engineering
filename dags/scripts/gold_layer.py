from pyspark.sql import SparkSession
from pyspark.sql.functions import * # Correct import for using F as alias
import datetime
import requests
import json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from read_script import read_parquet_from_azure
import os
from upload_script import upload_files
from delete_script import delete_folder, delete_folder_from_blob_storage


storage_account_name = "blobinbevtestbr"
container_name = "silver-inbev-test"
folder_path = "openbrewerydb/*/"  # Path to the folder containing the Parquet files
account_key = "S5Tfc1aNUgfAshhi0H9dXx3EPGRdcquLbhPjonlrJ93NullOdkFl9C+Xt7tBDEcQK54/UzC51sjX+AStgv88ZA=="

# Call the function to read data
brewery_df, spark = read_parquet_from_azure(storage_account_name, container_name, folder_path, account_key)

###############brewery_type_dim########################
# Extract unique brewery types
brewery_type_dim = brewery_df.select("brewery_type").distinct() \
    .withColumn("brewery_type_id", monotonically_increasing_id())

# Show the cleaned Silver layer data
brewery_type_dim.show(truncate=False)

###############location_dim########################
location_dim = brewery_df.select("city", "state", "state_province", "country", 
                                       "postal_code", "longitude", "latitude").distinct() \
    .withColumn("location_id", monotonically_increasing_id())

location_dim.show()

###############brewery_dim########################
# Join with dim_brewery_type to get brewery_type_id
brewery_dim = brewery_df.alias("brewery") \
    .join(brewery_type_dim.alias("type"), on="brewery_type", how="left") \
    .selectExpr("id as brewery_id", "name", "type.brewery_type_id", 
                "phone", "website_url") \
    .distinct()

brewery_dim.show()

# Define a dictionary with DataFrame names and corresponding DataFrame objects
dataframes = {
    "brewery_type_dim": brewery_type_dim,
    "location_dim": location_dim,
    "brewery_dim": brewery_dim
    # "fact_brewery_visits": fact_brewery_visits
}

# Ensure the output directory exists
os.makedirs("/opt/airflow/dags/temp/breweries", exist_ok=True)

# Loop through the dictionary and save each DataFrame as a Parquet file
for name, df in dataframes.items():
    df.coalesce(1).write.mode("append").parquet(f"/opt/airflow/dags/temp/breweries/{name}")
    delete_folder_from_blob_storage(
    account_name="blobinbevtestbr",
    account_key="S5Tfc1aNUgfAshhi0H9dXx3EPGRdcquLbhPjonlrJ93NullOdkFl9C+Xt7tBDEcQK54/UzC51sjX+AStgv88ZA==",
    container_name="gold-inbev-test",
    folder_name=f"openbrewerydb/{name}"
    )
    upload_files(
        directory_path=f"/opt/airflow/dags/temp/breweries/{name}",
        account_name="blobinbevtestbr",
        storage_account_key="S5Tfc1aNUgfAshhi0H9dXx3EPGRdcquLbhPjonlrJ93NullOdkFl9C+Xt7tBDEcQK54/UzC51sjX+AStgv88ZA==",
        container_name="gold-inbev-test",
        folder_in_container=f"openbrewerydb/{name}"  # Specify the folder here
    )

local_output_path = "/opt/airflow/dags/temp/breweries"

delete_folder('/opt/airflow/dags/temp/breweries')
