from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit  # Correct import for using F as alias
import datetime
import requests
import json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from read_script import read_parquet_from_azure
import os
from upload_script import upload_files
from delete_script import delete_folder


storage_account_name = "blobinbevtestbr"
container_name = "bronze-inbev-test"
folder_path = "openbrewerydb/date=2024-11-08"  # Path to the folder containing the Parquet files
account_key = "S5Tfc1aNUgfAshhi0H9dXx3EPGRdcquLbhPjonlrJ93NullOdkFl9C+Xt7tBDEcQK54/UzC51sjX+AStgv88ZA=="

# Call the function to read data
brewery_df, spark = read_parquet_from_azure(storage_account_name, container_name, folder_path, account_key)

# Clean Null values out, fill with TDB and -1
null_value_map = {
    "string": "TBD",
    "double": -1
}

# Initialize an empty DataFrame with the same schema as the original DataFrame
df_cleaned = brewery_df.select("*")

# Iterate over each column and apply the null value replacement
for col_name in brewery_df.columns:
    data_type = brewery_df.schema[col_name].dataType.typeName()
    
    if data_type in null_value_map:
        null_value = null_value_map[data_type]
        df_cleaned = df_cleaned.withColumn(col_name, 
                                            when(col(col_name).isNull(), lit(null_value))
                                            .otherwise(col(col_name)))

# Select relevant columns for the Silver Layer
df_silver = df_cleaned.select(
    "id", "name", "brewery_type", "address_1", "address_2", "address_3", "city", "state_province", "postal_code", 
    "country", "longitude", "latitude", "phone", "website_url", "state", "street"
)

# Show the cleaned Silver layer data
df_silver.show(truncate=False)

# Ensure the output directory exists
os.makedirs("/opt/airflow/dags/temp/breweries", exist_ok=True)

local_output_path = "/opt/airflow/dags/temp/breweries"
# Write the DataFrame to the local directory
df_silver.coalesce(1).write.mode("append").parquet(local_output_path)

upload_files(
    directory_path="/opt/airflow/dags/temp/breweries",
    account_name="blobinbevtestbr",
    storage_account_key="S5Tfc1aNUgfAshhi0H9dXx3EPGRdcquLbhPjonlrJ93NullOdkFl9C+Xt7tBDEcQK54/UzC51sjX+AStgv88ZA==",
    container_name="silver-inbev-test",
    folder_in_container="openbrewerydb/date=2024-11-08"  # Specify the folder here
)

delete_folder('/opt/airflow/dags/temp/breweries')
