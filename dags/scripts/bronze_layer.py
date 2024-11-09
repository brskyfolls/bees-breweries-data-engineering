import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os
from upload_script import upload_files
from delete_script import delete_folder

# Fetch data from the Open Brewery DB API
def fetch_brewery_data():
    url = "https://api.openbrewerydb.org/breweries"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
        # Convert longitude and latitude fields to float, handling invalid entries
        for entry in data:
            entry['longitude'] = float(entry['longitude']) if entry.get('longitude') else None
            entry['latitude'] = float(entry['latitude']) if entry.get('latitude') else None
        
        return data
    else:
        print(f"Error fetching data: {response.status_code}")
        return []

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Brewery Data Pipeline") \
    .master("local[*]") \
    .getOrCreate()

brewery_schema = StructType([
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

# Fetch and format data
brewery_data = fetch_brewery_data()

brewery_df = spark.createDataFrame(brewery_data, schema=brewery_schema)

# Show the DataFrame
brewery_df.show(truncate=False)

# Define local path for saving the output
local_output_path = "file:////opt/airflow/dags/temp/breweries"

# Ensure the output directory exists
os.makedirs("/opt/airflow/dags/temp/breweries", exist_ok=True)

# Write the DataFrame to the local directory
brewery_df.coalesce(1).write.mode("append").parquet(local_output_path)

upload_files(
    directory_path="/opt/airflow/dags/temp/breweries",
    account_name="blobinbevtestbr",
    storage_account_key="S5Tfc1aNUgfAshhi0H9dXx3EPGRdcquLbhPjonlrJ93NullOdkFl9C+Xt7tBDEcQK54/UzC51sjX+AStgv88ZA==",
    container_name="bronze-inbev-test",
    folder_in_container="openbrewerydb/date=2024-11-08"  # Specify the folder here
)

delete_folder('/opt/airflow/dags/temp/breweries')

# Stop the Spark session
spark.stop()
