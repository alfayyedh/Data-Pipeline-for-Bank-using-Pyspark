import os
import json
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pyspark.sql.functions import col, count, when, round

# Load .env and define the credentials
load_dotenv('/home/jovyan/work/.env', override=True)

print(f"File .env exists: {os.path.exists('/home/jovyan/work/.env')}")


SOURCE_DB_HOST = os.getenv("SRC_DB_HOST")
SOURCE_DB_NAME = os.getenv("SRC_DB_NAME")
SOURCE_DB_USER = os.getenv("SRC_DB_USER")
SOURCE_DB_PASS = os.getenv("SRC_DB_PASSWORD")
SOURCE_DB_PORT = os.getenv("SRC_DB_PORT")

DWH_DB_HOST = os.getenv("WH_DB_HOST")
DWH_DB_NAME = os.getenv("WH_DB_NAME")
DWH_DB_USER = os.getenv("WH_DB_USER")
DWH_DB_PASS = os.getenv("WH_DB_PASSWORD")
DWH_DB_PORT = os.getenv("WH_DB_PORT")

LOG_DB_HOST = os.getenv("LOG_DB_HOST")
LOG_DB_NAME = os.getenv("LOG_DB_NAME")
LOG_DB_USER = os.getenv("LOG_DB_USER")
LOG_DB_PASS = os.getenv("LOG_DB_PASSWORD")
LOG_DB_PORT = os.getenv("LOG_DB_PORT")

print(f"LOG_DB_URL: {LOG_DB_NAME}")
print(f"LOG_DB_USER: {LOG_DB_USER}")
print(f"LOG_DB_PASS: {LOG_DB_PASS}")
print(f"LOG_DB_PASS: {LOG_DB_PORT}")

print(f"LOG_DB_URL: {SOURCE_DB_NAME}")
print(f"LOG_DB_USER: {SOURCE_DB_USER}")
print(f"LOG_DB_PASS: {SOURCE_DB_PASS}")
print(f"LOG_DB_PASS: {SOURCE_DB_PORT}")

print(f"LOG_DB_URL: {DWH_DB_NAME}")
print(f"LOG_DB_USER: {DWH_DB_USER}")
print(f"LOG_DB_PASS: {DWH_DB_PASS}")
print(f"LOG_DB_PASS: {DWH_DB_PORT}")


def source_engine():
    """
    This function returns the source database engine.
    """
    SOURCE_DB_URL = f"jdbc:postgresql://{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}"
    return SOURCE_DB_URL, SOURCE_DB_USER, SOURCE_DB_PASS 

def dwh_engine():
    """
    This function returns the DWH database engine.
    """
    DWH_DB_URL = f"jdbc:postgresql://{DWH_DB_HOST}:{DWH_DB_PORT}/{DWH_DB_NAME}"
    return DWH_DB_URL, DWH_DB_USER, DWH_DB_PASS 

def dwh_engine_sqlalchemy():
    """
    This function returns the DWH database engine using sqlalchemy.
    """
    return create_engine(f"postgresql://{DWH_DB_USER}:{DWH_DB_PASS}@{DWH_DB_HOST}:{DWH_DB_PORT}/{DWH_DB_NAME}")

def log_engine():
    """
    This function returns the log database engine.
    """
    LOG_DB_URL = f"jdbc:postgresql://{LOG_DB_HOST}:{LOG_DB_PORT}/{LOG_DB_NAME}"
    return LOG_DB_URL, LOG_DB_USER, LOG_DB_PASS 


def load_log_msg(spark: SparkSession, log_msg):
    """
    This function loads the log message to the log database.

    Args:
        spark (SparkSession): spark session object.
        log_msg (_type_): log message in dataframe format.
    """

    # Set log database engine
    LOG_DB_URL, LOG_DB_USER, LOG_DB_PASS = log_engine()
    table_name = "etl_log"

    print(f"LOG_DB_URL: {LOG_DB_URL}")
    print(f"LOG_DB_USER: {LOG_DB_USER}")
    print(f"LOG_DB_PASS: {LOG_DB_PASS}")

    # set config
    connection_properties = {
        "user": LOG_DB_USER,
        "password": LOG_DB_PASS,
        "driver": "org.postgresql.Driver" # set driver postgres
    }

    # Write log message to log database
    log_msg.write.jdbc(url = LOG_DB_URL,
                  table = table_name,
                  mode = "append",
                  properties = connection_properties)



# Create a function to save the final report to a JSON file
def save_to_json(dict_result: dict, filename: str) -> None:
    """
    This function saves the data profiling result to a JSON file.

    Args:
        dict_result (dict): Data profiling result to save to a JSON file.
        filename (str): Name of the JSON file to save the data profiling result to.

    Returns:
        None
    """

    try:
        
        # Save the data profiling result to a JSON file
        with open(f'{filename}.json', 'w') as file:
            file.write(json.dumps(dict_result, indent= 4))
    
    except Exception as e:
        print(f"Error: {e}")



# Check Percentage of Missing Values for each column with pyspark
def check_missing_values(df):
    """
    This function checks the percentage of missing values for each column in a dataframe.

    Args:
        df (_type_): Dataframe to check for missing values.

    Returns:
        int : Percentage of missing values for each column in the dataframe.
    """

    total_data = df.count()

    # Calculate the percentage of missing values for each column
    get_missing_values = df.select([
        round((count(when(col(c).isNull(), c)) / total_data) * 100, 2).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()
    
    return get_missing_values