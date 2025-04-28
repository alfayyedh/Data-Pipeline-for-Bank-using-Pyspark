from pyspark.sql import SparkSession
from datetime import datetime
from helper.utils import source_engine, load_log_msg

def extract_database(spark: SparkSession, table_name: str):
    """
    This function extracts data from a selected table in source database.

    Args:
        spark (SparkSession): spark session object.
        table_name (str): table name to extract data from.

    Returns:
        jdbc dataframe: extracted data from source database in jdbc format.
    """

    # Get source db config
    SOURCE_DB_URL, SOURCE_DB_USER, SOURCE_DB_PASS = source_engine()

    # Set config
    connection_properties = {
        "user" : SOURCE_DB_USER,
        "password" : SOURCE_DB_PASS,
        "driver" : "org.postgresql.Driver"
    }

    # Set current timestamp for logging
    current_timestamp = datetime.now()

    try:

        # Read data
        df = spark \
            .read \
            .jdbc(url=SOURCE_DB_URL,
                  table=table_name,
                  properties=connection_properties)

        print(f"Extraction process successful for table: {table_name}")

        # Set success log message
        log_message = spark.sparkContext \
            .parallelize([("sources", "extraction", "success", "source_db", table_name, current_timestamp)]) \
            .toDF(["step", "process", "status", "source", "table_name", "etl_date"])
    
        return df

    except Exception as e:
        print(f"Extraction process failed: {e}")

        # Set failed log message
        log_message = spark.sparkContext \
            .parallelize([("sources", "extraction", "failed", "source_db", table_name, current_timestamp, str(e))]) \
            .toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])

    finally:
        load_log_msg(spark=spark, log_msg=log_message)


# --------------------------------------------------------------------------------------------------------------------------- #


def extract_csv(spark: SparkSession, file_name: str):
    """
    This function extracts data from a selected csv file.

    Args:
        spark (SparkSession): spark session object.
        file_name (str): csv file name to extract data from.

    Returns:
        jdbc dataframe: extracted data from csv file in jdbc format.
    """

    # Define csv path    
    path = "/home/jovyan/work/data/"

    # Define current timestamp for logging
    current_timestamp = datetime.now()

    try:
        # Read data
        df = spark.read.csv(path + file_name, header=True)
        
        print(f"Extraction process successful for file: {file_name}")

        # Set success log message
        log_message = spark.sparkContext \
            .parallelize([("sources", "extraction", "success", "csv", file_name, current_timestamp)]) \
            .toDF(["step", "process", "status", "source", "table_name", "etl_date"])
    
        return df

    except Exception as e:
        print(f"Extraction process failed: {e}")

        # Set failed log message
        log_message = spark.sparkContext \
            .parallelize([("sources", "extraction", "failed", "csv", file_name, current_timestamp, str(e))]) \
            .toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])

    finally:
        load_log_msg(spark=spark, log_msg=log_message)