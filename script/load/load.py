from pyspark.sql import SparkSession
from datetime import datetime
from sqlalchemy import text
from helper.utils import dwh_engine_sqlalchemy, dwh_engine, load_log_msg


def load_to_dwh(spark, df, table_name, source_name):
    """
    This function loads transformed DataFrame to data warehouse.

    Args:
        spark (_type_): spark session object.
        df (_type_): dataframe to be loaded to data warehouse.
        table_name (_type_): table name to load data to.
        source_name (_type_): source name of the data.
    """

    # Set current timestamp for logging
    current_timestamp = datetime.now()

    try:

        # Establish connection to warehouse db
        conn = dwh_engine_sqlalchemy()

        # Create connection
        with conn.begin() as connection:

            # Truncate all tables in data warehouse
            connection.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))

        print(f"Success truncating table: {table_name}")

    except Exception as e:
        print(f"Error when truncating table: {e}")

        # Set failed log message
        log_message = spark.sparkContext\
            .parallelize([("warehouse", "load", "failed", source_name, table_name, current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])
        
        # Log failed message to log database
        load_log_msg(spark=spark, log_msg=log_message) 

    finally:
        conn.dispose()

    # Load transformed DataFrame to warehouse db
    try:
        
        # Get dwh db config
        DWH_DB_URL, DWH_DB_USER, DWH_DB_PASS = dwh_engine()
    
        # Set config
        properties = {
            "user" : DWH_DB_USER,
            "password" : DWH_DB_PASS,
        }

        # Write data to warehouse db
        df.write.jdbc(url=DWH_DB_URL,
                      table=table_name,
                      mode="append",
                      properties=properties)

        print(f"Load process successful for table: {table_name}")

        # Set success log message
        log_message = spark.sparkContext\
            .parallelize([("warehouse", "load", "success", source_name, table_name, current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])
        
        # Log success message to log database
        load_log_msg(spark=spark, log_msg=log_message) 
        
    except Exception as e:
        print(f"Load process failed: {e}")

        # Set failed log message
        log_message = spark.sparkContext\
            .parallelize([("warehouse", "load", "failed", source_name, table_name, current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])
        
    finally:
        
        # Log failed message to log database
        load_log_msg(spark=spark, log_msg=log_message) 