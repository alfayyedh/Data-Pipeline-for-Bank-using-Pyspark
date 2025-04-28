from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, year, round, to_date, date_format, from_unixtime
from datetime import datetime
from helper.utils import load_log_msg


def transform_transactions(spark, df):
    """
    This function is used to transform the transactions data.

    Args:
        spark (_type_): spark session object
        df (_type_): transactions dataframe object
    """

    # Set up current timestamp for logging
    current_timestamp = datetime.now()

    try:

        # Rename selected columns
        rename_columns = {
            "TransactionID" : "transaction_id",
            "CustomerID" : "customer_id",
            "TransactionDate" : "transaction_date",
            "TransactionTime" : "transaction_time",
            "TransactionAmount (INR)" : "transaction_amount"
        }
        
        df = df.withColumnsRenamed(rename_columns)

        # Convert transaction_date column value to DATE format
        df = df.withColumn("transaction_date", 
                           to_date(col("transaction_date"), "d/M/yy")
                          )

        # Give flag if there's a year that > 2025
        df = df.withColumn("transaction_date", 
                           when(year(col("transaction_date")) > 2025, lit(None))
                           .otherwise(col("transaction_date"))
                          )

        # Convert transaction_time column value to HH:MM:SS format
        df = df.withColumn("transaction_time",
                           from_unixtime(col("transaction_time").cast("int"), "HH:mm:ss")
                          )

        # Cast transaction_amount column to float
        df = df.withColumn("transaction_amount", round(df["transaction_amount"].cast("float")))
                          
        # Arrange columns
        df = df.select(
            'transaction_id', 
            'customer_id', 
            'transaction_date', 
            'transaction_time', 
            'transaction_amount' 
        )
        
        print("Transformation process successful for table: transactions")

        # Set success log message
        log_message = spark.sparkContext \
            .parallelize([("sources", "transformation", "success", "csv", "transactions", current_timestamp)]) \
            .toDF(["step", "process", "status", "source", "table_name", "etl_date"])
    
        return df

    except Exception as e:
        print(f"Transformation process failed: {e}")

        # Set failed log message
        log_message = spark.sparkContext \
            .parallelize([("sources", "transformation", "failed", "csv", "transactions", current_timestamp)]) \
            .toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])

    finally:
        load_log_msg(spark=spark, log_msg=log_message)  