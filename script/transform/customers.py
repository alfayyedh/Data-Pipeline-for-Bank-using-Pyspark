from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, when, lit, to_date, date_format, year, round
from helper.utils import load_log_msg

def transform_customers(spark, df):
    """
    This function is used to transform the customers table.

    Args:
        spark (_type_): spark session object
        df (_type_): customers dataframe object
    """

    # Set up current timestamp for logging
    current_timestamp = datetime.now()

    try:

        # Rename selected columns
        rename_columns = {
            "CustomerID" : "customer_id",
            "CustomerDOB" : "birth_date",
            "CustGender" : "gender",
            "CustLocation" : "location",
            "CustAccountBalance" : "account_balance"
        }
        
        df = df.withColumnsRenamed(rename_columns)

        # Mapping gender column value
        df = df.withColumn("gender", 
                           when(col("gender") == "M", "Male")
                           .when(col("gender") == "F", "Female")
                           .when(col("gender") == "T", "Other")
                           .otherwise(col("gender"))
                          )

        # Convert to DATE format
        df = df.withColumn("birth_date", 
                           to_date(col("birth_date"), "d/M/yy")
                          )

        # # Give flag if there's a year that > 2025
        df = df.withColumn("birth_date", 
                           when(year(col("birth_date")) > 2025, lit(None))
                           .otherwise(col("birth_date"))
                          )

        # Cast account_balance column to float
        df = df.withColumn("account_balance", round(df["account_balance"].cast("float")))

        # Arrange columns
        df = df.select(
            'customer_id', 
            'birth_date', 
            'gender', 
            'location', 
            'account_balance' 
        )
        
        print("Transformation process successful for table: customers")

        # Set success log message
        log_message = spark.sparkContext \
            .parallelize([("sources", "transformation", "success", "csv", "customers", current_timestamp)]) \
            .toDF(["step", "process", "status", "source", "table_name", "etl_date"])
    
        return df

    except Exception as e:
        print(f"Transformation process failed: {e}")

        # Set failed log message
        log_message = spark.sparkContext \
            .parallelize([("sources", "transformation", "failed", "csv", "customers", current_timestamp)]) \
            .toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])

    finally:
        load_log_msg(spark=spark, log_msg=log_message)
        