from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, floor
from datetime import datetime
from helper.utils import load_log_msg

def transform_marketing_campaign(spark, df):
    """
    This function is used to transform the marketing_campaign_deposit table.

    Args:
        spark (_type_): spark session object
        df (_type_): marketing_campaign_deposit dataframe object
    """

    # Set up current timestamp for logging
    current_timestamp = datetime.now()

    try:

        # Rename selected columns
        rename_columns = {
            "pdays" : "days_since_last_campaign",
            "previous" : "previous_campaign_contacts",
            "poutcome" : "previous_campaign_outcome"
        }
        
        df = df.withColumnsRenamed(rename_columns)

        # Remove dollar ($) sign in balance column
        df = df.withColumn('balance', regexp_replace(df['balance'], r"\$", " "))

        # Then, cast balance column to integer
        df = df.withColumn('balance', df['balance'].cast('int'))

        # Create new column duration_in_year
        # Divide value in duration column with 365, then round down and convert to integer
        df = df.withColumn('duration_in_year', floor(df['duration'] / 365).cast('int'))

        # Arrange columns
        df = df.select(
            'loan_data_id', 
            'age', 
            'job', 
            'marital_id', 
            'education_id', 
            'default', 
            'balance', 
            'housing',
            'loan',
            'contact',
            'day',
            'month',
            'duration',
            'duration_in_year',
            'campaign',
            'days_since_last_campaign',
            'previous_campaign_contacts',
            'previous_campaign_outcome',
            'subscribed_deposit',
            'created_at',
            'updated_at'
        )

        print("Transformation process successful for table: marketing_campaign_deposit")

        # Set success log message
        log_message = spark.sparkContext \
            .parallelize([("sources", "transformation", "success", "source_db", "marketing_campaign_deposit", current_timestamp)]) \
            .toDF(["step", "process", "status", "source", "table_name", "etl_date"])
    
        return df

    except Exception as e:
        print(f"Transformation process failed: {e}")

        # Set failed log message
        log_message = spark.sparkContext \
            .parallelize([("sources", "transformation", "failed", "source_db", "marketing_campaign_deposit", current_timestamp)]) \
            .toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])

    finally:
        load_log_msg(spark=spark, log_msg=log_message)