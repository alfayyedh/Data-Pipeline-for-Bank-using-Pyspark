import pyspark
from pyspark.sql import SparkSession
from extract.extract import extract_database, extract_csv
from load.load import load_to_dwh
from transform.customers import transform_customers
from transform.transactions import transform_transactions
from transform.marketing_campaign_deposit import transform_marketing_campaign


if __name__ == "__main__":
    
    # Create SparkSession
    spark = SparkSession \
            .builder \
            .appName("ETL Pipeline to DWH") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
    
    
    # Extract data 
    education_status_df = extract_database(spark=spark, table_name="education_status")
    marital_status_df = extract_database(spark=spark, table_name="marital_status")
    marketing_campaign_deposit_df = extract_database(spark=spark, table_name="marketing_campaign_deposit")
    bank_df = extract_csv(spark=spark, file_name="new_bank_transaction.csv")
    
    
    # Transform data
    transformed_customers_df = transform_customers(spark=spark, df=bank_df)
    transformed_transactions_df = transform_transactions(spark=spark, df=bank_df)
    transformed_marketing_df = transform_marketing_campaign(spark=spark, df=marketing_campaign_deposit_df)
    
    
    # Load data
    load_to_dwh(spark=spark, df=transformed_customers_df, table_name="customers", source_name="source_db")
    load_to_dwh(spark=spark, df=transformed_transactions_df, table_name="transactions", source_name="source_db")
    load_to_dwh(spark=spark, df=education_status_df, table_name="education_status", source_name="source_db")
    load_to_dwh(spark=spark, df=marital_status_df, table_name="marital_status", source_name="source_db")
    load_to_dwh(spark=spark, df=transformed_marketing_df, table_name="marketing_campaign_deposit", source_name="source_db")