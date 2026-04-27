import sys
from utils import get_spark_session, get_logger
from dataReader import read_customers
from datamanipulation import clean_customers

def main():
    # Initialize Spark and Logger
    spark = get_spark_session("LoanBridge")
    logger = get_logger(spark)

    logger.info("Starting Ingestion...")
    
    # 1. Read (In a real app, these paths come from configReader)
    raw_df = read_customers(spark, "/public/...", "member_id string...")
    
    # 2. Transform
    logger.info("Cleaning Customer Data...")
    cleaned_df = clean_customers(raw_df)
    
    # 3. Write (Always prefer Parquet for production!)
    logger.info("Writing results to Parquet...")
    cleaned_df.write.format("parquet").mode("overwrite").save("loanbridge/silver/customers")

if __name__ == "__main__":
    main()