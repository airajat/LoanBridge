import sys
from lib import dataManipulation, dataReader, utils
from logger import Log4j

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment (LOCAL, DEV, or PROD)")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    
    # 1. Spark Session & Logging
    spark = utils.get_spark_session(job_run_env)
    logger = Log4j(spark)
    logger.info(f"Starting LoanBridge Customer Cleaning in {job_run_env}")

    # 2. Extract
    logger.info("Reading raw customer data from HDFS...")
    raw_customers_df = dataReader.read_customers(spark, job_run_env)

    # 3. Transform
    logger.info("Applying data cleaning transformations...")
    cleaned_customers_df = dataManipulation.clean_customer_data(raw_customers_df)

    # 4. Load
    logger.info("Writing cleaned data to Silver zone in Parquet format...")
    # You would typically pull this path from ConfigReader as well
    cleaned_customers_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(f"loanbridge/{job_run_env}/silver/customers")

    logger.info("Job successfully completed.")