import sys
from lib import dataManipulation, dataReader, utils, ConfigReader
from logger import Log4j

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: spark-submit application_main.py <ENV>")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    
    # Load App Configs
    app_conf = ConfigReader.get_app_config(job_run_env)
    
    # Initialize Spark
    spark = utils.get_spark_session(job_run_env)
    logger = Log4j(spark)
    
    logger.info(f"Starting LoanBridge in {job_run_env} mode")

    # READ (Paths from config)
    logger.info("Reading raw customer data from HDFS...")
    raw_customers_df = dataReader.read_customers(spark, job_run_env)

    # TRANSFORM
    logger.info("Applying data cleaning transformations...")
    cleaned_customers_df = dataManipulation.clean_customer_data(raw_customers_df)

    # WRITE (Optimized for Small Files)
    output_path = app_conf["customer.output.path"]
    logger.info(f"Writing to {output_path}")
    
    # Using coalesce(1) because customer data is a small dimension
    cleaned_customers_df.coalesce(1).write \
        .format("parquet") \
        .mode("overwrite") \
        .save(output_path)

    logger.info("Job successfully completed.")