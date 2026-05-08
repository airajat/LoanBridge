import sys
from lib import dataManipulation, dataReader, utils, configReader
from logger import Log4j

if __name__ == '__main__':
    job_run_env = sys.argv[1].upper()
    app_conf = configReader.get_app_config(job_run_env)
    spark = utils.get_spark_session(job_run_env)
    logger = Log4j(spark)
    
    logger.info(f"Starting LoanBridge in {job_run_env} mode")

    # ==========================================
    # PROCESS CUSTOMERS
    # ==========================================
    logger.info("Processing Customer Data...")
    raw_customers_df = dataReader.read_customers(spark, job_run_env)
    
    # Audit: Initial Count
    total_cust = raw_customers_df.count()
    
    cleaned_customers_df, rejected_customers_df = dataManipulation.clean_customer_data(raw_customers_df)
    
    # Audit: Final Counts
    success_cust = cleaned_customers_df.count()
    bad_cust = rejected_customers_df.count()

    cleaned_customers_df.coalesce(1).write.mode("overwrite").parquet(app_conf["customer.output.path"])
    rejected_customers_df.coalesce(1).write.mode("overwrite").parquet(app_conf["customer.reject.path"])

    # ==========================================
    # PROCESS LOANS
    # ==========================================
    logger.info("Processing Loan Data...")
    raw_loans_df = dataReader.read_loans(spark, job_run_env)
    
    # Audit: Initial Count
    total_loans = raw_loans_df.count()
    
    cleaned_loans_df, rejected_loans_df = dataManipulation.clean_loans_data(raw_loans_df)
    
    # Audit: Final Counts
    success_loans = cleaned_loans_df.count()
    bad_loans = rejected_loans_df.count()

    cleaned_loans_df.write.mode("overwrite").parquet(app_conf["loan.output.path"])
    rejected_loans_df.coalesce(1).write.mode("overwrite").parquet(app_conf["loan.reject.path"])

    # ==========================================
    # FINAL RECONCILIATION SUMMARY
    # ==========================================
    logger.info("**********************************************")
    logger.info("FINAL RECONCILIATION SUMMARY")
    logger.info(f"CUSTOMERS: Total={total_cust} | Success={success_cust} | Rejected={bad_cust}")
    logger.info(f"LOANS:     Total={total_loans} | Success={success_loans} | Rejected={bad_loans}")
    
    if (success_cust + bad_cust) != total_cust:
        logger.warn("WARNING: Customer count mismatch! Check for unintended drops.")
    
    logger.info("**********************************************")
    logger.info("LoanBridge Job successfully completed.")