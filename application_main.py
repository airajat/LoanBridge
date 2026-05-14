import sys
from lib import dataManipulation, dataReader, utils, configReader
from logger import Log4j

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please provide the environment (LOCAL, DEV, or PROD)")
        sys.exit(-1)

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
    total_cust = raw_customers_df.count()
    
    cleaned_customers_df, rejected_customers_df = dataManipulation.clean_customer_data(raw_customers_df)
    success_cust = cleaned_customers_df.count()
    bad_cust = rejected_customers_df.count()

    cleaned_customers_df.coalesce(1).write.mode("overwrite").parquet(app_conf["customer.output.path"])
    rejected_customers_df.coalesce(1).write.mode("overwrite").parquet(app_conf["customer.reject.path"])

    # ==========================================
    # PROCESS LOANS
    # ==========================================
    logger.info("Processing Loan Data...")
    raw_loans_df = dataReader.read_loans(spark, job_run_env)
    total_loans = raw_loans_df.count()
    
    cleaned_loans_df, rejected_loans_df = dataManipulation.clean_loans_data(raw_loans_df)
    success_loans = cleaned_loans_df.count()
    bad_loans = rejected_loans_df.count()

    cleaned_loans_df.write.mode("overwrite").parquet(app_conf["loan.output.path"])
    rejected_loans_df.coalesce(1).write.mode("overwrite").parquet(app_conf["loan.reject.path"])

    # ==========================================
    # PROCESS REPAYMENTS
    # ==========================================
    logger.info("Processing Loan Repayments Data...")
    raw_repay_df = dataReader.read_loans_repayments(spark, job_run_env)
    total_repay = raw_repay_df.count()
    
    cleaned_repay_df, rejected_repay_df = dataManipulation.clean_repayments_data(raw_repay_df)
    success_repay = cleaned_repay_df.count()
    bad_repay = rejected_repay_df.count()

    cleaned_repay_df.write.mode("overwrite").parquet(app_conf["loanrepayments.output.path"])
    rejected_repay_df.coalesce(1).write.mode("overwrite").parquet(app_conf["loanrepayments.reject.path"])


# ==========================================
    # PROCESS DEFAULTERS (SPLIT-FLOW)
    # ==========================================
    logger.info("Processing Loan Defaulters Data...")
    raw_def_df = dataReader.read_defaulters(spark, job_run_env)
    total_def = raw_def_df.count()

    # Split into Delinquency and Public Records/Enquiry streams
    delinq_df, records_enq_df = dataManipulation.clean_defaulters_data(raw_def_df)
    
    count_delinq = delinq_df.count()
    count_enq = records_enq_df.count()

    delinq_df.write.mode("overwrite").parquet(app_conf["defaulters.delinq.output.path"])
    records_enq_df.write.mode("overwrite").parquet(app_conf["defaulters.enq.output.path"])

    # ==========================================
    # FINAL RECONCILIATION SUMMARY
    # ==========================================
    logger.info("**********************************************")
    logger.info("FINAL RECONCILIATION SUMMARY")
    logger.info(f"CUSTOMERS:  Total={total_cust} | Success={success_cust} | Rejected={bad_cust}")
    logger.info(f"LOANS:      Total={total_loans} | Success={success_loans} | Rejected={bad_loans}")
    logger.info(f"REPAYMENTS: Total={total_repay} | Success={success_repay} | Rejected={bad_repay}")
    logger.info(f"DEFAULTERS: Total={total_def} | Delinq={count_delinq} | Rec/Enq={count_enq}")
    
    # Audit for Data Integrity
    if (success_cust + bad_cust) != total_cust:
        logger.warn("WARNING: Customer count mismatch!")
    if (success_loans + bad_loans) != total_loans:
        logger.warn("WARNING: Loan count mismatch!")
    if (success_repay + bad_repay) != total_repay:
        logger.warn("WARNING: Repayments count mismatch!")
    
    # Note: Defaulters use business filters, so Total != (Delinq + Rec/Enq)
    logger.info("**********************************************")
    logger.info("LoanBridge Job successfully completed.")