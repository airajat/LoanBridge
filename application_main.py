import sys
import logging
from lib import dataManipulation, dataReader, goldProcessing, utils, configReader
from logger import Log4j

# Ensure logging configuration is resolved
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please provide the environment (LOCAL, DEV, or PROD) [Optional: DAILY/WEEKLY]")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    job_schedule_type = sys.argv[2].upper() if len(sys.argv) > 2 else "DAILY"
    
    app_conf = configReader.get_app_config(job_run_env)
    spark = utils.get_spark_session(job_run_env)
    logger = Log4j(spark)
    
    logger.info(f"Starting LoanBridge in {job_run_env} mode with {job_schedule_type} schedule")
    db_name = app_conf["hive.database"]

    # ==========================================
    # PROCESS CUSTOMERS (Silver Layer)
    # ==========================================
    logger.info("MILESTONE: Starting Customer Data Read Block...")
    raw_customers_df = dataReader.read_customers(spark, job_run_env)
    
    logger.info("MILESTONE: Triggering Customer Input Count Action...")
    total_cust = raw_customers_df.count()
    
    logger.info("MILESTONE: Executing Customer Clean and Partition Transformations...")
    cleaned_customers_df, rejected_customers_df = dataManipulation.clean_customer_data(raw_customers_df)
    
    logger.info("MILESTONE: Triggering Customer Output Quality Counts...")
    success_cust = cleaned_customers_df.count()
    bad_cust = rejected_customers_df.count()

    logger.info("MILESTONE: Writing Customer Parquet Directories to HDFS...")
    cleaned_customers_df.write.mode("overwrite").parquet(app_conf["customer.output.path"])
    rejected_customers_df.write.mode("overwrite").parquet(app_conf["customer.reject.path"])
    
    logger.info("MILESTONE: Registering Customer Schema inside Hive Metastore Database...")
    cleaned_customers_df.write.mode("overwrite").saveAsTable(f"{db_name}.customers")
    logger.info("MILESTONE: Customer Data Processing Block Completed Successfully.")

    # ==========================================
    # PROCESS LOANS (Silver Layer)
    # ==========================================
    logger.info("MILESTONE: Entering Loan Data Processing Block...")
    
    logger.info("MILESTONE: Ingesting Raw Loan Files from DataReader...")
    raw_loans_df = dataReader.read_loans(spark, job_run_env)
    
    logger.info("MILESTONE: Triggering Eager Action: Counting Raw Loans...")
    total_loans = raw_loans_df.count()
    
    logger.info("MILESTONE: Executing Loan Filtering & Data Type casting...")
    cleaned_loans_df, rejected_loans_df = dataManipulation.clean_loans_data(raw_loans_df)
    
    logger.info("MILESTONE: Triggering Loan Output Quality Counts...")
    success_loans = cleaned_loans_df.count()
    bad_loans = rejected_loans_df.count()

    logger.info("MILESTONE: Writing Loan Parquet Directories to HDFS...")
    cleaned_loans_df.write.mode("overwrite").parquet(app_conf["loan.output.path"])
    rejected_loans_df.write.mode("overwrite").parquet(app_conf["loan.reject.path"])
    
    logger.info("MILESTONE: Registering Loan Schema inside Hive Metastore Database...")
    cleaned_loans_df.write.mode("overwrite").saveAsTable(f"{db_name}.loans")
    logger.info("MILESTONE: Loan Data Processing Block Completed Successfully.")

    # ==========================================
    # PROCESS REPAYMENTS (Silver Layer)
    # ==========================================
    logger.info("MILESTONE: Entering Loan Repayments Data Processing Block...")
    raw_repay_df = dataReader.read_loans_repayments(spark, job_run_env)
    
    logger.info("MILESTONE: Triggering Eager Action: Counting Raw Repayments...")
    total_repay = raw_repay_df.count()
    
    logger.info("MILESTONE: Executing Repayment Financial Balancing Calculations...")
    cleaned_repay_df, rejected_repay_df = dataManipulation.clean_repayments_data(raw_repay_df)
    
    logger.info("MILESTONE: Triggering Repayments Output Quality Counts...")
    success_repay = cleaned_repay_df.count()
    bad_repay = rejected_repay_df.count()

    logger.info("MILESTONE: Writing Repayment Parquet Directories to HDFS...")
    cleaned_repay_df.write.mode("overwrite").parquet(app_conf["loanrepayments.output.path"])
    rejected_repay_df.write.mode("overwrite").parquet(app_conf["loanrepayments.reject.path"])
    
    logger.info("MILESTONE: Registering Repayments Schema inside Hive Metastore Database...")
    cleaned_repay_df.write.mode("overwrite").saveAsTable(f"{db_name}.loans_repayments")
    logger.info("MILESTONE: Repayments Data Processing Block Completed Successfully.")

    # ==========================================
    # PROCESS DEFAULTERS (Silver Layer Split-Flow)
    # ==========================================
    logger.info("MILESTONE: Entering Defaulters Data Processing Block...")
    raw_def_df = dataReader.read_defaulters(spark, job_run_env)
    
    logger.info("MILESTONE: Triggering Eager Action: Counting Raw Defaulters...")
    total_def = raw_def_df.count()

    logger.info("MILESTONE: Bifurcating Defaulters Stream into Delinquencies and Inquiries...")
    delinq_df, records_enq_df = dataManipulation.clean_defaulters_data(raw_def_df)
    
    logger.info("MILESTONE: Triggering Split Stream Output Quality Counts...")
    count_delinq = delinq_df.count()
    count_enq = records_enq_df.count()

    logger.info("MILESTONE: Writing Split Streams to HDFS Parquet... ")
    delinq_df.write.mode("overwrite").parquet(app_conf["defaulters.delinq.output.path"])
    records_enq_df.write.mode("overwrite").parquet(app_conf["defaulters.enq.output.path"])
    
    logger.info("MILESTONE: Registering Dual Defaulter Tables inside Hive Metastore Database...")
    delinq_df.write.mode("overwrite").saveAsTable(f"{db_name}.loans_defaulters_delinq")
    records_enq_df.write.mode("overwrite").saveAsTable(f"{db_name}.loans_defaulters_detail_rec_enq")
    logger.info("MILESTONE: Defaulters Data Processing Block Completed Successfully.")

    # ===================================================================================
    # ORCHESTRATE GOLD LAYER RISK PLATFORM
    # ===================================================================================
    logger.info("MILESTONE: Orchestrating Gold Layer Governance Platform...")

    logger.info("MILESTONE: Generating Consolidated Bad Members Data Assets...")
    consolidated_bad_df = dataManipulation.generate_consolidated_bad_members(spark, db_name)
    consolidated_bad_df.write.mode("overwrite").format("parquet").option("path", app_conf["consolidated.reject.path"]).saveAsTable(f"{db_name}.consolidated_bad_members")

    logger.info("MILESTONE: Externalising Mapping Views for Bad Member Processing...")
    bad_members_df = dataReader.read_external_bad_members(spark, job_run_env)
    bad_members_df.write.mode("overwrite").saveAsTable(f"{db_name}.view_internal_bad_members")

    logger.info("MILESTONE: Compiling Business Logic Views into Hive Context MetaCatalog...")
    goldProcessing.deploy_realtime_scoring_view(spark, db_name, app_conf)

    if job_schedule_type == "WEEKLY":
        logger.info("MILESTONE: Weekly Schedule Confirmed. Constructing Historical Scoring Matrix...")
        loan_score_final = goldProcessing.calculate_weekly_loan_scores(spark, db_name, app_conf)
        
        logger.info("MILESTONE: Materialising Gold Scoring Matrix to HDFS & Hive Metastore...")
        loan_score_final.write \
            .mode("overwrite") \
            .format("parquet") \
            .option("path", app_conf["gold.weekly.output.path"]) \
            .saveAsTable(f"{db_name}.gold_loan_scores_weekly")
            
        logger.info("MILESTONE: Gold Scoring Layer Materialisation Completed Successfully.")
    else:
        logger.info("Daily processing run active. Skipping heavy physical table calculations.")

    # ==========================================
    # FINAL RECONCILIATION SUMMARY
    # ==========================================
    logger.info("**********************************************")
    logger.info("FINAL RECONCILIATION SUMMARY")
    logger.info(f"CUSTOMERS:  Total={total_cust} | Success={success_cust} | Rejected={bad_cust}")
    logger.info(f"LOANS:      Total={total_loans} | Success={success_loans} | Rejected={bad_loans}")
    logger.info(f"REPAYMENTS: Total={total_repay} | Success={success_repay} | Rejected={bad_repay}")
    logger.info(f"DEFAULTERS: Total={total_def} | Delinq={count_delinq} | Rec/Enq={count_enq}")
    
    if (success_cust + bad_cust) != total_cust:
        logger.warn("WARNING: Customer count mismatch!")
    if (success_loans + bad_loans) != total_loans:
        logger.warn("WARNING: Loan count mismatch!")
    if (success_repay + bad_repay) != total_repay:
        logger.warn("WARNING: Repayments count mismatch!")
    
    logger.info("**********************************************")
    logger.info("LoanBridge Job successfully completed.")