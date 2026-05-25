from lib.configReader import get_app_config
from lib.schemas import customer_schema, loan_raw_schema, raw_loans_repay_schema, loan_defaulters_schema

def _get_read_format(file_path: str) -> str:
    """Helper to dynamically adapt ingestion formats based on extension."""
    if "parquet" in file_path.lower():
        return "parquet"
    return "csv"

def read_customers(spark, env):
    conf = get_app_config(env)
    customer_file_path = conf["customer.obj.path"]
    file_format = _get_read_format(customer_file_path)
    
    reader = spark.read.format(file_format)
    if file_format == "csv":
        reader = reader.option("header", "true")
    return reader.schema(customer_schema).load(customer_file_path)

def read_loans(spark, env):
    conf = get_app_config(env)
    loan_file_path = conf["loan.obj.path"]
    file_format = _get_read_format(loan_file_path)

    reader = spark.read.format(file_format)
    if file_format == "csv":
        reader = reader.option("header", "true")

    raw_df = reader.schema(loan_raw_schema).load(loan_file_path)
    return raw_df.withColumnRenamed("loan_amnt", "loan_amount") \
                 .withColumnRenamed("funded_amnt", "funded_amount") \
                 .withColumnRenamed("term", "loan_term_months") \
                 .withColumnRenamed("int_rate", "interest_rate") \
                 .withColumnRenamed("installment", "monthly_installment") \
                 .withColumnRenamed("issue_d", "issue_date") \
                 .withColumnRenamed("purpose", "loan_purpose") \
                 .withColumnRenamed("title", "loan_title")

def read_loans_repayments(spark, env):
    conf = get_app_config(env)
    loans_repayments_file_path = conf["loanrepayments.obj.path"]
    file_format = _get_read_format(loans_repayments_file_path)
    
    reader = spark.read.format(file_format)
    if file_format == "csv":
        reader = reader.option("header", "true")
        
    loans_repayments_df = reader.schema(raw_loans_repay_schema).load(loans_repayments_file_path)
    return loans_repayments_df.withColumnRenamed("total_rec_prncp", "total_principal_received") \
                               .withColumnRenamed("total_rec_int", "total_interest_received") \
                               .withColumnRenamed("total_rec_late_fee", "total_late_fee_received") \
                               .withColumnRenamed("total_pymnt", "total_payment_received") \
                               .withColumnRenamed("last_pymnt_amnt", "last_payment_amount") \
                               .withColumnRenamed("last_pymnt_d", "last_payment_date") \
                               .withColumnRenamed("next_pymnt_d", "next_payment_date")

def read_defaulters(spark, env):
    conf = get_app_config(env)
    defaulters_file_path = conf["defaulters.obj.path"]
    file_format = _get_read_format(defaulters_file_path)
    
    reader = spark.read.format(file_format)
    if file_format == "csv":
        reader = reader.option("header", "true")
    return reader.schema(loan_defaulters_schema).load(defaulters_file_path)

def read_external_bad_members(spark, env):
    """Ingests cross-layer bad members list using paths from the configuration."""
    conf = get_app_config(env)
    bad_members_path = conf["consolidated.reject.path"]
    file_format = _get_read_format(bad_members_path)
    
    reader = spark.read.format(file_format)
    if file_format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")
    return reader.load(bad_members_path)