from lib.configReader import get_app_config
from lib.schemas import customer_schema, loan_raw_schema, raw_loans_repay_schema, loan_defaulters_schema

def read_customers(spark, env):
    conf = get_app_config(env)
    customer_file_path = conf["customer.obj.path"]
    
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(customer_schema) \
        .load(customer_file_path)

def read_loans(spark, env):
    conf = get_app_config(env)
    loan_file_path = conf["loan.obj.path"]

    raw_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(loan_raw_schema) \
        .load(loan_file_path)

    # Standardize names for the Transformation layer
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
    
    loans_repayments_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(raw_loans_repay_schema) \
        .load(loans_repayments_file_path)
    
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
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(loan_defaulters_schema) \
        .load(defaulters_file_path)
