from lib.configReader import get_app_config
from lib.schemas import customer_schema, loan_raw_schema

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

    # Uses the raw schema with 'loan_amnt', 'term', etc.
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