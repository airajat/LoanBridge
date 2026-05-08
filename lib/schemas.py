from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# 1. CUSTOMER RAW SCHEMA (Matches CSV Headers)
customer_schema = StructType([
    StructField("member_id", StringType(), True),
    StructField("emp_title", StringType(), True),
    StructField("emp_length", StringType(), True),
    StructField("home_ownership", StringType(), True),
    StructField("annual_inc", FloatType(), True),
    StructField("addr_state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("grade", StringType(), True),
    StructField("sub_grade", StringType(), True),
    StructField("verification_status", StringType(), True),
    StructField("tot_hi_cred_lim", FloatType(), True),
    StructField("application_type", StringType(), True),
    StructField("annual_inc_joint", FloatType(), True),
    StructField("verification_status_joint", StringType(), True)
])

# 2. LOAN RAW SCHEMA (Matches your local CSV abbreviations)
loan_raw_schema = StructType([
    StructField("loan_id", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("loan_amnt", FloatType(), True),
    StructField("funded_amnt", FloatType(), True),
    StructField("term", StringType(), True),
    StructField("int_rate", FloatType(), True),
    StructField("installment", FloatType(), True),
    StructField("issue_d", StringType(), True),
    StructField("loan_status", StringType(), True),
    StructField("purpose", StringType(), True),
    StructField("title", StringType(), True)
])

# 3. LOAN CLEAN SCHEMA (For testing transformations)
# This matches what clean_loans_data() expects AFTER the reader renames them
loan_clean_schema = StructType([
    StructField("loan_id", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("loan_amount", FloatType(), True),
    StructField("funded_amount", FloatType(), True),
    StructField("loan_term_months", StringType(), True),
    StructField("interest_rate", FloatType(), True),
    StructField("monthly_installment", FloatType(), True),
    StructField("issue_date", StringType(), True),
    StructField("loan_status", StringType(), True),
    StructField("loan_purpose", StringType(), True),
    StructField("loan_title", StringType(), True)
])