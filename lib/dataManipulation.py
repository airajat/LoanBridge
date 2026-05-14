from pyspark.sql.functions import current_timestamp, regexp_replace, col, when, length, floor, avg, lit
from functools import reduce
from operator import or_


def clean_customer_data(df):
    # 1. Basic Formatting & Metadata
    renamed_df = df.withColumnRenamed("annual_inc", "annual_income") \
        .withColumnRenamed("addr_state", "address_state") \
        .withColumnRenamed("zip_code", "address_zipcode") \
        .withColumnRenamed("country", "address_country") \
        .withColumnRenamed("tot_hi_cred_lim", "total_high_credit_limit") \
        .withColumnRenamed("annual_inc_joint", "join_annual_income") \
        .withColumn("ingest_date", current_timestamp())

    # 2. SEPARATE REJECTS: Null Income or Invalid State
    # Note: We capture these BEFORE distinct() to see the raw failures
    bad_customers_df = renamed_df.filter("annual_income is null OR address_state is null") \
                                 .withColumn("reject_reason", lit("Missing Income or State"))

    # 3. Success Path: Filtering and Deduplication
    good_customers_df = renamed_df.filter("annual_income is not null AND address_state is not null") \
                                  .dropDuplicates()

    # 4. Clean and Cast Employment Length
    cleaned_emp_df = good_customers_df.withColumn("emp_length", regexp_replace(col("emp_length"), r"(\D)", "")) \
                                      .withColumn("emp_length", col("emp_length").cast("int"))
    
    # Fill Null Employment Length with Mean
    avg_val = cleaned_emp_df.select(floor(avg("emp_length"))).collect()[0][0]
    final_emp_df = cleaned_emp_df.na.fill(avg_val, subset=['emp_length'])

    # 5. Final State Validation
    final_df = final_emp_df.withColumn(
        "address_state",
        when(length(col("address_state")) > 2, "NA").otherwise(col("address_state"))
    )
    
    return final_df, bad_customers_df

def clean_loans_data(df):
    loans_ingest_date = df.withColumn("ingest_date", current_timestamp())

    # 1. Identify Rejects (Rows with any null in critical columns)
    columns_to_check = ["loan_amount", "funded_amount", "loan_term_months", "interest_rate", "monthly_installment", "issue_date", "loan_status", "loan_purpose"]
    null_condition = " OR ".join([f"{c} IS NULL" for c in columns_to_check])
    
    bad_loans_df = loans_ingest_date.filter(null_condition) \
                                   .withColumn("reject_reason", lit("Null values in critical columns"))
    
    # 2. Success Path
    success_loans_df = loans_ingest_date.filter(f"NOT ({null_condition})")
    
    # 3. Transform Loan Term
    loans_term_modified_df = success_loans_df.withColumn("loan_term_years", (regexp_replace(col("loan_term_months"), " months", "").cast("int") / 12).cast("int")) \
           .drop("loan_term_months")
    
    # 4. Standardize Purpose
    loan_purpose_lookup = ["debt_consolidation", "credit_card", "home_improvement", "other", "major_purchase", "medical", "small_business", "car", "vacation", "moving", "house", "wedding", "renewable_energy", "educational"]
    
    final_loans_df = loans_term_modified_df.withColumn("loan_purpose", when(col("loan_purpose").isin(loan_purpose_lookup), col("loan_purpose")).otherwise("other"))
    
    return final_loans_df, bad_loans_df

def clean_repayments_data(df):
    # 1. Add ingestion metadata
    df_ingest_date = df.withColumn("ingest_date", current_timestamp())

    # 2. Define quality gate (critical columns)
    columns_to_check = ["total_principal_received", "total_interest_received", 
                        "total_late_fee_received", "total_payment_received", "last_payment_amount"]
    # Create a dynamic filter that checks if ANY column in the list is null
    null_condition = reduce(or_, [col(c).isNull() for c in columns_to_check])
    
    # Capture the rejects (Anything that has a null in our list)
    bad_repayments_df = df_ingest_date.filter(null_condition) \
        .withColumn("reject_reason", lit(f"Null values detected in one of: {columns_to_check}"))
    
    # Capture the good data
    good_repayments_df = df_ingest_date.na.drop(subset=columns_to_check)

    # fixing total_payment_received calculation
    fixed_payments_df = good_repayments_df.withColumn(
        "total_payment_received",
        when(
            (col("total_principal_received") != 0.0) & (col("total_payment_received") == 0.0),
            col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received")
        ).otherwise(col("total_payment_received"))
    )
    
    # 4. Filter 0.0 payments and clean date strings
    final_df = fixed_payments_df.filter("total_payment_received != 0.0") \
        .withColumn("last_payment_date", 
                    when(col("last_payment_date") == "0.0", None).otherwise(col("last_payment_date"))) \
        .withColumn("next_payment_date", 
                    when(col("next_payment_date") == "0.0", None).otherwise(col("next_payment_date")))
        
    return final_df, bad_repayments_df

from pyspark.sql.functions import col, current_timestamp

def clean_defaulters_data(df):
    # 1. Ingestion Date
    df_ingested = df.withColumn("ingest_date", current_timestamp())
    
    # 2. Data Type Correction & Null Handling
    # Converting float to int and filling nulls with 0 for delinq_2yrs
    processed_df = df_ingested.withColumn("delinq_2yrs", col("delinq_2yrs").cast("integer")) \
                              .fillna(0, subset=["delinq_2yrs"])
    
    # 3. Stream A: Delinquency Data
    # Filter: Members with historical delinquency or current delinquency amounts
    delinq_df = processed_df.filter(
        (col("delinq_2yrs") > 0) | (col("mths_since_last_delinq") > 0)
    ).select(
        "member_id", "delinq_2yrs", "delinq_amnt", 
        col("mths_since_last_delinq").cast("integer").alias("mths_since_last_delinq")
    )
    
    # 4. Stream B: Public Records / Enquiry Data
    # Filter: Members with public records, bankruptcies, or recent enquiries
    records_enq_df = processed_df.filter(
        (col("pub_rec") > 0.0) | 
        (col("pub_rec_bankruptcies") > 0.0) | 
        (col("inq_last_6mths") > 0.0)
    ).select("member_id")
    
    return delinq_df, records_enq_df