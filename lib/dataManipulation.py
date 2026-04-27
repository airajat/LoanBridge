from pyspark.sql.functions import current_timestamp, regexp_replace, col, when, length, floor, avg

def clean_customers(df):
    # 1. Rename columns
    renamed_df = df.withColumnRenamed("annual_inc", "annual_income") \
        .withColumnRenamed("addr_state", "address_state") \
        .withColumnRenamed("zip_code", "address_zipcode") \
        .withColumnRenamed("country", "address_country") \
        .withColumnRenamed("tot_hi_cred_lim", "total_high_credit_limit") \
        .withColumnRenamed("annual_inc_joint", "join_annual_income") \
        .withColumn("ingest_date", current_timestamp())

    # 2. Remove duplicates and null income
    distinct_df = renamed_df.distinct().filter("annual_income is not null")

    # 3. Clean and Cast Employment Length
    cleaned_emp_df = distinct_df.withColumn("emp_length", regexp_replace(col("emp_length"), r"(\D)", "")) \
                                .withColumn("emp_length", col("emp_length").cast("int"))
    
    # 4. Fill Null Employment Length with Mean
    avg_val = cleaned_emp_df.select(floor(avg("emp_length"))).collect()[0][0]
    final_emp_df = cleaned_emp_df.na.fill(avg_val, subset=['emp_length'])

    # 5. Clean State Length
    final_df = final_emp_df.withColumn(
        "address_state",
        when(length(col("address_state")) > 2, "NA").otherwise(col("address_state"))
    )
    
    return final_df